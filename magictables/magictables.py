import random
from string import Formatter
import urllib.parse
from magictables.fallback_driver import HybridDriver

import asyncio
import os
import aiohttp
import dateparser
import polars as pl
import json
from typing import Dict, Any, Optional, List, Tuple, Union
from neo4j import Query
import hashlib

from magictables.utils import (
    call_ai_model,
    generate_embeddings,
    flatten_nested_structure,
)
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

import logging
import os
from logging.handlers import RotatingFileHandler

# Set up logging
log_directory = "logs"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

log_file = os.path.join(log_directory, "magictables.log")

# Create a RotatingFileHandler
file_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
file_handler.setLevel(logging.DEBUG)

# Create a StreamHandler for console output
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter and set it for both handlers
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Get the root logger and add both handlers
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)
logger.addHandler(console_handler)


class MagicTable(pl.DataFrame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.openai_api_key = "your_openai_api_key_here"
        self.neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.neo4j_user = os.getenv("NEO4J_USER", "neo4j")
        self.neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        self.jina_api_key = os.getenv("JINA_API_KEY")
        self.api_urls = []
        self._driver = None

    async def _get_driver(self) -> HybridDriver:
        if self._driver is None:
            parsed_uri = urllib.parse.urlparse(self.neo4j_uri)
            scheme = parsed_uri.scheme
            if scheme in ["bolt", "neo4j", "bolt+s", "http", "https"]:
                self._driver = HybridDriver(
                    self.neo4j_uri, self.neo4j_user, self.neo4j_password
                )
                await self._driver._load_cache()
            else:
                raise ValueError(f"Unsupported URI scheme: {scheme}")
        return self._driver

    async def _close_driver(self):
        if self._driver is not None:
            await self._driver.close()
            self._driver = None

    async def __aenter__(self):
        self._driver = await self._get_driver()
        return self

    def __del__(self):
        pass

    @classmethod
    async def from_polars(cls, df: pl.DataFrame, label: str) -> "MagicTable":
        magic_df = cls(df)
        api_url = f"local://{label}"
        description = f"Local DataFrame: {label}"
        embedding = await generate_embeddings([description])
        await magic_df._store_in_neo4j(label, api_url, description, embedding[0])
        return magic_df

    @classmethod
    async def from_api(
        cls,
        api_url: str,
        params: Optional[Dict[str, Any]] = None,
        include_embedding=False,
    ) -> "MagicTable":
        logger.debug(f"Attempting to retrieve data for API URL: {api_url}")

        existing_data = await cls._get_existing_api_data(api_url, include_embedding)
        if existing_data:
            logger.info(f"Found existing data for API URL: {api_url}")
            df = cls(existing_data)
            df.api_urls.append(api_url)
            return df._parse_json_fields()
        else:
            logger.warning(f"No existing data found for API URL: {api_url}")

        # If data doesn't exist, proceed with API call and storage
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url, params=params) as response:
                data = await response.json()

        data = flatten_nested_structure(data)
        df = cls(data)

        parsed_url = urllib.parse.urlparse(api_url)
        path_parts = parsed_url.path.split("/")
        label = (
            path_parts[-1].capitalize()
            if path_parts[-1]
            else path_parts[-2].capitalize()
        )

        logger.debug(f"Storing data for API URL: {api_url}")
        await df._store_rows_in_neo4j(label, api_url)

        df.api_urls.append(api_url)
        return df._parse_json_fields()

    async def _store_rows_in_neo4j(
        self,
        label: str,
        api_url: str,
        transform_query: Optional[str] = None,
        pandas_code: Optional[str] = None,
    ):
        sanitized_label = self._sanitize_label(label)
        logger.debug(f"Storing rows with label: {sanitized_label}, API URL: {api_url}")

        driver = await self._get_driver()
        async with driver.session() as session:
            # Check if API description already exists
            result = await session.run(
                "MATCH (a:APIEndpoint {url: $api_url}) RETURN a.description",
                api_url=api_url,
            )
            existing_description = await result.single()

            if existing_description and existing_description["a.description"]:
                api_description = existing_description["a.description"]
            else:
                # Generate API description if it doesn't exist
                # api_description = await self._generate_api_description(
                #     api_url, self.to_dicts()[:50]
                # )
                api_description = self.to_dicts()[:70]

            # Generate query_key
            query_key = self._generate_query_key(
                transform_query or "", [[(api_url, api_description, 1.0)]]
            )

            # Prepare all rows at once
            rows = []
            combined_texts = []
            for row in self.to_dicts():
                standardized_row = self._standardize_data_types(row)
                node_id = self._generate_node_id(sanitized_label, standardized_row)
                combined_text = f"{api_description} {json.dumps(standardized_row)}"
                rows.append(
                    {
                        "node_id": node_id,
                        "row": standardized_row,
                    }
                )
                combined_texts.append(combined_text)

            # Generate embeddings in one batch
            embeddings = await generate_embeddings(combined_texts)

            # Add embeddings to rows
            for row, embedding in zip(rows, embeddings):
                row["embedding"] = embedding

            # Single query to check existing nodes, insert new ones, and create relationships
            query = f"""
            MERGE (a:APIEndpoint {{url: $api_url}})
            SET a.description = $api_description
            WITH a
            UNWIND $rows AS row
            MERGE (d:{sanitized_label} {{id: row.node_id}})
            ON CREATE SET d += row.row, d.embedding = row.embedding
            MERGE (d)-[:SOURCED_FROM]->(a)
            WITH d, a
            
            // Create TransformedData node if transform was applied
            {f'''
            MERGE (q:QueryNode {{key: $query_key}})
            SET q.natural_query = $transform_query
            MERGE (t:TransformedData {{id: apoc.create.uuid()}})
            SET t += d
            MERGE (q)-[r:TRANSFORMED_BY]->(t)
            SET r.pandas_code = $pandas_code
            ''' if transform_query and pandas_code else ''}
            RETURN count(d) as nodes_created
            """

            result = await session.run(
                query,
                api_url=api_url,
                api_description=api_description,
                rows=rows,
                query_key=query_key,
                transform_query=transform_query,
                pandas_code=pandas_code,
            )
            summary = await result.consume()
            logger.debug(f"Stored {summary.counters.nodes_created} nodes in Neo4j")

    @classmethod
    async def _get_existing_api_data(
        cls, api_url: str, include_embedding: bool = False
    ) -> Optional[List[Dict[str, Any]]]:
        logger.debug(f"Attempting to retrieve existing data for API URL: {api_url}")
        instance = cls()
        driver = await instance._get_driver()
        async with driver.session() as session:
            query = """
            MATCH (n)-[:SOURCED_FROM]->(a:APIEndpoint {url: $api_url})
            RETURN collect(n) as nodes
            """
            result = await session.run(query, api_url=api_url)
            record = await result.single()
            if record and record["nodes"]:
                nodes = record["nodes"]
                filtered_nodes = []
                for node in nodes:
                    filtered_node = {
                        k: v
                        for k, v in node.items()
                        if (include_embedding or k != "embedding")
                    }
                    filtered_nodes.append(filtered_node)
                logger.info(
                    f"Found {len(filtered_nodes)} records for API URL: {api_url}"
                )
                return filtered_nodes
            else:
                logger.warning(f"No records found for API URL: {api_url}")
        return None

    async def _generate_api_description(
        self, api_url: str, data: List[Dict[str, Any]]
    ) -> str:
        # Remove query parameters from the URL
        base_url = api_url.split("?")[0]

        # Check if description already exists in the graph
        driver = await self._get_driver()
        async with driver.session() as session:
            result = await session.run(
                """
                MATCH (a:APIEndpoint {url: $base_url})
                RETURN a.description
                """,
                base_url=base_url,
            )
            existing_description = await result.single()

            if existing_description:
                return existing_description["a.description"]

        # If no existing description, generate a new one
        prompt = f"""Generate a concise description of this API endpoint based on the URL and data sample.

    Examples:
    1. URL: https://api.example.com/users
    Data: [{{"id": 1, "name": "John Doe", "email": "john@example.com"}}]
    {{"description": "This API endpoint provides user information including user ID, name, and email address."}}

    2. URL: https://api.example.com/products
    Data: [{{"id": 101, "name": "Laptop", "price": 999.99, "category": "Electronics"}}]
    {{"description": "This API endpoint returns product details such as product ID, name, price, and category."}}

    3. URL: https://api.example.com/orders
    Data: [{{"order_id": "ORD-001", "user_id": 1, "total": 1299.99, "status": "shipped"}}]
    {{"description": "This API endpoint provides order information including order ID, associated user ID, total amount, and current status."}}

    Please provide a similar concise description for the given API endpoint:
    URL: {base_url}
    Data: {json.dumps(data[:1])}

    Your response should be in the following JSON format:
    {{"description": "Your concise description here"}}
    """

        response = await call_ai_model(data, prompt)
        logger.debug(response)

        if isinstance(response, dict):
            description = response.get(
                "description", "Error generating API description"
            )
        else:
            description = "Error generating API description"

        # Store the new description in the graph
        await session.run(
            """
            MERGE (a:APIEndpoint {url: $base_url})
            SET a.description = $description
            """,
            base_url=base_url,
            description=description,
        )

        return description

    @staticmethod
    def _generate_node_id(label: str, data: Dict[str, Any]) -> str:
        key_fields = ["id", "uuid", "name", "email"]
        key_data = {k: str(v) for k, v in data.items() if k in key_fields}

        if not key_data:
            key_data = {k: str(v) for k, v in data.items()}

        data_str = json.dumps(key_data, sort_keys=True)
        hash_object = hashlib.md5((label + data_str).encode())
        return hash_object.hexdigest()

    async def _search_relevant_api_urls(
        self, queries: Union[str, List[str]], top_k: int = 3
    ) -> List[List[Tuple[str, str, float]]]:
        if isinstance(queries, str):
            queries = [queries]

        async def process_query(query):
            query_embedding = await generate_embeddings(texts=[query])

            driver = await self._get_driver()
            async with driver.session() as session:
                result = await session.run(
                    """
                    MATCH (d)
                    WHERE d.api_url IS NOT NULL
                    WITH d, gds.similarity.cosine(d.embedding, $query_embedding) AS similarity
                    ORDER BY similarity DESC
                    LIMIT $top_k
                    RETURN DISTINCT d.api_url, d.api_description, similarity
                    """,
                    query_embedding=query_embedding,
                    top_k=top_k,
                )

                records = await result.fetch(n=-1)  # Fetch all records
                return [
                    (
                        record["d.api_url"],
                        record["d.api_description"],
                        record["similarity"],
                    )
                    for record in records
                ]

        return await asyncio.gather(*[process_query(query) for query in queries])

    def _generate_query_key(
        self, natural_query: str, relevant_urls: List[List[Tuple[str, str, float]]]
    ) -> str:
        # Flatten the list of relevant URLs
        flattened_urls = [url for sublist in relevant_urls for url, _, _ in sublist]
        # Sort the URLs to ensure consistency
        sorted_urls = sorted(flattened_urls)
        # Combine the query and URLs into a single string
        combined_string = f"{natural_query}|{'|'.join(sorted_urls)}"
        # Generate a hash of the combined string
        return hashlib.md5(combined_string.encode()).hexdigest()

    async def _execute_cypher(
        self, query: Union[str, Query], params: Dict[str, Any] = {}
    ) -> "MagicTable":
        driver = await self._get_driver()
        async with driver.session() as session:
            if isinstance(query, str):
                query = Query(query)  # type: ignore
            result = await session.run(query, params)
            records = await result.fetch(n=-1)  # Fetch all records
            records_dicts = [dict(record) for record in records]

        df = pl.DataFrame(records_dicts)
        return MagicTable(df)._parse_json_fields()

    def _parse_json_fields(self) -> "MagicTable":
        for column in self.columns:
            if self[column].dtype == pl.Utf8:
                try:
                    parsed = pl.Series(
                        name=column,
                        values=[json.loads(x) if x else None for x in self[column]],
                    )
                    if isinstance(parsed[0], (list, dict)):
                        self = self.with_columns([parsed])
                except:
                    pass
        return MagicTable(self)

    def _standardize_data_types(self, data: Dict[str, Any]) -> Dict[str, Any]:
        standardized_data = {}
        for key, value in data.items():
            if isinstance(value, str):
                # Attempt to parse as date
                try:
                    parsed_date = dateparser.parse(value)
                    if parsed_date:
                        standardized_data[key] = parsed_date.isoformat()
                    else:
                        standardized_data[key] = value.strip()
                except:
                    standardized_data[key] = value.strip()
            elif isinstance(value, (int, float)):
                standardized_data[key] = float(value)
            elif isinstance(value, list):
                # Convert list to a string representation
                standardized_data[key] = json.dumps(value)
            elif isinstance(value, dict):
                # Convert dict to a string representation
                standardized_data[key] = json.dumps(value)
            else:
                standardized_data[key] = str(value)
        return standardized_data

    @staticmethod
    def _extract_node_labels(cypher_query: str) -> List[str]:
        # This is a simple extraction method and might need to be more sophisticated
        # depending on the complexity of your Cypher queries
        labels = []
        parts = cypher_query.split()
        for i, part in enumerate(parts):
            if part == "MATCH" and i + 1 < len(parts):
                label = parts[i + 1].strip("()")
                if ":" in label:
                    labels.append(label.split(":")[-1])
        return list(set(labels))

    async def _generate_and_execute_pandas_code(
        self, pandas_df: pd.DataFrame, natural_query: str
    ) -> Tuple[pd.DataFrame, Optional[str]]:
        query_key = self._generate_query_key(natural_query, [])

        # Check if we have stored pandas code
        stored_code = await self._get_stored_pandas_code(query_key)
        if stored_code:
            pandas_code = stored_code
        else:
            prompt = f"""Given the following pandas DataFrame structure and the query, generate Python code to process or analyze the data using pandas.

    What we are trying to achieve: {natural_query}
    Find popular movies with a vote average greater than 7.5 and list their cast members who are older than 40

    Current DataFrame you must only work with:
    DataFrame Structure:
    Columns: {pandas_df.columns.tolist()}
    Shape: {pandas_df.shape}
    Data Types:
    {pandas_df.dtypes}

    Sample Data (first 10 rows):
    {pandas_df.head(10).to_string()}

    Please provide Python code to process this DataFrame, adhering to the following guidelines:
    1. Only use columns that exist in the DataFrame. Do not reference any columns not listed above.
    2. Ensure all operations are efficient and use pandas vectorized operations where possible.
    3. Handle potential data type issues, especially for date/time columns or numeric calculations.
    4. The code should return a pandas DataFrame as the result.
    5. Do not include any print statements or comments in the code.
    6. The input DataFrame is named 'df'.
    7. If the DataFrame is empty or missing required columns, create a sample DataFrame with the necessary columns.
    8. When working with dates, always use pd.to_datetime() for conversion and handle potential errors.
    9. For age calculations, use a method that works across different pandas versions, avoiding timedelta conversions to years.
    10. You MUST only use pandas and no other libraries.

    Your response should be in the following JSON format:
    date_columns = df.select_dtypes(include=['object']).columns[df.select_dtypes(include=['object']).apply(lambda x: pd.to_datetime(x, errors='coerce').notnull().all())]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # Handle missing values
    df = df.fillna({{
        col: df[col].mean() if pd.api.types.is_numeric_dtype(df[col]) else df[col].mode()[0]
        for col in df.columns
    }})

    # Create age column if 'birthdate' exists
    if 'birthdate' in df.columns:
        df['age'] = (pd.Timestamp.now() - df['birthdate']).astype('<m8[Y]').astype(int)

    # Perform string operations
    text_columns = df.select_dtypes(include=['object']).columns
    for col in text_columns:
        df[col] = df[col].str.strip().str.lower()

    # One-hot encode categorical variables
    categorical_columns = df.select_dtypes(include=['object']).columns
    df = pd.get_dummies(df, columns=categorical_columns, drop_first=True)

    # Scale numerical columns
    from sklearn.preprocessing import StandardScaler
    scaler = StandardScaler()
    numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns
    df[numeric_columns] = scaler.fit_transform(df[numeric_columns])

    # Perform aggregations
    if 'category' in df.columns and 'value' in df.columns:
        df['total_value'] = df.groupby('category')['value'].transform('sum')
        df['percentage'] = df['value'] / df['total_value'] * 100

    # Filter data based on conditions
    if 'age' in df.columns:
        df = df[df['age'] >= 18]

    # Sort the dataframe
    if 'date' in df.columns:
        df = df.sort_values('date', ascending=False)

    # Rename columns for clarity
    df = df.rename(columns={{
        'col1': 'feature_1',
        'col2': 'feature_2'
    }})

    # Create a new calculated column
    if 'price' in df.columns and 'quantity' in df.columns:
        df['total_revenue'] = df['price'] * df['quantity']

    # Perform a rolling average if time-series data is present
    if 'date' in df.columns and 'value' in df.columns:
        df = df.sort_values('date')
        df['rolling_avg'] = df['value'].rolling(window=7).mean()

    # Drop unnecessary columns
    columns_to_drop = ['temp_col1', 'temp_col2']
    df = df.drop(columns=columns_to_drop, errors='ignore')

    # Reset index if needed
    df = df.reset_index(drop=True)

    result = df
    """
            response = await call_ai_model(
                [],
                prompt,
                model="gpt-4o",
                # model="openrouter/anthropic/claude-3.5-sonnet:beta",
            )

            if isinstance(response, str):
                logger.error(
                    f"Failed to generate pandas code. AI model response: {response}"
                )
                return pandas_df, None

            pandas_code = response

            logger.debug(f"Generated pandas code: {pandas_code}")

        # Execute the generated code
        try:
            local_vars = {"df": pandas_df, "pd": pd}
            exec(pandas_code, globals(), local_vars)
            result = local_vars.get("result", pandas_df)
            if not isinstance(result, pd.DataFrame):
                raise ValueError("The generated code did not return a pandas DataFrame")
            logger.debug(
                f"Pandas code executed successfully. Result shape: {result.shape}"
            )
            await self._store_pandas_query(natural_query, pandas_code)

            return result, pandas_code
        except Exception as e:
            logger.error(f"Error executing pandas code: {str(e)}")
            logger.error(f"Generated code:\n{pandas_code}")
            logger.debug("Falling back to original DataFrame")
            return pandas_df, None

    async def _get_database_schema(self) -> Dict[str, Any]:
        driver = await self._get_driver()
        async with driver.session() as session:
            try:
                # Use built-in Neo4j procedure to get schema information
                result = await session.run(
                    """
                    CALL db.schema.visualization()
                    YIELD nodes, relationships
                    RETURN nodes, relationships
                    """
                )
                schema = await result.single()

                if schema:
                    return {
                        "nodes": [
                            self._process_schema_node(node) for node in schema["nodes"]
                        ],
                        "relationships": [
                            self._process_schema_relationship(rel)
                            for rel in schema["relationships"]
                        ],
                    }
                else:
                    return {"nodes": [], "relationships": []}
            except Exception as e:
                # print(f"Error retrieving database schema: {str(e)}")
                return {"nodes": [], "relationships": []}

    def _process_schema_node(self, node):
        return {"label": node.labels[0], "properties": list(node.properties.keys())}

    def _process_schema_relationship(self, relationship):
        return {
            "type": relationship.type,
            "start_node": relationship.start_node.labels[0],
            "end_node": relationship.end_node.labels[0],
            "properties": list(relationship.properties.keys()),
        }

    async def _store_queries(
        self, query_key: str, cypher_query: str, pandas_query: str
    ):
        driver = await self._get_driver()
        async with driver.session() as session:
            await session.run(
                """
                MERGE (q:QueryNode {key: $query_key})
                SET q.cypher_query = $cypher_query,
                    q.pandas_query = $pandas_query
                """,
                query_key=query_key,
                cypher_query=cypher_query,
                pandas_query=pandas_query,
            )

    async def _get_stored_queries(self, query_key: str) -> Optional[Tuple[str, str]]:
        driver = await self._get_driver()
        async with driver.session() as session:
            result = await session.run(
                """
                MATCH (q:QueryNode {key: $query_key})
                RETURN q.cypher_query, q.pandas_query
                """,
                query_key=query_key,
            )
            stored_queries = await result.single()

        if (
            stored_queries
            and stored_queries[0] is not None
            and stored_queries[1] is not None
        ):
            return (str(stored_queries[0]), str(stored_queries[1]))
        return None

    def _identify_template_params(self, url_template: str) -> List[str]:
        """
        Identify the parameters in the URL template.
        """
        return [fname for _, fname, _, _ in Formatter().parse(url_template) if fname]

    def _format_url_template(self, url_template: str, row: Dict[str, Any]) -> str:
        """
        Format the URL template with values from the row.
        """
        return url_template.format(
            **{k: row.get(k, "") for k in self._identify_template_params(url_template)}
        )

    @staticmethod
    def _is_numeric_dtype(dtype):
        return dtype in [
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
            pl.Float32,
            pl.Float64,
        ]

    @classmethod
    async def gen(cls, query: str) -> "MagicTable":
        """
        Generate a MagicTable with data based on the provided natural language query.

        This method uses AI to generate Python code that creates a pandas DataFrame
        based on the given query. It can handle various data generation scenarios,
        including creating mock data or simulating data from APIs with pagination.

        :param query: A string containing the natural language query for data generation.
                    This can include descriptions of the desired data structure,
                    API endpoints, pagination requirements, and data transformations.

        :return: A MagicTable with generated data.

        Usage examples:
        1. Generate mock data:
        query = "Create a dataset of 100 customers with name, age, and purchase amount"

        2. Fetch data from an API with pagination:
        query = "Fetch top 100 movies from TMDB API with title, release date, and rating.
                Use this URL template: https://api.themoviedb.org/3/movie/top_rated?api_key={api_key}&page={page}"

        3. Generate time series data:
        query = "Generate daily stock prices for AAPL, GOOGL, and MSFT for Q1 2024"

        Note: When using API-like queries, the method will generate query parameter pairs
        based on the URL structure, focusing on pagination requirements.
        """
        prompt = f"""
        Generate Python code to create a pandas DataFrame using iterators or generators based on the following natural language query:

        "{query}"

        The code should use pandas to efficiently create the DataFrame.
        Only import and use pandas (as pd). Do not use any other libraries.

        If the query mentions or implies the need for pagination or fetching data from an API,
        generate query parameter pairs based on the URL template, focusing on pagination requirements.

        Examples:

        1. Query: "Fetch top 100 movies from TMDB API with title, release date, and rating. Use this URL template: https://api.themoviedb.org/3/movie/top_rated?api_key={{api_key}}&page={{page}}"
        Expected output:

        import pandas as pd

        def generate_query_params():
            for page in range(1, 6):  # Assuming 20 movies per page, 5 pages for 100 movies
                yield {{
                    'api_key': 'dummy_api_key',
                    'page': page
                }}

        df = pd.DataFrame(generate_query_params())

        2. Query: "Fetch weather data for New York for the first week of January 2024, using a weather API with pagination. Use this URL template: https://api.example.com/weather?city={{city}}&date={{date}}&page={{page}}"
        Expected output:

        import pandas as pd

        def generate_query_params():
            city = 'New York'
            date_range = pd.date_range(start='2024-01-01', end='2024-01-07')
            for date in date_range:
                yield {{
                    'city': city,
                    'date': date.strftime('%Y-%m-%d'),
                    'page': 1  # Assuming one page per day
                }}

        df = pd.DataFrame(generate_query_params())

        Your response should be the Python code directly, without any JSON formatting.
        """
        code = await call_ai_model([], prompt, return_json=False)

        if not code:
            raise ValueError("Failed to generate DataFrame code")

        print("code", code)

        # Execute the generated code
        local_vars = {"pd": pd}
        exec(code, globals(), local_vars)

        if "df" in local_vars and isinstance(local_vars["df"], pd.DataFrame):
            # Convert pandas DataFrame to polars DataFrame
            return cls(pl.from_pandas(local_vars["df"]))
        else:
            raise ValueError("Generated code did not produce a valid DataFrame")

    async def chain(self, other: Union["MagicTable", str]) -> "MagicTable":
        if isinstance(other, str):
            other_api_url_template = other
        elif isinstance(other, MagicTable) and other.api_urls:
            other_api_url_template = other.api_urls[-1]
        else:
            raise ValueError(
                "Invalid input for chaining. Expected MagicTable with API URL or API URL template string."
            )

        # Prepare API URLs for each row
        api_urls = [
            self._format_url_template(other_api_url_template, row)
            for row in self.iter_rows(named=True)
        ]

        # Process all API calls in a single batch
        all_results = await self._process_api_batch_single_query(api_urls)

        # Create a list to store the expanded original data
        expanded_original_data = []

        # Create a list to store the new data
        new_data = []

        # Iterate through the original data and the results
        for original_row, result in zip(self.iter_rows(named=True), all_results):
            if isinstance(result, list):
                # If the result is a list (multiple rows returned)
                for item in result:
                    expanded_original_data.append(original_row)
                    new_data.append(item)
            else:
                # If the result is a single item
                expanded_original_data.append(original_row)
                new_data.append(result)

        # Create DataFrames from the expanded data
        expanded_original_df = pl.DataFrame(expanded_original_data)
        new_df = pl.DataFrame(new_data)

        # Combine results with expanded original data
        combined_results = pl.concat([expanded_original_df, new_df], how="horizontal")

        # Create a new MagicTable with combined results
        result_df = MagicTable(combined_results)
        result_df.api_urls = self.api_urls + [other_api_url_template]

        return result_df

    async def _process_url(self, url: str) -> Dict[str, Any]:
        max_retries = 5
        base_delay = 1

        for attempt in range(max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        if response.status == 429:
                            raise aiohttp.ClientResponseError(
                                response.request_info,
                                response.history,
                                status=429,
                            )
                        response.raise_for_status()
                        data = await response.json()
                        data = flatten_nested_structure(data)
                        logger.debug(f"Fetched data for URL {url}: {data}")
                    return data
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.error(f"Error fetching data for URL {url}: {e}")
                if attempt == max_retries - 1:
                    logger.error(f"Last attempt failed for URL {url}")
                    return {}
                else:
                    delay = (2**attempt * base_delay) + (random.randint(0, 1000) / 1000)
                    logger.warning(
                        f"Attempt {attempt + 1} failed for URL {url}. Retrying in {delay:.2f} seconds..."
                    )
                    await asyncio.sleep(delay)
        return {}

    async def _process_api_batch_single_query(
        self, api_urls: List[str]
    ) -> List[Dict[str, Any]]:
        driver = await self._get_driver()
        async with driver.session() as session:
            stored_results_query = """
            UNWIND $urls AS url
            MATCH (a:APIEndpoint {url: url})-[:SOURCED_FROM]->(d)
            WITH url, collect(d) AS data_nodes
            RETURN url, [node IN data_nodes | node {.*}] AS data
            """
            # Check for stored results and identify URLs to fetch
            stored_result = await session.run(stored_results_query, urls=api_urls)
            stored_data = {}
            async for record in stored_result:
                logging.debug(f"Retrieved record: {record}")
                if record["data"]:
                    stored_data[record["url"]] = record["data"]
                else:
                    logging.warning(f"Empty data retrieved for URL: {record['url']}")

            logging.info(f"Number of stored results: {len(stored_data)}")
            logging.info(f"Number of API URLs: {len(api_urls)}")

            # If all URLs have stored data, return immediately
            if len(stored_data) == len(api_urls):
                return [stored_data[url] for url in api_urls]

            # Identify URLs to fetch
            urls_to_fetch = [url for url in api_urls if url not in stored_data]
            logging.debug(f"URLs to fetch: {urls_to_fetch}")

            if urls_to_fetch:
                # Fetch new results
                new_results = await asyncio.gather(
                    *[self._process_url(url) for url in urls_to_fetch]
                )

                # Prepare the batch data
                batch_data = [
                    {"url": url, "data": data if isinstance(data, list) else [data]}
                    for url, data in zip(urls_to_fetch, new_results)
                    if data  # Only include non-empty results
                ]

                if batch_data:
                    # Store new results
                    store_query = """
                    UNWIND $batch AS item
                    MERGE (a:APIEndpoint {url: item.url})
                    WITH a, item
                    UNWIND item.data AS data_item
                    CREATE (d:Data)
                    SET d = data_item
                    MERGE (d)-[:SOURCED_FROM]->(a)
                    """
                    await session.run(store_query, batch=batch_data)

                    # Update stored_data with new results
                    for item in batch_data:
                        stored_data[item["url"]] = item["data"]

            # Prepare final results in the order of input api_urls
            all_results = [stored_data.get(url, []) for url in api_urls]

        return all_results

    def _get_label_from_url(self, url: str) -> str:
        parsed_url = urllib.parse.urlparse(url)
        path_parts = parsed_url.path.split("/")
        label = (
            path_parts[-1].capitalize()
            if path_parts[-1]
            else path_parts[-2].capitalize()
        )
        return self._sanitize_label(label)

    async def _identify_key_columns(self, api_url_template: str) -> List[str]:
        """
        Identify the most suitable key columns for the given API URL template.

        :param api_url_template: The API URL template to analyze.
        :return: A list of names of the identified key columns.
        """
        # Extract placeholders from the API URL template
        placeholders = [p.strip("{}") for p in api_url_template.split("{") if "}" in p]

        # Get column information
        column_info = {
            col: {"dtype": str(dtype), "sample": self[col].head(5).to_list()}
            for col, dtype in zip(self.columns, self.dtypes)
        }

        # Find matching columns for each placeholder
        matches = {}
        for placeholder in placeholders:
            for col, info in column_info.items():
                # Check if the column name is similar to the placeholder
                if (
                    placeholder.lower() in col.lower()
                    or col.lower() in placeholder.lower()
                ):
                    matches[placeholder] = matches.get(placeholder, []) + [col]

        # If we have a single match for each placeholder, return them
        if all(len(cols) == 1 for cols in matches.values()):
            return [next(iter(cols)) for cols in matches.values()]

        # If we have multiple matches or no matches for any placeholder, use AI to decide
        if not matches or any(len(cols) > 1 for cols in matches.values()):
            prompt = f"""Given the following API URL template and the current DataFrame structure, 
    identify the most suitable columns to use as keys for chaining API calls.

    API URL Template: {api_url_template}

    Placeholders: {placeholders}

    DataFrame Columns and Types:
    {json.dumps(column_info, indent=2)}

    Potential Matches:
    {json.dumps(matches, indent=2)}

    Please provide the names of the columns that best match the placeholders in the API URL template.
    Your response should be in the following JSON format:
    {{"column_names": ["example_column1", "example_column2"]}}
    Replace the example column names with the actual column names you identify as the best matches.
    """

            response = await call_ai_model([column_info], prompt)
            key_columns = response.get("column_names", [])

            if not key_columns or any(col not in self.columns for col in key_columns):
                raise ValueError(
                    f"Unable to identify suitable key columns for the given API URL template: {api_url_template}"
                )

            return key_columns

        # If we have a single match for some placeholders and multiple for others,
        # return the single matches and use the first match for the others
        return [next(iter(cols)) for cols in matches.values()]

    async def _store_in_neo4j(
        self, label: str, api_url: str, description: str, embedding: List[float]
    ):
        sanitized_label = self._sanitize_label(label)

        driver = await self._get_driver()
        async with driver.session() as session:
            await session.run(
                """
                MERGE (n:APIEndpoint {url: $api_url})
                SET n.description = $description,
                    n.embedding = $embedding
                """,
                api_url=api_url,
                description=description,
                embedding=embedding,
            )

            # Standardize data types for the entire DataFrame
            standardized_df = self._standardize_data_types_polars(self)

            # Generate node IDs for the entire DataFrame
            node_ids = self._generate_node_ids_polars(standardized_df, sanitized_label)

            # Combine standardized data and node IDs
            combined_df = standardized_df.with_columns([node_ids.alias("node_id")])

            # Construct Cypher queries for all rows
            queries = [
                Query(
                    f"""
                    MERGE (d:{sanitized_label} {{id: $node_id}})
                    SET d += $row
                    WITH d
                    MATCH (a:APIEndpoint {{url: $api_url}})
                    MERGE (d)-[:SOURCED_FROM]->(a)
                    """
                )
                for _ in range(len(combined_df))
            ]

            # Execute Cypher queries in bulk
            await asyncio.gather(
                *[
                    session.run(
                        query,
                        node_id=row["node_id"],
                        row={k: v for k, v in row.items() if k != "node_id"},
                        api_url=api_url,
                    )
                    for query, row in zip(queries, combined_df.to_dicts())
                ]
            )

    def _standardize_data_types_polars(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.select(
            [
                pl.when(pl.col(col).is_numeric())
                .then(pl.col(col).cast(pl.Float64))
                .when(pl.col(col).str.strptime(pl.Datetime, strict=False).is_not_null())
                .then(pl.col(col).str.strptime(pl.Datetime, strict=False))
                .otherwise(pl.col(col).cast(pl.Utf8))
                .alias(col)
                for col in df.columns
            ]
        )

    def _generate_node_ids_polars(self, df: pl.DataFrame, label: str) -> pl.Series:
        # Combine all columns into a single string
        combined = pl.concat_str(
            [pl.lit(label)] + [pl.col(col).cast(pl.Utf8) for col in df.columns]
        )

        # Generate MD5 hash
        return combined.map_elements(lambda x: hashlib.md5(x.encode()).hexdigest())

    def _sanitize_label(self, label: str) -> str:
        # Ensure the label starts with a letter
        if not label[0].isalpha():
            label = "N" + label
        # Replace any non-alphanumeric characters with underscores
        return "".join(c if c.isalnum() else "_" for c in label)

    async def _store_cypher_query_as_edge(self, natural_query: str, cypher_query: str):
        relevant_urls = await self._search_relevant_api_urls([natural_query])
        query_key = self._generate_query_key(natural_query, relevant_urls)

        driver = await self._get_driver()
        async with driver.session() as session:
            # Create a QueryNode for the natural language query
            await session.run(
                """
                MERGE (q:QueryNode {key: $query_key})
                SET q.natural_query = $natural_query,
                    q.cypher_query = $cypher_query
                """,
                query_key=query_key,
                natural_query=natural_query,
                cypher_query=cypher_query,
            )

            # Find the nodes involved in the Cypher query
            involved_nodes = self._extract_node_labels(cypher_query)

            # Create edges from the QueryNode to involved nodes, with the Cypher query as a property
            for node_label in involved_nodes:
                await session.run(
                    f"""
                    MATCH (q:QueryNode {{key: $query_key}}), (n:{node_label})
                    MERGE (q)-[r:QUERIES]->(n)
                    SET r.natural_query = $natural_query,
                        r.cypher_query = $cypher_query
                    """,  # type: ignore
                    query_key=query_key,
                    natural_query=natural_query,
                    cypher_query=cypher_query,
                )

    async def _store_cypher_query(self, query_key: str, cypher_query: str):
        driver = await self._get_driver()
        async with driver.session() as session:
            await session.run(
                """
                MERGE (q:QueryNode {key: $query_key})
                SET q.cypher_query = $cypher_query
                """,
                query_key=query_key,
                cypher_query=cypher_query,
            )

    async def _get_stored_cypher_query(self, query_key: str) -> Optional[str]:
        driver = await self._get_driver()
        async with driver.session() as session:
            result = await session.run(
                """
                MATCH (q:QueryNode {key: $query_key})-[r:QUERIES]->()
                RETURN r.cypher_query
                LIMIT 1
                """,
                query_key=query_key,
            )
            stored_query = await result.single()

        return stored_query[0] if stored_query else None

    async def clear_all_data(self):
        """
        Clear all data from the Neo4j database, including APIEndpoint nodes.
        """
        driver = await self._get_driver()
        async with driver.session() as session:
            query = """
            MATCH (n)
            DETACH DELETE n
            """

            result = await session.run(query)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._close_driver()
        if exc_type:
            # Handle the exception if needed
            print(f"An error occurred: {exc_type.__name__}: {exc_val}")
        return False  # Propagate exceptions

    async def _store_pandas_query(self, natural_query: str, pandas_code: str):
        query_key = self._generate_query_key(
            natural_query, []
        )  # No relevant URLs for pandas queries

        driver = await self._get_driver()
        async with driver.session() as session:
            await session.run(
                """
                MERGE (q:QueryNode {key: $query_key})
                SET q.natural_query = $natural_query,
                    q.pandas_code = $pandas_code
                """,
                query_key=query_key,
                natural_query=natural_query,
                pandas_code=pandas_code,
            )

    async def _get_stored_pandas_code(self, query_key: str) -> Optional[str]:
        driver = await self._get_driver()
        async with driver.session() as session:
            result = await session.run(
                """
                MATCH (q:QueryNode {key: $query_key})
                RETURN q.pandas_code
                """,
                query_key=query_key,
            )
            stored_code = await result.single()

        return stored_code[0] if stored_code else None

    async def _get_cached_transform(self, query_key: str) -> Optional[pl.DataFrame]:
        logger.debug(
            f"Attempting to retrieve cached transform for query key: {query_key}"
        )
        driver = await self._get_driver()
        async with driver.session() as session:
            result = await session.run(
                """
                MATCH (q:QueryNode {key: $query_key})
                RETURN q.cypher_query, q.pandas_query
                """,
                query_key=query_key,
            )
            stored_queries = await result.single()
        # print("query", query_key)

        if stored_queries and stored_queries.data:  # Check if pandas_query exists
            print(stored_queries)

            pandas_query = stored_queries.get("q.pandas_query")

            try:
                local_vars = {"df": self.to_pandas(), "pd": pd}
                exec(pandas_query, globals(), local_vars)
                result = local_vars.get("result", self.to_pandas())
                if not isinstance(result, pd.DataFrame):
                    raise ValueError(
                        "The stored code did not return a pandas DataFrame"
                    )
                return pl.from_pandas(result)
            except Exception as e:
                logger.error(f"Error executing stored pandas query: {str(e)}")

        return None

    async def _cache_transform(
        self, query_key: str, transformed_df: pl.DataFrame, pandas_code: str
    ):
        logger.debug(f"Caching transformed data for query key: {query_key}")
        driver = await self._get_driver()
        async with driver.session() as session:
            # Prepare the query
            query = """
            MERGE (q:QueryNode {key: $query_key})
            SET q.pandas_query = $pandas_code
            """

            # Execute the query
            try:
                await session.run(
                    query,
                    query_key=query_key,
                    pandas_code=pandas_code,
                )
                logger.info(f"Successfully cached transform for query key: {query_key}")
            except Exception as e:
                logger.error(
                    f"Error caching transform for query key {query_key}: {str(e)}"
                )

    async def transform(self, natural_query: str) -> "MagicTable":
        # Generate a unique key for this transformation
        query_key = self._generate_query_key(natural_query, [])

        # Check for cached result
        cached_result = await self._get_cached_transform(query_key)
        if cached_result is not None:
            logger.info("Using cached transform result")
            return MagicTable(cached_result)

        logger.info("No cached transform result found, generating new result")
        result_df, pandas_code = await self._generate_and_execute_pandas_code(
            self.to_pandas(), natural_query
        )
        result_df = pl.from_pandas(result_df)
        if pandas_code is not None:
            # Cache the result
            await self._cache_transform(query_key, result_df, pandas_code)

        return MagicTable(result_df)
