import logging

from magictables.hybrid_driver import HybridDriver

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
import asyncio
import os
import aiohttp
import dateparser
import polars as pl
import json
from typing import Dict, Any, Optional, List, Tuple, Union
from neo4j import Query
import hashlib

from magictables.utils import call_ai_model, flatten_nested_structure
from dotenv import load_dotenv
import urllib.parse
import pandas as pd

load_dotenv()


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
        if self._driver:
            logging.warning(
                "MagicTable instance is being destroyed without properly closing the driver. "
                "This may lead to resource leaks. Please use 'await magic_table.close()' "
                "to properly close the driver."
            )

    @classmethod
    async def from_polars(cls, df: pl.DataFrame, label: str) -> "MagicTable":
        magic_df = cls(df)
        api_url = f"local://{label}"
        description = f"Local DataFrame: {label}"
        embedding = await magic_df._generate_embedding(description)
        await magic_df._store_in_neo4j(label, api_url, description, embedding)
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

        api_description = await df._generate_api_description(api_url, data[:1])

        logger.debug(f"Storing data for API URL: {api_url}")
        await df._store_rows_in_neo4j(label, api_url, api_description)

        df.api_urls.append(api_url)
        return df._parse_json_fields()

    async def _store_rows_in_neo4j(
        self,
        label: str,
        api_url: str,
        api_description: str,
        transform_query: Optional[str] = None,
        pandas_code: Optional[str] = None,
    ):
        sanitized_label = self._sanitize_label(label)
        logger.debug(f"Storing rows with label: {sanitized_label}, API URL: {api_url}")

        # Generate query_key
        query_key = self._generate_query_key(
            transform_query or "", [[(api_url, api_description, 1.0)]]
        )

        # Prepare all rows at once
        rows = []
        for row in self.to_dicts():
            standardized_row = self._standardize_data_types(row)
            node_id = self._generate_node_id(sanitized_label, standardized_row)
            combined_text = f"{api_description} {json.dumps(standardized_row)}"
            embedding = await self._generate_embedding(combined_text)
            rows.append(
                {
                    "node_id": node_id,
                    "row": standardized_row,
                    "embedding": embedding,
                }
            )

        # Construct a single Cypher query for bulk insert
        query = Query(
            f"""
            MERGE (a:APIEndpoint {{url: $api_url}})
            SET a.description = $api_description
            WITH a
            UNWIND $rows AS row
            MERGE (d:{sanitized_label} {{id: row.node_id}})
            SET d += row.row,
                d.embedding = row.embedding
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
            """  # type: ignore
        )

        driver = await self._get_driver()
        async with driver.session() as session:
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
URL: {api_url}
Data: {json.dumps(data[:1])}

Your response should be in the following JSON format:
{{"description": "Your concise description here"}}
"""

        response = await call_ai_model(data, prompt)
        return response.get("description", "Error generating API description")

    async def _generate_embedding(self, text: str) -> List[float]:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.jina_api_key}",
        }
        data = {
            "model": "jina-embeddings-v2-base-en",
            "embedding_type": "float",
            "input": [text],
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.jina.ai/v1/embeddings", headers=headers, json=data
            ) as response:
                result = await response.json()
                return result["data"][0]["embedding"]

    @staticmethod
    def _sanitize_label(label: str) -> str:
        # Ensure the label starts with a letter
        if not label[0].isalpha():
            label = "N" + label
        # Replace any non-alphanumeric characters with underscores
        return "".join(c if c.isalnum() else "_" for c in label)

    @staticmethod
    def _generate_node_id(label: str, data: Dict[str, Any]) -> str:
        key_fields = ["id", "uuid", "name", "email"]
        key_data = {k: v for k, v in data.items() if k in key_fields}

        if not key_data:
            key_data = data

        data_str = json.dumps(key_data, sort_keys=True)
        hash_object = hashlib.md5((label + data_str).encode())
        return hash_object.hexdigest()

    async def _search_relevant_api_urls(
        self, queries: Union[str, List[str]], top_k: int = 3
    ) -> List[List[Tuple[str, str, float]]]:
        if isinstance(queries, str):
            queries = [queries]

        async def process_query(query):
            query_embedding = await self._generate_embedding(query)

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

    async def _generate_cypher_query(self, natural_query: str) -> str:
        relevant_urls = await self._search_relevant_api_urls(natural_query)
        query_key = self._generate_query_key(natural_query, relevant_urls)

        # Check if we have a stored query for this key
        stored_query = await self._get_stored_cypher_query(query_key)
        if stored_query:
            return stored_query

        # Get a sample of the DataFrame (e.g., first 5 rows)
        sample_data = self.head(5).to_dicts()

        # Format relevant URLs for the prompt
        url_info = "\n".join(
            [
                f"URL: {url}, Description: {desc}, Similarity: {sim:.2f}"
                for url, desc, sim in relevant_urls[0]  # Use the first list of results
            ]
        )
        column_types = self.dtypes
        type_info = "\n".join(
            [f"{self.columns[i]}: {dtype}" for i, dtype in enumerate(column_types)]
        )

        prompt = f"""Generate a Cypher query based on the natural language query, considering the provided columns, sample data, relevant API URLs, and column data types.

        Relevant API URLs:
        {url_info}

        Sample Data:
        {json.dumps(sample_data, indent=2)}

        Column Data Types:
        {type_info}

        When joining or comparing columns, use appropriate type conversion functions if needed. For example:
        - For dates, use: apoc.date.parse(column, 'ms', 'yyyy-MM-dd') for consistent comparison
        - For numbers, use: toFloat(column) for consistent numeric comparison
        - For strings, use: toLower(trim(column)) for case-insensitive, trimmed comparison

        Examples:
        1. Natural Query: "Find all users older than 30"
        {{"cypher_query": "MATCH (n:User) WHERE toInteger(n.age) > 30 RETURN n"}}

        2. Natural Query: "Get all products with price less than 100"
        {{"cypher_query": "MATCH (p:Product) WHERE toFloat(p.price) < 100 RETURN p"}}

        3. Natural Query: "List all orders made in the last month"
        {{"cypher_query": "MATCH (o:Order) WHERE apoc.date.parse(o.date, 'ms', 'yyyy-MM-dd') >= apoc.date.parse(date() - duration('P1M'), 'ms', 'yyyy-MM-dd') RETURN o"}}

        Natural Query: "{natural_query}"

        Please provide a Cypher query for the given natural language query, considering the relevant API URLs, sample data, and column data types.
        Your response should be in the following JSON format:
        {{"cypher_query": "Your Cypher query here"}}
        """
        response = await call_ai_model(sample_data, prompt)
        cypher_query = response.get("cypher_query")
        if cypher_query:
            await self._store_cypher_query(query_key, cypher_query)
        return cypher_query  # type: ignore

    async def join_with_query(self, natural_query: str) -> "MagicTable":
        cypher_query = await self._generate_cypher_query(natural_query)
        result_df = await self._execute_cypher(cypher_query)
        await self._store_cypher_query_as_edge(natural_query, cypher_query)

        # Try to use the key column first
        key_column = self._identify_key_column(
            self.api_urls[-1]
        )  # Assuming the last API URL is relevant

        if key_column and key_column in result_df.columns:
            join_columns = [key_column]
        else:
            # Fallback to AI-assisted column identification
            join_columns = await self._identify_join_columns(MagicTable(result_df))

        if not join_columns:
            raise ValueError("No suitable columns found for joining the DataFrames")

        # Perform the join operation
        joined_df = self.join(result_df, on=join_columns, how="left")

        return MagicTable(joined_df)

    async def _identify_join_columns(self, result_df: "MagicTable") -> List[str]:
        # Get a sample of both DataFrames
        self_sample = self.head(5).to_dicts()
        result_sample = result_df.head(5).to_dicts()

        # Prepare column information
        self_columns = {
            col: str(dtype) for col, dtype in zip(self.columns, self.dtypes)
        }
        result_columns = {
            col: str(dtype) for col, dtype in zip(result_df.columns, result_df.dtypes)
        }

        prompt = f"""Given two DataFrames, identify the best columns to use for joining them.

        DataFrame 1 Columns and Types:
        {json.dumps(self_columns, indent=2)}

        DataFrame 1 Sample Data:
        {json.dumps(self_sample, indent=2)}

        DataFrame 2 Columns and Types:
        {json.dumps(result_columns, indent=2)}

        DataFrame 2 Sample Data:
        {json.dumps(result_sample, indent=2)}

        Please provide a list of column names that are best suited for joining these DataFrames.
        Consider semantic similarity, data types, and potential primary key relationships.
        Your response should be in the following JSON format:
        {{"join_columns": ["column1", "column2"]}}
        """
        response = await call_ai_model(self_sample + result_sample, prompt)
        join_columns = json.loads(response.get("join_columns", "[]"))

        return join_columns

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
        {{"pandas_code": "Your Python code here"}}
        """
            response = await call_ai_model(
                [],
                prompt,
                model="openrouter/anthropic/claude-3.5-sonnet:beta",
            )

            pandas_code = response.get("pandas_code", "df")

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

    async def chain(
        self,
        api_url: Union[str, Dict[str, str]],
        key: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        expand: bool = False,
    ) -> "MagicTable":
        if isinstance(api_url, str):
            if key is None:
                key = await self._identify_key_column(api_url)
            api_url_dict = {key: api_url}
        else:
            api_url_dict = api_url

        async def process_row(row: Dict[str, Any]) -> List[Dict[str, Any]]:
            results = []
            for col, url_template in api_url_dict.items():
                key_value = row[col]
                url = url_template.format(**{col: key_value})
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(url, params=params) as response:
                            data = await response.json()
                    flattened_data = flatten_nested_structure(data)
                    if expand and isinstance(flattened_data, list):
                        for item in flattened_data:
                            item[key] = key_value
                            results.append(item)
                    else:
                        results = flattened_data if flattened_data else [{}]
                        for item in results:
                            item[key] = key_value
                except Exception as e:
                    # print(f"Error fetching data for {col}: {str(e)}")
                    results.append({key: key_value})
            return results

        async def process_all_rows():
            tasks = [process_row(row) for row in self.to_dicts()]
            return await asyncio.gather(*tasks)

        try:
            results = await process_all_rows()
            flattened_results = [item for sublist in results for item in sublist]

            # Create a new DataFrame with the flattened API results
            api_df = pl.DataFrame(flattened_results)

            # Get the set of existing column names
            existing_columns = set(self.columns)

            # Keep only new columns and the key column from api_df
            columns_to_keep = [
                col
                for col in api_df.columns
                if col not in existing_columns or col == key
            ]
            api_df = api_df.select(columns_to_keep)

            # Ensure the key column exists in both DataFrames
            if key not in self.columns or key not in api_df.columns:
                raise ValueError(f"Key column '{key}' not found in both DataFrames")

            # Join the original DataFrame with the API results
            if expand:
                joined_df = self.join(api_df, on=key, how="left")
            else:
                joined_df = self.join(api_df, on=key, how="left")

            return MagicTable(joined_df)

        except Exception as e:
            # print(f"Error during chaining: {str(e)}")
            return self  # Return the original DataFrame if chaining fails

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

    async def _identify_key_column(self, api_url_template: str) -> str:
        """
        Identify the most suitable key column for the given API URL template.

        :param api_url_template: The API URL template to analyze.
        :return: The name of the identified key column.
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

        # If we have a single match for a placeholder, return it
        if len(matches) == 1 and len(next(iter(matches.values()))) == 1:
            return next(iter(matches.values()))[0]

        # If we have multiple matches or no matches, use AI to decide
        if not matches or any(len(cols) > 1 for cols in matches.values()):
            prompt = f"""Given the following API URL template and the current DataFrame structure, 
    identify the most suitable column to use as a key for chaining API calls.

    API URL Template: {api_url_template}

    Placeholders: {placeholders}

    DataFrame Columns and Types:
    {json.dumps(column_info, indent=2)}

    Potential Matches:
    {json.dumps(matches, indent=2)}

    Please provide the name of the column that best matches the placeholder in the API URL template.
    Your response should be in the following JSON format:
    {{"column_name": "example_column"}}
    Replace "example_column" with the actual column name you identify as the best match.
    """

            response = await call_ai_model([column_info], prompt)
            key_column = response.get("column_name")

            if not key_column or key_column not in self.columns:
                raise ValueError(
                    f"Unable to identify a suitable key column for the given API URL template: {api_url_template}"
                )

            return key_column

        # If we have a single match for each placeholder, return the first one
        return next(iter(matches.values()))[0]

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

            for row in self.to_dicts():
                standardized_row = self._standardize_data_types(row)
                node_id = self._generate_node_id(sanitized_label, standardized_row)
                query = Query(
                    f"""
                    MERGE (d:{sanitized_label} {{id: $node_id}})
                    SET d += $row
                    WITH d
                    MATCH (a:APIEndpoint {{url: $api_url}})
                    MERGE (d)-[:SOURCED_FROM]->(a)
                    """  # type: ignore
                )
                await session.run(
                    query,
                    node_id=node_id,
                    row=standardized_row,
                    api_url=api_url,
                )

    async def _store_api_metadata(
        self, api_url: str, sample_data: List[Dict[str, Any]]
    ):
        driver = await self._get_driver()
        async with driver.session() as session:
            # Check if API metadata already exists
            result = await session.run(
                "MATCH (n:APIEndpoint {url: $api_url}) RETURN n", api_url=api_url
            )
            if not await result.single():
                # If not, generate and store it
                description = self._generate_api_description(api_url, sample_data)
                await session.run(
                    """
                    MERGE (n:APIEndpoint {url: $api_url})
                    SET n.description = $description
                    """,
                    api_url=api_url,
                    description=description,
                )

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
            # print(f"An error occurred: {exc_type.__name__}: {exc_val}")
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
            pandas_query = stored_queries.data["q.pandas_code"]

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
