import asyncio
import os
import aiohttp
import dateparser
import polars as pl
import json
from typing import Dict, Any, Optional, List, Tuple, Union
from neo4j import Query, basic_auth
import hashlib
from neo4j import AsyncGraphDatabase, AsyncDriver, basic_auth

from magictables.utils import call_ai_model, flatten_nested_structure
from dotenv import load_dotenv
import urllib.parse

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

    async def _get_driver(self) -> AsyncDriver:
        if self._driver is None:
            parsed_uri = urllib.parse.urlparse(self.neo4j_uri)
            scheme = parsed_uri.scheme
            if scheme in ["bolt", "neo4j", "bolt+s"]:
                self._driver = AsyncGraphDatabase.driver(
                    self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password)
                )
            elif scheme in ["http", "https"]:
                self._driver = AsyncGraphDatabase.driver(
                    self.neo4j_uri,
                    auth=basic_auth(self.neo4j_user, self.neo4j_password),
                )
            else:
                raise ValueError(f"Unsupported URI scheme: {scheme}")
        return self._driver

    async def _close_driver(self):
        if self._driver is not None:
            await self._driver.close()
            self._driver = None

    async def __aenter__(self):
        await self._get_driver()
        return self

    def __del__(self):
        asyncio.create_task(self._close_driver())

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
        cls, api_url: str, params: Optional[Dict[str, Any]] = None
    ) -> "MagicTable":
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

        # Generate API description
        api_description = await df._generate_api_description(api_url, data[:1])

        # Store rows with API metadata and combined embeddings
        await df._store_rows_in_neo4j(label, api_url, api_description)

        df.api_urls.append(api_url)
        return df._parse_json_fields()

    async def _store_rows_in_neo4j(
        self, label: str, api_url: str, api_description: str
    ):
        sanitized_label = self._sanitize_label(label)

        async def process_row(row):
            standardized_row = self._standardize_data_types(row)
            node_id = self._generate_node_id(sanitized_label, standardized_row)

            combined_text = f"{api_description} {json.dumps(standardized_row)}"
            embedding = await self._generate_embedding(combined_text)

            query = Query(
                f"""
                MERGE (d:{sanitized_label} {{id: $node_id}})
                SET d += $row,
                    d.embedding = $embedding,
                    d.api_url = $api_url,
                    d.api_description = $api_description
                """  # type: ignore
            )
            return query, {
                "node_id": node_id,
                "row": standardized_row,
                "embedding": embedding,
                "api_url": api_url,
                "api_description": api_description,
            }

        # Generate all embeddings and prepare queries concurrently
        tasks = [process_row(row) for row in self.to_dicts()]
        prepared_queries = await asyncio.gather(*tasks)

        # Execute all Neo4j queries concurrently
        driver = await self._get_driver()
        async with driver.session() as session:
            await asyncio.gather(
                *[session.run(query, **params) for query, params in prepared_queries]
            )

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

    @classmethod
    async def from_query(cls, natural_query: str) -> "MagicTable":
        instance = cls()
        cypher_query = await instance._generate_cypher_query(natural_query)
        result_df = await instance._execute_cypher(cypher_query)
        await instance._store_cypher_query(natural_query, cypher_query)
        return cls(result_df)

    async def chain(
        self,
        api_url: Union[str, Dict[str, str]],
        key: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> "MagicTable":
        if isinstance(api_url, str):
            if key is None:
                key = await self._identify_key_column(api_url)
            api_url_dict = {key: api_url}
        else:
            api_url_dict = api_url

        print("key", key)

        async def process_row(row: Dict[str, Any]) -> Dict[str, Any]:
            results = {}
            for col, url_template in api_url_dict.items():
                key_value = row[col]
                url = url_template.format(**{col: key_value})
                try:
                    # Use from_api to fetch and process the data
                    chained_table = await MagicTable.from_api(url, params=params)
                    results[col] = chained_table
                except Exception as e:
                    print(f"Error fetching data for {col}: {str(e)}")
                    results[col] = None
            return {**row, **results}

        async def process_all_rows():
            tasks = [process_row(row) for row in self.to_dicts()]
            return await asyncio.gather(*tasks)

        try:
            results = await process_all_rows()

            # Flatten the results
            flattened_results = []
            for row in results:
                flat_row = {}
                for key, value in row.items():
                    if isinstance(value, MagicTable):
                        # If the value is a MagicTable, add its columns with a prefix
                        for col in value.columns:
                            flat_row[f"{key}_{col}"] = value[col].to_list()
                    else:
                        flat_row[key] = value
                flattened_results.append(flat_row)

            chained_df = MagicTable(flattened_results)

            # No need to join, as the results are already merged
            return chained_df

        except Exception as e:
            print(f"Error during chaining: {str(e)}")
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
                    MATCH (q:QueryNode {{query: $natural_query}}), (n:{node_label})
                    MERGE (q)-[r:QUERIES]->(n)
                    SET r.cypher_query = $cypher_query
                    """,  # type: ignore
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
                MATCH (q:QueryNode {key: $query_key})
                RETURN q.cypher_query
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
