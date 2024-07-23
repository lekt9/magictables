import os
import dateparser
import polars as pl
import requests
import json
from typing import Dict, Any, Optional, List, Tuple
from neo4j import GraphDatabase, Query
import hashlib

from magictables.utils import call_ai_model, flatten_nested_structure
from dotenv import load_dotenv

load_dotenv()


class MagicDataFrame(pl.DataFrame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.openai_api_key = "your_openai_api_key_here"
        self.neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.neo4j_user = os.getenv("NEO4J_USER", "neo4j")
        self.neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        self.jina_api_key = os.getenv("JINA_API_KEY")
        self.api_urls = []

    @classmethod
    def from_polars(cls, df: pl.DataFrame, label: str) -> "MagicDataFrame":
        magic_df = cls(df)
        # Generate a dummy API URL and description for consistency
        api_url = f"local://{label}"
        description = f"Local DataFrame: {label}"
        embedding = magic_df._generate_embedding(description)
        magic_df._store_in_neo4j(label, api_url, description, embedding)
        return magic_df

    @classmethod
    def from_api(
        cls, api_url: str, params: Optional[Dict[str, Any]] = None
    ) -> "MagicDataFrame":
        response = requests.get(api_url, params=params)
        data = response.json()

        data = flatten_nested_structure(data)

        df = cls(data)

        label = api_url.split("/")[-1].capitalize()

        description = df._generate_api_description(api_url, data)
        embedding = df._generate_embedding(description)

        df._store_in_neo4j(label, api_url, description, embedding)
        df.api_urls.append(api_url)
        return df

    def _generate_api_description(
        self, api_url: str, data: List[Dict[str, Any]]
    ) -> str:
        prompt = (
            """Generate a concise description of this API endpoint based on the URL and data sample.

    Examples:
    1. URL: https://api.example.com/users
    Data: [{"id": 1, "name": "John Doe", "email": "john@example.com"}]
    Description: "This API endpoint provides user information including user ID, name, and email address."

    2. URL: https://api.example.com/products
    Data: [{"id": 101, "name": "Laptop", "price": 999.99, "category": "Electronics"}]
    Description: "This API endpoint returns product details such as product ID, name, price, and category."

    3. URL: https://api.example.com/orders
    Data: [{"order_id": "ORD-001", "user_id": 1, "total": 1299.99, "status": "shipped"}]
    Description: "This API endpoint provides order information including order ID, associated user ID, total amount, and current status."

    Please provide a similar concise description for the given API endpoint:"""
            + api_url
        )

        response = call_ai_model(data, prompt)
        return response.get("description", "Error generating API description")

    def _generate_embedding(self, text: str) -> List[float]:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.jina_api_key}",
        }
        data = {
            "model": "jina-clip-v1",
            "embedding_type": "float",
            "input": [{"text": text}],
        }
        response = requests.post(
            "https://api.jina.ai/v1/embeddings", headers=headers, json=data
        )
        return response.json()["data"][0]["embedding"]

    def _store_in_neo4j(
        self, label: str, api_url: str, description: str, embedding: List[float]
    ):
        driver = GraphDatabase.driver(
            self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password)
        )

        with driver.session() as session:
            session.run(
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
                node_id = self._generate_node_id(label, standardized_row)
                query = Query(
                    f"""
                    MERGE (d:{label} {{id: $node_id}})
                    SET d += $row
                    WITH d
                    MATCH (a:APIEndpoint {{url: $api_url}})
                    MERGE (d)-[:SOURCED_FROM]->(a)
                    """  # type: ignore
                )
                session.run(
                    query,
                    node_id=node_id,
                    row=standardized_row,
                    api_url=api_url,
                )

        driver.close()

    @staticmethod
    def _generate_node_id(label: str, data: Dict[str, Any]) -> str:
        key_fields = ["id", "uuid", "name", "email"]
        key_data = {k: v for k, v in data.items() if k in key_fields}

        if not key_data:
            key_data = data

        data_str = json.dumps(key_data, sort_keys=True)
        hash_object = hashlib.md5((label + data_str).encode())
        return hash_object.hexdigest()

    def _search_relevant_api_urls(
        self, query: str, top_k: int = 3
    ) -> List[Tuple[str, str, float]]:
        query_embedding = self._generate_embedding(query)

        driver = GraphDatabase.driver(
            self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password)
        )

        with driver.session() as session:
            result = session.run(
                """
                MATCH (a:APIEndpoint)
                WITH a, gds.similarity.cosine(a.embedding, $query_embedding) AS similarity
                ORDER BY similarity DESC
                LIMIT $top_k
                RETURN a.url, a.description, similarity
                """,
                query_embedding=query_embedding,
                top_k=top_k,
            )

            relevant_urls = [
                (record["a.url"], record["a.description"], record["similarity"])
                for record in result
            ]

        driver.close()
        return relevant_urls

    def _generate_query_key(
        self, natural_query: str, relevant_urls: List[Tuple[str, str, float]]
    ) -> str:
        # Sort the URLs to ensure consistency
        sorted_urls = sorted([url for url, _, _ in relevant_urls])
        # Combine the query and URLs into a single string
        combined_string = f"{natural_query}|{'|'.join(sorted_urls)}"
        # Generate a hash of the combined string
        return hashlib.md5(combined_string.encode()).hexdigest()

    def _generate_cypher_query(self, natural_query: str) -> str:
        relevant_urls = self._search_relevant_api_urls(natural_query)
        query_key = self._generate_query_key(natural_query, relevant_urls)

        # Check if we have a stored query for this key
        stored_query = self._get_stored_cypher_query(query_key)
        if stored_query:
            return stored_query

        # Get a sample of the DataFrame (e.g., first 5 rows)
        sample_data = self.head(5).to_dicts()

        # Format relevant URLs for the prompt
        url_info = "\n".join(
            [
                f"URL: {url}, Description: {desc}, Similarity: {sim:.2f}"
                for url, desc, sim in relevant_urls
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
        Cypher Query: "MATCH (n:User) WHERE toInteger(n.age) > 30 RETURN n"

        2. Natural Query: "Get all products with price less than 100"
        Cypher Query: "MATCH (p:Product) WHERE toFloat(p.price) < 100 RETURN p"

        3. Natural Query: "List all orders made in the last month"
        Cypher Query: "MATCH (o:Order) WHERE apoc.date.parse(o.date, 'ms', 'yyyy-MM-dd') >= apoc.date.parse(date() - duration('P1M'), 'ms', 'yyyy-MM-dd') RETURN o"

        Natural Query: "{natural_query}"

        Please provide a Cypher query for the given natural language query, considering the relevant API URLs, sample data, and column data types."""

        response = call_ai_model(sample_data, prompt)
        cypher_query = response.get("cypher_query", "Error generating Cypher query")
        self._store_cypher_query(query_key, cypher_query)

        return cypher_query

    def join_with_query(self, natural_query: str) -> "MagicDataFrame":
        cypher_query = self._generate_cypher_query(natural_query)
        result_df = self._execute_cypher(cypher_query)
        self._store_cypher_query_as_edge(natural_query, cypher_query)
        return MagicDataFrame(self.join(result_df, how="left"))

    def _execute_cypher(self, query: str, params: Dict[str, Any] = {}) -> pl.DataFrame:
        driver = GraphDatabase.driver(
            self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password)
        )

        with driver.session() as session:
            result = session.run(query, params)  # type: ignore
            records = [dict(record) for record in result]

        driver.close()
        return pl.DataFrame(records)

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
            else:
                standardized_data[key] = value
        return standardized_data

    def _store_cypher_query_as_edge(self, natural_query: str, cypher_query: str):
        relevant_urls = self._search_relevant_api_urls(natural_query)
        query_key = self._generate_query_key(natural_query, relevant_urls)

        driver = GraphDatabase.driver(
            self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password)
        )

        with driver.session() as session:
            # Create a QueryNode for the natural language query
            session.run(
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
                session.run(
                    f"""
                    MATCH (q:QueryNode {{query: $natural_query}}), (n:{node_label})
                    MERGE (q)-[r:QUERIES]->(n)
                    SET r.cypher_query = $cypher_query
                    """,  # type: ignore
                    natural_query=natural_query,
                    cypher_query=cypher_query,
                )

        driver.close()

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
    def from_query(cls, natural_query: str) -> "MagicDataFrame":
        instance = cls()
        cypher_query = instance._generate_cypher_query(natural_query)
        result_df = instance._execute_cypher(cypher_query)
        instance._store_cypher_query_as_edge(natural_query, cypher_query)
        return cls(result_df)

    def _store_cypher_query(self, query_key: str, cypher_query: str):
        driver = GraphDatabase.driver(
            self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password)
        )

        with driver.session() as session:
            session.run(
                """
                MERGE (q:QueryNode {key: $query_key})
                SET q.cypher_query = $cypher_query
                """,
                query_key=query_key,
                cypher_query=cypher_query,
            )

        driver.close()

    def _get_stored_cypher_query(self, query_key: str) -> Optional[str]:
        driver = GraphDatabase.driver(
            self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password)
        )

        with driver.session() as session:
            result = session.run(
                """
                MATCH (q:QueryNode {key: $query_key})
                RETURN q.cypher_query
                """,
                query_key=query_key,
            )
            stored_query = result.single()

        driver.close()

        return stored_query[0] if stored_query else None
