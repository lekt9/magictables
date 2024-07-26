# file: magictables/tablegraph.py

from __future__ import annotations
from typing import TYPE_CHECKING, Dict, Any, List, Optional, Tuple, Union
import logging
import os
import pickle
import networkx as nx
from py2neo import (
    Graph as Neo4jGraph,
    Node as Neo4jNode,
)
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta

if TYPE_CHECKING:
    from magictables.magictables import MagicTable

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class Node:
    def __init__(
        self,
        node_id: str,
        table_name: str,
        row_data: Dict[str, Any],
        metadata: Dict[str, Any],
        source: str,
    ):
        self.node_id = node_id
        self.table_name = table_name
        self.row_data = row_data
        self.metadata = metadata
        self.source = source

    def __repr__(self):
        return f"Node(id={self.node_id}, table={self.table_name}, data={self.row_data}, metadata={self.metadata}, source={self.source})"


class Edge:
    def __init__(
        self,
        source_table: str,
        target_table: str,
        relationship_type: str,
        properties: Dict[str, Any],
    ):
        self.source_table = source_table
        self.target_table = target_table
        self.relationship_type = relationship_type
        self.properties = properties

    def __repr__(self):
        return f"Edge(source={self.source_table}, target={self.target_table}, type={self.relationship_type}, props={self.properties})"


class TableGraph:
    def __init__(
        self,
        backend=None,
        neo4j_uri=None,
        neo4j_user=None,
        neo4j_password=None,
        pickle_file="magictable.pkl",
        cache_expiry: timedelta = timedelta(hours=1),
    ):
        load_dotenv()

        self.backend = backend or (
            "neo4j"
            if all(
                [
                    os.getenv("NEO4J_URI"),
                    os.getenv("NEO4J_USER"),
                    os.getenv("NEO4J_PASSWORD"),
                    os.getenv("LOCAL_GRAPH", "false") == "false",
                ]
            )
            else "memory"
        )

        self.pickle_file = pickle_file
        self.transformations = {}
        self.cache_expiry = cache_expiry

        if self.backend == "neo4j":
            neo4j_uri = neo4j_uri or os.getenv("NEO4J_URI")
            neo4j_user = neo4j_user or os.getenv("NEO4J_USER")
            neo4j_password = neo4j_password or os.getenv("NEO4J_PASSWORD")

            if not all([neo4j_uri, neo4j_user, neo4j_password]):
                raise ValueError(
                    "Neo4j credentials not found in environment variables."
                )

            try:
                self.graph = Neo4jGraph(neo4j_uri, auth=(neo4j_user, neo4j_password))
            except Exception as e:
                logger.error(f"Failed to connect to Neo4j: {str(e)}")
                logger.info("Falling back to memory backend.")
                self.backend = "memory"
                self.graph = nx.MultiDiGraph()
        elif self.backend == "memory":
            self.graph = nx.MultiDiGraph()
        else:
            raise ValueError("Unsupported backend. Use 'memory' or 'neo4j'.")

    def add_table(
        self,
        table_name: str,
        df: pd.DataFrame,
        metadata: Dict[str, Any] = None,
        source: str = None,
    ):
        from magictables import MagicTable

        metadata = metadata or {}
        metadata["cache_time"] = datetime.now().isoformat()

        # Create a MagicTable instance to use its @name property
        magic_table = MagicTable(df)

        # Create a list with a single tuple containing the table_name and DataFrame
        url_data_pairs = [(table_name, df, magic_table.name)]

        # Call add_tables_batch with this single-item list
        self.add_tables_batch(url_data_pairs, metadata=metadata, source=source)

    def add_tables_batch(
        self,
        url_data_pairs: List[Tuple[str, pd.DataFrame, str]],
        metadata: Dict[str, Any] = None,
        source: str = None,
    ):
        metadata = metadata or {}
        metadata["cache_time"] = datetime.now().isoformat()

        if self.backend == "neo4j":
            batch_data = [
                {
                    **row.to_dict(),
                    "node_id": table_name,
                    "table_name": url,
                    "source": source or "API",
                    **metadata,
                }
                for url, data, table_name in url_data_pairs
                for _, row in data.iterrows()
            ]

            query = """
            UNWIND $batch AS row
            CREATE (n:Row)
            SET n = row
            WITH n
            MATCH (t:Table {name: n.table_name})
            MERGE (n)-[:BELONGS_TO]->(t)
            """

            self.graph.run(query, batch=batch_data)

        elif self.backend == "memory":
            nodes_to_add = [
                (
                    table_name,
                    {
                        "table_name": url,
                        "row_data": row.to_dict(),
                        "metadata": metadata,
                        "source": source or "API",
                    },
                )
                for url, df, table_name in url_data_pairs
                for _, row in df.iterrows()
            ]
            self.graph.add_nodes_from(nodes_to_add)
            self._pickle_state()

        else:
            raise ValueError("Unsupported backend. Use 'memory' or 'neo4j'.")

    def add_nodes_batch(self, nodes: List[Node]):
        if self.backend == "memory":
            nodes_to_add = [(node.node_id, node.__dict__) for node in nodes]
            self.graph.add_nodes_from(nodes_to_add)
            self._pickle_state()
        elif self.backend == "neo4j":
            batch = [
                Neo4jNode(
                    "Row",
                    **{
                        **node.row_data,
                        **node.metadata,
                        "node_id": node.node_id,
                        "table_name": node.table_name,
                        "source": node.source,
                    },
                )
                for node in nodes
            ]
            self.graph.create(*batch)

    def add_relationships_batch(
        self, relationships: List[Tuple[str, str, str, Dict[str, Any]]]
    ):
        if self.backend == "memory":
            edges_to_add = [
                (source, target, {"relationship_type": rel_type, **properties})
                for source, target, rel_type, properties in relationships
            ]
            self.graph.add_edges_from(edges_to_add)
            self._pickle_state()
        elif self.backend == "neo4j":
            batch_query = """
            UNWIND $batch AS rel
            MATCH (source:Row {node_id: rel.source})
            MATCH (target:Row {node_id: rel.target})
            CREATE (source)-[r:RELATED_TO {type: rel.type}]->(target)
            SET r += rel.properties
            """
            batch_data = [
                {
                    "source": source,
                    "target": target,
                    "type": rel_type,
                    "properties": properties,
                }
                for source, target, rel_type, properties in relationships
            ]
            self.graph.run(batch_query, batch=batch_data)

    def get_nodes_batch(self, node_ids: List[str]) -> List[Optional[Node]]:
        if self.backend == "memory":
            return [
                (
                    Node(**self.graph.nodes[node_id])
                    if node_id in self.graph.nodes
                    else None
                )
                for node_id in node_ids
            ]
        elif self.backend == "neo4j":
            query = "MATCH (n:Row) WHERE n.node_id IN $node_ids RETURN n"
            result = self.graph.run(query, node_ids=node_ids).data()
            nodes = {
                node["n"]["node_id"]: Node(
                    node_id=node["n"]["node_id"],
                    table_name=node["n"]["table_name"],
                    row_data={
                        k: v
                        for k, v in node["n"].items()
                        if k not in ["node_id", "table_name", "source"]
                    },
                    metadata={},
                    source=node["n"]["source"],
                )
                for node in result
            }
            return [nodes.get(node_id) for node_id in node_ids]

    def get_nodes_batch_with_cache_check(
        self, node_ids: List[str]
    ) -> List[Tuple[str, Optional[Node]]]:
        nodes = self.get_nodes_batch(node_ids)
        current_time = datetime.now()
        return [
            (
                node_id,
                (
                    node
                    if (
                        node is not None
                        and current_time
                        - datetime.fromisoformat(
                            node.metadata.get("cache_time", "2000-01-01T00:00:00")
                        )
                        < self.cache_expiry
                    )
                    else None
                ),
            )
            for node_id, node in zip(node_ids, nodes)
        ]

    def query_or_fetch(
        self, table_name: str, conditions: Union[Dict[str, Any], str] = None
    ) -> Optional[List[Node]]:
        if conditions is None:
            conditions = {}

        if isinstance(conditions, str):
            # If conditions is a string, treat it as a table name
            conditions = {"table_name": conditions}

        query_result = self.query_nodes_batch([conditions])
        if query_result and query_result[0]:
            return query_result[0]
        return None

    def query_nodes_batch(self, conditions: List[Dict[str, Any]]) -> List[List[Node]]:
        if self.backend == "memory":
            results = []
            for condition in conditions:
                matching_nodes = [
                    Node(
                        node_id=node_id,
                        table_name=data["table_name"],
                        row_data=data["row_data"],
                        metadata=data["metadata"],
                        source=data["source"],
                    )
                    for node_id, data in self.graph.nodes(data=True)
                    if all(data["row_data"].get(k) == v for k, v in condition.items())
                ]
                results.append(matching_nodes)
            return results
        elif self.backend == "neo4j":
            results = []
            batch_query = """
            UNWIND $conditions AS condition
            MATCH (n:Row)
            WHERE all(key IN keys(condition) WHERE n[key] = condition[key])
            RETURN collect(n) AS nodes
            """
            batch_result = self.graph.run(batch_query, conditions=conditions).data()
            for result in batch_result:
                nodes = [
                    Node(
                        node_id=node["node_id"],
                        table_name=node["table_name"],
                        row_data={
                            k: v
                            for k, v in node.items()
                            if k not in ["node_id", "table_name", "source"]
                        },
                        metadata={},
                        source=node["source"],
                    )
                    for node in result["nodes"]
                ]
                results.append(nodes)
            return results

    def add_transformation(self, table_name: str, query: str, code: str):
        if table_name not in self.transformations:
            self.transformations[table_name] = {}
        self.transformations[table_name][query] = {
            "code": code,
            "timestamp": datetime.now().isoformat(),
        }
        self._pickle_state()

    def get_transformation(
        self, table_name: str, query: str
    ) -> Optional[Dict[str, str]]:
        if (
            table_name in self.transformations
            and query in self.transformations[table_name]
        ):
            transformation = self.transformations[table_name][query]
            cache_time = datetime.fromisoformat(transformation["timestamp"])
            if datetime.now() - cache_time < self.cache_expiry:
                return transformation
        return None

    async def execute_cypher(
        self, query: str, params: Dict[str, Any] = {}
    ) -> "MagicTable":
        if self.backend == "neo4j":
            try:
                from magictables.magictables import MagicTable

                result = self.graph.run(query, **params)
                return MagicTable(result.data())
            except Exception as e:
                logger.error(f"Error executing Cypher query: {str(e)}")
                raise
        else:
            logger.warning("Cypher queries are only supported with Neo4j backend")
            raise ValueError("Cypher queries are only supported with Neo4j backend")

    def _pickle_state(self):
        if self.backend == "memory":
            state = {
                "graph": self.graph,
                "transformations": self.transformations,
            }
            with open(self.pickle_file, "wb") as f:
                pickle.dump(state, f)

    def _unpickle_state(self):
        if self.backend == "memory" and os.path.exists(self.pickle_file):
            with open(self.pickle_file, "rb") as f:
                state = pickle.load(f)
                logging.debug(state)
                self.graph = state["graph"]
                self.transformations = state["transformations"]

    def __enter__(self):
        self._unpickle_state()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._pickle_state()
