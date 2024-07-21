import numpy as np
from tinydb import TinyDB, Query
import polars as pl
from typing import List, Dict, Any, Optional, Tuple
import json
from datetime import datetime
import os
import requests
from magictables.utils import call_ai_model


class GraphNode:
    def __init__(
        self, id: str, data: Dict[str, Any], embedding: Optional[List[float]] = None
    ):
        self.id = id
        self.data = data
        self.embedding = embedding


class GraphEdge:
    def __init__(self, source: str, target: str, relationship: str):
        self.source = source
        self.target = target
        self.relationship = relationship


class MagicTable:
    _instance = None
    _index = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        db_path = os.path.join(os.getcwd(), "magic.json")
        self.db = TinyDB(db_path)
        self._create_system_tables()
        self._initialize_index()
        self.last_analysis_result = None
        self.jina_api_key = (
            "jina_bb47710a966b4a5dbe209b7e4df0a546B5Tg812DLcdbCccNNwivqeK6kX4m"
        )

    def _create_system_tables(self):
        self.nodes = self.db.table("nodes")
        self.edges = self.db.table("edges")
        self.routes = self.db.table("routes")
        self.cached_results = self.db.table("cached_results")
        self.cached_relationships = self.db.table("cached_relationships")

    def _initialize_index(self):
        if self._index is None:
            self._index = []  # Initialize as an empty list

    def _data_to_text(self, data: Dict[str, Any]) -> str:
        """Convert a dictionary of data to a string representation."""
        return json.dumps(data, sort_keys=True)

    def _text_to_embedding(self, text: str) -> List[float]:
        """Convert text to an embedding using Jina AI API."""
        url = "https://api.jina.ai/v1/embeddings"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.jina_api_key}",
        }
        data = {
            "model": "jina-embeddings-v2-base-en",
            "embedding_type": "float",
            "input": [text],
        }
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()["data"][0]["embedding"]

    def add_node(self, node: GraphNode):
        text_representation = self._data_to_text(node.data)
        embedding = self._text_to_embedding(text_representation)
        self._index.append(embedding)
        self.nodes.insert(
            {"id": node.id, "data": node.data, "embedding_id": len(self._index) - 1}
        )

    def add_edge(self, edge: GraphEdge):
        self.edges.insert(
            {
                "source": edge.source,
                "target": edge.target,
                "relationship": edge.relationship,
            }
        )

    def get_node(self, node_id: str) -> Optional[GraphNode]:
        Node = Query()
        result = self.nodes.get(Node.id == node_id)
        if result:
            return GraphNode(result["id"], result["data"], None)
        return None

    def get_neighbors(self, node_id: str) -> List[GraphNode]:
        Edge = Query()
        neighbor_edges = self.edges.search(
            (Edge.source == node_id) | (Edge.target == node_id)
        )
        neighbor_ids = set(
            [edge["source"] for edge in neighbor_edges if edge["target"] == node_id]
            + [edge["target"] for edge in neighbor_edges if edge["source"] == node_id]
        )
        return [self.get_node(nid) for nid in neighbor_ids if nid != node_id]

    def add_route(self, source_name: str, route_name: str, url: str, query: str):
        self.routes.insert(
            {
                "source_name": source_name,
                "route_name": route_name,
                "url": url,
                "query": query,
            }
        )

    def get_route(self, source_name: str, route_name: str) -> Dict[str, str]:
        Route = Query()
        result = self.routes.get(
            (Route.source_name == source_name) & (Route.route_name == route_name)
        )
        return result if result else {}

    def cache_result(self, source_name: str, call_id: str, result: Dict[str, Any]):
        timestamp = str(datetime.now())
        self.cached_results.insert(
            {
                "source_name": source_name,
                "call_id": call_id,
                "result": result,
                "timestamp": timestamp,
            }
        )

    def get_cached_result(
        self, source_name: str, call_id: str
    ) -> Optional[Dict[str, Any]]:
        CachedResult = Query()
        result = self.cached_results.get(
            (CachedResult.source_name == source_name)
            & (CachedResult.call_id == call_id)
        )
        return result["result"] if result else None

    def cache_relationships(
        self, cache_key: str, relationships: List[Tuple[str, str, float]]
    ):
        self.cached_relationships.upsert(
            {
                "cache_key": cache_key,
                "relationships": relationships,
                "timestamp": str(datetime.now()),
            },
            Query().cache_key == cache_key,
        )

    def get_cached_relationships(
        self, cache_key: str
    ) -> Optional[List[Tuple[str, str, float]]]:
        CachedRelationships = Query()
        result = self.cached_relationships.get(
            CachedRelationships.cache_key == cache_key
        )
        return result["relationships"] if result else None

    def invalidate_cache(self, source_name: str):
        CachedResult = Query()
        self.cached_results.remove(CachedResult.source_name == source_name)

    def to_dataframe(self, table_name: str) -> pl.DataFrame:
        table = self.db.table(table_name)
        return pl.DataFrame(table.all())

    def get_relationships(self, source: str) -> List[Dict[str, str]]:
        Edge = Query()
        edges = self.edges.search(Edge.source == source)
        return [
            {"target": edge["target"], "relationship": edge["relationship"]}
            for edge in edges
        ]

    def add_relationship(self, source: str, target: str, relationship: str):
        self.edges.insert(
            {"source": source, "target": target, "relationship": relationship}
        )

    def find_similar_nodes(
        self, query_data: Dict[str, Any], top_k: int = 5
    ) -> List[GraphNode]:
        query_text = self._data_to_text(query_data)
        query_embedding = self._text_to_embedding(query_text)

        similarities = []
        for idx, embedding in enumerate(self._index):
            similarity = np.dot(query_embedding, embedding) / (
                np.linalg.norm(query_embedding) * np.linalg.norm(embedding)
            )
            similarities.append((idx, similarity))

        top_k_indices = sorted(similarities, key=lambda x: x[1], reverse=True)[:top_k]

        similar_nodes = []
        for idx, _ in top_k_indices:
            Node = Query()
            node = self.nodes.get(Node.embedding_id == idx)
            if node:
                similar_nodes.append(GraphNode(node["id"], node["data"], None))

        return similar_nodes

    def smart_parse(self, data: Dict[str, Any]) -> pl.DataFrame:
        input_data = {"raw_data": data}
        prompt = "Parse the given raw data. Identify key entities, attributes, and relationships. Return a JSON object with the parsed and structured data."
        result = call_ai_model(input_data, prompt)

        return pl.DataFrame(result)

    def analyze(self, query: str) -> pl.DataFrame:
        input_data = {
            "data": self.to_dataframe("nodes").to_dict(as_series=False),
            "query": query,
        }
        prompt = f"Analyze the given data based on the query: {query}. Provide insights, patterns, and recommendations. Return a JSON object with the analysis results."
        result = call_ai_model(input_data, prompt)

        # Convert the AI model's response to a DataFrame
        return pl.DataFrame(result)

    def get_analysis_result(self) -> str:
        return (
            self.last_analysis_result
            if self.last_analysis_result
            else "No analysis has been performed yet."
        )

    def get_related(self, node_id: str, relationship: str) -> List[GraphNode]:
        Edge = Query()
        related_edges = self.edges.search(
            (Edge.source == node_id) & (Edge.relationship == relationship)
        )
        related_nodes = [self.get_node(edge["target"]) for edge in related_edges]
        return related_nodes
