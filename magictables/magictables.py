# magictables.py
from tinydb import TinyDB, Query
import polars as pl
from typing import List, Dict, Any, Optional
import json
from datetime import datetime
import os
import torch
from transformers import AutoTokenizer, AutoModel
import faiss


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


class ColBERTModel:
    def __init__(self, model_name="bert-base-uncased", max_length=128):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name).to(self.device)
        self.max_length = max_length

    def encode(self, texts: List[str]) -> torch.Tensor:
        inputs = self.tokenizer(
            texts,
            padding=True,
            truncation=True,
            max_length=self.max_length,
            return_tensors="pt",
        ).to(self.device)
        with torch.no_grad():
            outputs = self.model(**inputs)
        return outputs.last_hidden_state

    def similarity(
        self, query_embedding: torch.Tensor, doc_embeddings: torch.Tensor
    ) -> torch.Tensor:
        return torch.sum(
            torch.max(query_embedding @ doc_embeddings.transpose(-1, -2), dim=-1)[0],
            dim=-1,
        )


class MagicTable:
    _instance = None
    _model = None
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
        self._initialize_model()
        self.last_analysis_result = None

    def _create_system_tables(self):
        self.nodes = self.db.table("nodes")
        self.edges = self.db.table("edges")
        self.routes = self.db.table("routes")
        self.cached_results = self.db.table("cached_results")

    def _initialize_model(self):
        if self._model is None:
            self._model = ColBERTModel()
        if self._index is None:
            self._index = faiss.IndexFlatIP(self._model.model.config.hidden_size)

    def _data_to_text(self, data: Dict[str, Any]) -> str:
        """Convert a dictionary of data to a string representation."""
        return json.dumps(data, sort_keys=True)

    def _text_to_embedding(self, text: str) -> torch.Tensor:
        """Convert text to an embedding using ColBERT."""
        return self._model.encode([text]).mean(dim=1)  # Average over token dimension

    def add_node(self, node: GraphNode):
        text_representation = self._data_to_text(node.data)
        embedding = self._text_to_embedding(text_representation)
        self._index.add(embedding.cpu().numpy())
        self.nodes.insert(
            {"id": node.id, "data": node.data, "embedding_id": self._index.ntotal - 1}
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
            return GraphNode(
                result["id"], result["data"], None
            )  # We don't store the actual embedding in the database
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

    def cache_result(self, source_name: str, call_id: str, result: pl.DataFrame):
        timestamp = str(datetime.now())
        self.cached_results.insert(
            {
                "source_name": source_name,
                "call_id": call_id,
                "result": result.to_dict(as_series=False),
                "timestamp": timestamp,
            }
        )

    def get_cached_result(
        self, source_name: str, call_id: str
    ) -> Optional[pl.DataFrame]:
        CachedResult = Query()
        result = self.cached_results.get(
            (CachedResult.source_name == source_name)
            & (CachedResult.call_id == call_id)
        )
        if result:
            return pl.DataFrame(result["result"])
        return None

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

    def find_similar_nodes(
        self, query_data: Dict[str, Any], top_k: int = 5
    ) -> List[GraphNode]:
        query_text = self._data_to_text(query_data)
        query_embedding = self._text_to_embedding(query_text)

        # Perform similarity search
        scores, indices = self._index.search(query_embedding.cpu().numpy(), top_k)

        # Retrieve the corresponding nodes
        similar_nodes = []
        for idx in indices[0]:
            Node = Query()
            node = self.nodes.get(Node.embedding_id == idx)
            if node:
                similar_nodes.append(GraphNode(node["id"], node["data"], None))

        return similar_nodes

    def smart_join(self, description: str) -> pl.DataFrame:
        # This is a placeholder implementation for AI-powered data joining
        # In a real implementation, you would use an AI model to determine the join strategy
        print(f"Performing smart join based on description: {description}")
        # For now, we'll just return an empty DataFrame
        return pl.DataFrame()

    def analyze(self, query: str) -> str:
        # This is a placeholder implementation for AI-powered analysis
        # In a real implementation, you would use an AI model to perform the analysis
        analysis_result = f"Analysis result for query: {query}"
        self.last_analysis_result = analysis_result
        return analysis_result

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
