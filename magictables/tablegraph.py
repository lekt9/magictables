# tablegraph.py

import networkx as nx
import polars as pl
import json
import os
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from .magictablechain import MagicTableChain


class TableGraph:
    def __init__(self, backend="memory"):
        self.backend = backend
        self.graph = nx.MultiDiGraph()
        self.transformations = {}
        self.pickle_file = "table_graph.json"
        self.cache_expiry = timedelta(hours=1)

    def add_table(
        self,
        table_name: str,
        df: pl.DataFrame,
        metadata: Dict[str, Any],
        source_info: List[Dict[str, Any]],
    ):
        self.graph.add_node(
            table_name,
            df=df.to_dict(as_series=False),
            metadata=metadata,
            source_info=source_info,
            created_at=datetime.now(),
        )
        self._pickle_state()

    def add_chain(self, chain: MagicTableChain):
        self.graph.add_edge(
            chain.source_table,
            chain.api_result_table,
            key="api_result",
            chain_type=chain.chain_type,
            source_key=chain.source_key,
            target_key=chain.target_key,
            metadata=chain.metadata,
            created_at=chain.created_at,
        )
        self.graph.add_edge(
            chain.api_result_table,
            chain.merged_result_table,
            key="merged_result",
            chain_type=chain.chain_type,
            source_key=chain.source_key,
            target_key=chain.target_key,
            metadata=chain.metadata,
            created_at=chain.created_at,
        )
        self._pickle_state()

    def get_chains(self, table_name: Optional[str] = None) -> List[MagicTableChain]:
        if table_name:
            edges = list(self.graph.in_edges(table_name, data=True, keys=True)) + list(
                self.graph.out_edges(table_name, data=True, keys=True)
            )
        else:
            edges = list(self.graph.edges(data=True, keys=True))

        chains = []
        for source, target, key, data in edges:
            if key == "api_result":
                merged_result = next(
                    (
                        t
                        for _, t, k in self.graph.out_edges(target, keys=True)
                        if k == "merged_result"
                    ),
                    None,
                )
                if merged_result:
                    chains.append(
                        MagicTableChain(
                            source,
                            target,
                            merged_result,
                            data["chain_type"],
                            data["source_key"],
                            data["target_key"],
                            data["metadata"],
                        )
                    )
        return chains

    def get_cached_chain_result(self, table_id: str) -> Optional[str]:
        if table_id in self.graph.nodes:
            node_data = self.graph.nodes[table_id]
            cache_time = node_data.get("created_at")
            if cache_time and datetime.now() - cache_time < self.cache_expiry:
                return table_id
        return None

    def get_table(self, table_name: str) -> Optional[Dict[str, Any]]:
        if table_name in self.graph.nodes:
            node_data = self.graph.nodes[table_name]
            return {
                "df": pl.DataFrame(node_data["df"]),
                "metadata": node_data["metadata"],
                "source_info": node_data["source_info"],
            }
        return None

    def query_or_fetch(
        self, table_id: str, conditions: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        if table_id in self.graph.nodes:
            df = pl.DataFrame(self.graph.nodes[table_id]["df"])
            if conditions:
                for col, value in conditions.items():
                    df = df.filter(pl.col(col) == value)
            return df.to_dicts()
        return []

    def _pickle_state(self):
        if self.backend == "memory":
            state = {
                "nodes": [
                    {
                        "name": node,
                        "df": data["df"],
                        "metadata": data["metadata"],
                        "source_info": data["source_info"],
                        "created_at": data["created_at"].isoformat(),
                    }
                    for node, data in self.graph.nodes(data=True)
                ],
                "edges": [
                    {
                        "source": source,
                        "target": target,
                        "key": key,
                        "data": {
                            k: v.isoformat() if isinstance(v, datetime) else v
                            for k, v in data.items()
                        },
                    }
                    for source, target, key, data in self.graph.edges(
                        data=True, keys=True
                    )
                ],
                "transformations": self.transformations,
            }
            with open(self.pickle_file, "w") as f:
                json.dump(state, f)

    def _unpickle_state(self):
        if self.backend == "memory" and os.path.exists(self.pickle_file):
            with open(self.pickle_file, "r") as f:
                state = json.load(f)

            self.graph = nx.MultiDiGraph()
            for node in state["nodes"]:
                self.graph.add_node(
                    node["name"],
                    df=node["df"],
                    metadata=node["metadata"],
                    source_info=node["source_info"],
                    created_at=datetime.fromisoformat(node["created_at"]),
                )

            for edge in state["edges"]:
                edge_data = edge["data"]
                edge_data["created_at"] = datetime.fromisoformat(
                    edge_data["created_at"]
                )
                self.graph.add_edge(
                    edge["source"], edge["target"], key=edge["key"], **edge_data
                )

            self.transformations = state["transformations"]

    def __enter__(self):
        self._unpickle_state()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._pickle_state()
