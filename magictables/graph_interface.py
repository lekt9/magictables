# graph_interface.py

from abc import ABC, abstractmethod
import polars as pl
from typing import List, Tuple, Dict, Any

class GraphInterface(ABC):
    @abstractmethod
    def add_dataframe(self, name: str, df: pl.DataFrame, source_name: str):
        pass

    @abstractmethod
    def apply_chaining(self, source_df_name: str, target_df_name: str, query: str) -> Tuple[pl.DataFrame, pl.DataFrame]:
        pass

    @abstractmethod
    def get_transformations(self, df_name: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_chaining_suggestions(self, source_df_name: str, target_df_name: str) -> List[str]:
        pass

    @abstractmethod
    async def find_similar_dataframes(self, df_name: str, top_k: int = 5) -> List[Tuple[str, float]]:
        pass

    @abstractmethod
    async def execute_cypher(self, query: str, params: Dict[str, Any] = {}) -> pl.DataFrame:
        pass