from typing import Dict, Any, Optional
from datetime import datetime


class MagicTableChain:
    def __init__(
        self,
        source_table: str,
        api_result_table: str,
        merged_result_table: str,
        chain_type: str,
        source_key: str,
        target_key: str,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.source_table = source_table
        self.api_result_table = api_result_table
        self.merged_result_table = merged_result_table
        self.chain_type = chain_type
        self.source_key = source_key
        self.target_key = target_key
        self.metadata = metadata or {}
        self.created_at = datetime.now()

    def __repr__(self):
        return (
            f"MagicTableChain(source={self.source_table}, "
            f"api_result={self.api_result_table}, "
            f"merged_result={self.merged_result_table}, "
            f"type={self.chain_type}, "
            f"source_key={self.source_key}, "
            f"target_key={self.target_key})"
        )

    def to_dict(self):
        return {
            "source_table": self.source_table,
            "api_result_table": self.api_result_table,
            "merged_result_table": self.merged_result_table,
            "chain_type": self.chain_type,
            "source_key": self.source_key,
            "target_key": self.target_key,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        chain = cls(
            data["source_table"],
            data["api_result_table"],
            data["merged_result_table"],
            data["chain_type"],
            data["source_key"],
            data["target_key"],
            data["metadata"],
        )
        chain.created_at = datetime.fromisoformat(data["created_at"])
        return chain
