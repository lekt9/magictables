from contextlib import contextmanager
import json
import logging
from typing import Any, Dict, List, Optional, Union

import dataset
import pandas as pd

MAGIC_DB = "sqlite:///magic.db"


class MagicDB:
    def __init__(self, db_path: str = MAGIC_DB):
        self.db = dataset.connect(db_path)

    @contextmanager
    def get_connection(self):
        with self.db as tx:
            yield tx

    def check_cache_and_get_new_items(
        self, table_name: str, batch: List[Dict[str, Any]], keys: List[str]
    ):
        table = self.db[table_name]
        cached_results = {row["id"]: row for row in table.find(id={"in": keys})}
        new_items = [
            item for item, key in zip(batch, keys) if key not in cached_results
        ]
        new_keys = [key for key in keys if key not in cached_results]
        return cached_results, new_items, new_keys

    def get_cached_result(
        self, table_name: str, call_id: str
    ) -> Optional[pd.DataFrame]:
        table = self.db[table_name]
        row = table.find_one(call_id=call_id)
        if not row:
            return None
        df = pd.DataFrame([row])
        df = df.drop(columns=["call_id"], errors="ignore")
        return self.reconstruct_nested_data(table_name, df)

    def cache_results(
        self, table_name: str, keys: List[str], results: List[Dict[str, Any]]
    ) -> None:
        table = self.db[table_name]
        for key, result in zip(keys, results):
            result["id"] = key
            table.upsert(result, ["id"])

    def create_table_if_not_exists(self, table_name: str, columns: List[str]):
        if table_name not in self.db:
            self.db.create_table(
                table_name, primary_id="id", primary_type=self.db.types.string
            )
            table = self.db[table_name]
            for column in columns:
                if column not in table.columns:
                    table.create_column(column, self.db.types.string)

    def sanitize_sql_name(self, name: str) -> str:
        if name and not name[0].isalpha() and name[0] != "_":
            name = "_" + name
        return "".join(c if c.isalnum() or c == "_" else "_" for c in name)

    def create_tables_for_nested_data(
        self,
        table_name: str,
        data: Dict[str, Any],
        parent_table: Optional[str] = None,
        parent_column: Optional[str] = None,
    ) -> None:
        table = self.db.create_table(
            table_name, primary_id="id", primary_type=self.db.types.string
        )

        if parent_table and parent_column:
            table.create_column(f"{parent_table}_id", self.db.types.string)

        for key, value in data.items():
            sanitized_key = self.sanitize_sql_name(key)
            if isinstance(value, (str, int, float, bool)) or value is None:
                table.create_column(sanitized_key, self.db.types.string)
            elif isinstance(value, dict):
                nested_table_name = f"{table_name}_{sanitized_key}"
                self.create_tables_for_nested_data(
                    nested_table_name, value, table_name, sanitized_key
                )
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                junction_table_name = f"{table_name}_{sanitized_key}_junction"
                self.create_junction_table(
                    table_name, junction_table_name, sanitized_key
                )
                self.create_tables_for_nested_data(
                    f"{table_name}_{sanitized_key}",
                    value[0],
                    junction_table_name,
                    sanitized_key,
                )

    def create_junction_table(
        self, parent_table: str, junction_table: str, child_key: str
    ):
        table = self.db.create_table(
            junction_table, primary_id="id", primary_type=self.db.types.string
        )
        table.create_column(f"{parent_table}_id", self.db.types.string)
        table.create_column(f"{child_key}_id", self.db.types.string)

    def insert_nested_data(self, table_name: str, data: pd.DataFrame, call_id: str):
        table = self.db[table_name]
        data = data.copy()
        data["call_id"] = call_id

        for _, row in data.iterrows():
            row_dict = row.to_dict()
            for col, value in row_dict.items():
                if isinstance(value, (dict, list)):
                    nested_table_name = f"{table_name}_{col}"
                    if isinstance(value, dict):
                        nested_df = pd.DataFrame([value])
                    else:
                        nested_df = pd.DataFrame(value)
                    nested_df[f"{table_name}_id"] = row_dict["id"]
                    self.insert_nested_data(nested_table_name, nested_df, call_id)
                    row_dict[col] = json.dumps(value)
            table.upsert(row_dict, ["id"])

    def reconstruct_nested_data(
        self, table_name: str, df: pd.DataFrame
    ) -> pd.DataFrame:
        for col in df.columns:
            if col.endswith("_id"):
                continue
            sample_value = df[col].iloc[0] if len(df) > 0 else None
            if isinstance(sample_value, str):
                try:
                    parsed_value = json.loads(sample_value)
                    if isinstance(parsed_value, (dict, list)):
                        nested_table_name = f"{table_name}_{col}"
                        if nested_table_name in self.db.tables:
                            nested_data = []
                            for _, row in df.iterrows():
                                nested_rows = list(
                                    self.db[nested_table_name].find(
                                        **{f"{table_name}_id": row["id"]}
                                    )
                                )
                                nested_data.append(
                                    self.reconstruct_nested_data(
                                        nested_table_name, pd.DataFrame(nested_rows)
                                    )
                                )
                            df[col] = nested_data
                except json.JSONDecodeError:
                    pass
        return df


magic_db = MagicDB()
