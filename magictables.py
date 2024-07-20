import dataset
import polars as pl
from typing import List, Dict, Any, Optional
import json
from datetime import datetime

MAGIC_DB = "sqlite:///magic.db"


class MagicTable:
    def __init__(self, table_name: str):
        self.db = dataset.connect(MAGIC_DB)
        self.table_name = table_name
        self.table = self.db[table_name]

    def create_table(self):
        if self.table_name not in self.db:
            self.table = self.db.create_table(
                self.table_name, primary_id="id", primary_type=self.db.types.string
            )
            self.table.create_column("timestamp", self.db.types.datetime)

    def get_columns(self) -> List[str]:
        return self.table.columns

    def add_column(self, column_name: str, column_type: str):
        if column_name not in self.table.columns:
            self.table.create_column(column_name, column_type)

    def insert(self, key: str, data: Dict[str, Any]):
        data["id"] = key
        data["timestamp"] = datetime.now()
        self.table.upsert(data, ["id"])

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        return self.table.find_one(id=key)

    def get_all(self) -> List[Dict[str, Any]]:
        return list(self.table.all())

    def to_dataframe(self) -> pl.DataFrame:
        return pl.DataFrame(self.get_all())

    def to_json(self) -> str:
        return json.dumps(self.get_all())

    def from_dataframe(self, df: pl.DataFrame):
        for row in df.iter_rows(named=True):
            self.insert(row["id"], row)

    def from_json(self, json_str: str):
        data = json.loads(json_str)
        for item in data:
            self.insert(item["id"], item)

    def query(self, **kwargs) -> List[Dict[str, Any]]:
        return list(self.table.find(**kwargs))

    def update(self, key: str, data: Dict[str, Any]):
        data["id"] = key
        self.table.update(data, ["id"])

    def delete(self, key: str):
        self.table.delete(id=key)

    def clear(self):
        self.table.drop()
        self.create_table()


if __name__ == "__main__":
    # Example usage
    table = MagicTable("example_table")
    table.create_table()
    table.add_column("name", table.db.types.string)
    table.add_column("age", table.db.types.integer)

    table.insert("1", {"name": "Alice", "age": 30})
    table.insert("2", {"name": "Bob", "age": 25})

    print(table.get("1"))
    print(table.get_all())
    print(table.to_dataframe())
    print(table.to_json())

    table.update("1", {"name": "Alice", "age": 31})
    print(table.get("1"))

    table.delete("2")
    print(table.get_all())

    table.clear()
