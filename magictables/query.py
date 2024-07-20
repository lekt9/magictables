from typing import List, Dict, Any, Optional
from magictables.database import magic_db


class SQLQueryBuilder:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.conditions = []
        self.order_by = None
        self.limit = None

    def where(self, condition: str) -> "SQLQueryBuilder":
        self.conditions.append(condition)
        return self

    def order(self, column: str, ascending: bool = True) -> "SQLQueryBuilder":
        self.order_by = f"{column} {'ASC' if ascending else 'DESC'}"
        return self

    def limit(self, n: int) -> "SQLQueryBuilder":
        self.limit = n
        return self

    def build(self) -> str:
        query = f"SELECT * FROM {self.table_name}"
        if self.conditions:
            query += " WHERE " + " AND ".join(self.conditions)
        if self.order_by:
            query += f" ORDER BY {self.order_by}"
        if self.limit:
            query += f" LIMIT {self.limit}"
        return query

    def execute(self) -> List[Dict[str, Any]]:
        query = self.build()
        with magic_db.get_connection() as conn:
            result = conn.query(query)
            return list(result)


def query(table_name: str) -> SQLQueryBuilder:
    return SQLQueryBuilder(table_name)
