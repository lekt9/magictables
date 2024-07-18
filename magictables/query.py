import sqlite3
from typing import List, Dict, Any
from .database import MAGIC_DB, get_connection


def query_magic_db(query: str, params: tuple = ()) -> List[Dict[str, Any]]:
    """
    Execute a custom SQL query on the magic database.

    :param query: SQL query string
    :param params: Query parameters (optional)
    :return: List of dictionaries representing the query results
    """
    with get_connection() as (conn, cursor):
        cursor.execute(query, params)
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_table_info() -> Dict[str, List[str]]:
    """
    Get information about all tables in the magic database.

    :return: Dictionary with table names as keys and lists of column names as values
    """
    with get_connection() as (conn, cursor):
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        table_info = {}
        for (table_name,) in tables:
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]
            table_info[table_name] = columns

    return table_info


def join_magic_tables(
    table1: str, table2: str, join_column: str, select_columns: List[str]
) -> List[Dict[str, Any]]:
    """
    Perform a simple inner join between two tables in the magic database.

    :param table1: Name of the first table
    :param table2: Name of the second table
    :param join_column: Column to join on (must exist in both tables)
    :param select_columns: List of columns to select in the result
    :return: List of dictionaries representing the join results
    """
    query = f"""
    SELECT {', '.join(select_columns)}
    FROM {table1}
    INNER JOIN {table2} ON {table1}.{join_column} = {table2}.{join_column}
    """
    return query_magic_db(query)


class QueryBuilder:
    def __init__(self):
        self.select_clause = []
        self.from_clause = ""
        self.where_clause = []
        self.join_clause = []
        self.order_by_clause = []
        self.limit_clause = ""

    def select(self, *columns):
        self.select_clause.extend(columns)
        return self

    def from_table(self, table_name):
        self.from_clause = table_name
        return self

    def where(self, condition):
        self.where_clause.append(condition)
        return self

    def join(self, table, on_condition):
        self.join_clause.append(f"INNER JOIN {table} ON {on_condition}")
        return self

    def order_by(self, *columns):
        self.order_by_clause.extend(columns)
        return self

    def limit(self, limit_value):
        self.limit_clause = f"LIMIT {limit_value}"
        return self

    def build(self):
        query = f"SELECT {', '.join(self.select_clause)} FROM {self.from_clause}"

        if self.join_clause:
            query += " " + " ".join(self.join_clause)

        if self.where_clause:
            query += " WHERE " + " AND ".join(self.where_clause)

        if self.order_by_clause:
            query += f" ORDER BY {', '.join(self.order_by_clause)}"

        if self.limit_clause:
            query += f" {self.limit_clause}"

        return query


def execute_query(query_builder: QueryBuilder) -> List[Dict[str, Any]]:
    """
    Execute a query built with the QueryBuilder.

    :param query_builder: QueryBuilder instance
    :return: List of dictionaries representing the query results
    """
    query = query_builder.build()
    return query_magic_db(query)
