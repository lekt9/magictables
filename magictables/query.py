import sqlite3
from typing import List, Dict, Any, Union
import pandas as pd
from .database import MAGIC_DB, get_connection, reconstruct_nested_data


def execute_sql(query: str, params: tuple = ()) -> List[Dict[str, Any]]:
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


class SQLQueryBuilder:
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


def query_table(
    table_name: str,
    conditions: Dict[str, Any] = None,
    limit: int = None,
    order_by: List[str] = None,
    output_format: str = "dataframe",
) -> Union[pd.DataFrame, List[Dict[str, Any]], str]:
    """
    Query stored data from a specific table with optional conditions, limit, and ordering.

    :param table_name: Name of the table to query
    :param conditions: Dictionary of column-value pairs for WHERE clause
    :param limit: Maximum number of rows to return
    :param order_by: List of columns to order by
    :param output_format: Desired output format ('dataframe', 'dict', or 'json')
    :return: Query results in the specified format
    """
    query = f"SELECT * FROM {table_name}"
    params = []

    if conditions:
        where_clauses = []
        for column, value in conditions.items():
            where_clauses.append(f"{column} = ?")
            params.append(value)
        query += " WHERE " + " AND ".join(where_clauses)

    if order_by:
        query += f" ORDER BY {', '.join(order_by)}"

    if limit:
        query += f" LIMIT {limit}"

    with get_connection() as (conn, cursor):
        cursor.execute(query, params)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

    df = pd.DataFrame(rows, columns=columns)
    df = reconstruct_nested_data(cursor, table_name, df)

    if output_format == "dataframe":
        return df
    elif output_format == "dict":
        return df.to_dict("records")
    elif output_format == "json":
        return df.to_json(orient="records")
    else:
        raise ValueError(
            "Invalid output format. Choose 'dataframe', 'dict', or 'json'."
        )
