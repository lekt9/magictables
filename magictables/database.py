import sqlite3
from contextlib import contextmanager
from typing import List

MAGIC_DB = "magic.db"


@contextmanager
def get_connection():
    conn = sqlite3.connect(MAGIC_DB)
    cursor = conn.cursor()
    try:
        yield conn, cursor
    finally:
        cursor.close()
        conn.close()


def create_table(cursor, table_name: str):
    cursor.execute(
        f"""
    CREATE TABLE IF NOT EXISTS [{table_name}] (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        reference_id TEXT
    )
    """
    )


def update_table_schema(cursor, table_name: str, columns: List[str]):
    existing_columns = set(
        row[1] for row in cursor.execute(f"PRAGMA table_info([{table_name}])")
    )
    for column in columns:
        if column not in existing_columns and column != "reference_id":
            cursor.execute(f"ALTER TABLE [{table_name}] ADD COLUMN [{column}]")


def get_existing_columns(cursor, table_name: str) -> List[str]:
    cursor.execute(f"PRAGMA table_info([{table_name}])")
    return [row[1] for row in cursor.fetchall()]
