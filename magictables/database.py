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


def create_table(cursor: sqlite3.Cursor, table_name: str):
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS [{table_name}] (
            id TEXT PRIMARY KEY,
            related_tables TEXT
        )
    """
    )


def update_table_schema(cursor, table_name: str, columns: List[str]):
    existing_columns = get_existing_columns(cursor, table_name)
    new_columns = set(columns) - set(existing_columns)

    for column in new_columns:
        cursor.execute(f"ALTER TABLE [{table_name}] ADD COLUMN [{column}] TEXT")


def get_existing_columns(cursor, table_name: str) -> List[str]:
    cursor.execute(f"PRAGMA table_info([{table_name}])")
    return [row[1] for row in cursor.fetchall()]
