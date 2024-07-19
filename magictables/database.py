import sqlite3
from contextlib import contextmanager
from typing import List, Optional

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


def create_table(
    cursor: sqlite3.Cursor, table_name: str, parent_table: Optional[str] = None
):
    cursor.execute(
        f"""
    CREATE TABLE IF NOT EXISTS [{table_name}] (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        call_id TEXT UNIQUE,
        reference_id TEXT,
        {f'{parent_table}_reference_id TEXT,' if parent_table else ''}
        data TEXT
    )
    """
    )


def update_table_schema(cursor, table_name: str, columns: List[tuple]):
    existing_columns = set(
        row[1] for row in cursor.execute(f"PRAGMA table_info([{table_name}])")
    )
    for column_info in columns:
        column = column_info[0]
        column_type = column_info[1]
        if column not in existing_columns and column != "reference_id":
            additional_constraints = (
                " ".join(column_info[2:]) if len(column_info) > 2 else ""
            )
            cursor.execute(
                f"ALTER TABLE [{table_name}] ADD COLUMN [{column}] {column_type} {additional_constraints}"
            )


def get_existing_columns(cursor, table_name: str) -> List[str]:
    cursor.execute(f"PRAGMA table_info([{table_name}])")
    return [row[1] for row in cursor.fetchall()]
