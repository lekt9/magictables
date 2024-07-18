import sqlite3
from contextlib import contextmanager

MAGIC_DB = "magic.db"


@contextmanager
def get_connection():
    conn = sqlite3.connect(MAGIC_DB)
    cursor = conn.cursor()
    try:
        yield conn, cursor
    finally:
        conn.close()


def create_table(cursor, table_name):
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS [{table_name}] (
            id TEXT PRIMARY KEY,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """
    )


def update_table_schema(cursor, table_name, columns):
    existing_columns = set(
        col[1]
        for col in cursor.execute(f"PRAGMA table_info([{table_name}])").fetchall()
    )
    for column in columns:
        if column not in existing_columns and column not in ["id", "timestamp"]:
            cursor.execute(f"ALTER TABLE [{table_name}] ADD COLUMN [{column}] TEXT")
