from contextlib import contextmanager
import json
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

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


def check_cache_and_get_new_items(cursor, table_name, batch, keys):
    placeholders = ",".join(["?" for _ in keys])
    cursor.execute(
        f"SELECT id, response FROM [{table_name}] WHERE id IN ({placeholders})",
        keys,
    )
    cached_results = {row[0]: json.loads(row[1]) for row in cursor.fetchall()}
    new_items = [item for item, key in zip(batch, keys) if key not in cached_results]
    new_keys = [key for key in keys if key not in cached_results]
    return cached_results, new_items, new_keys


def cache_result(
    cursor: sqlite3.Cursor, table_name: str, call_id: str, result: pd.DataFrame
) -> None:
    """Cache the result in the database."""
    create_tables_for_nested_data(cursor, table_name, result)
    insert_nested_data(cursor, table_name, result, call_id)


def cache_results(
    cursor: sqlite3.Cursor,
    table_name: str,
    keys: List[str],
    results: List[Dict[str, Any]],
) -> None:
    for key, result in zip(keys, results):
        cursor.execute(
            f"INSERT INTO [{table_name}] (id, response) VALUES (?, ?)",
            (key, json.dumps(result)),
        )


def get_cached_result(
    cursor: sqlite3.Cursor, table_name: str, call_id: str
) -> Optional[pd.DataFrame]:
    try:
        cursor.execute(
            f"SELECT * FROM [{sanitize_sql_name(table_name)}] WHERE call_id = ?",
            (call_id,),
        )
        rows = cursor.fetchall()
        if rows:
            columns = [description[0] for description in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            return reconstruct_nested_data(cursor, table_name, df)
    except sqlite3.OperationalError as e:
        if "no such table" in str(e):
            return None
        else:
            raise
    return None


def create_table(cursor: sqlite3.Cursor, table_name: str, df: pd.DataFrame):
    # Get the column names and types from the DataFrame
    columns = df.dtypes.to_dict()

    # Determine the primary key
    if "id" in columns:
        primary_key = "id"
    else:
        # Use the first column as the primary key if 'id' doesn't exist
        primary_key = df.columns[0]

    # Create the table
    column_defs = [
        f"[{col}] {get_sqlite_type(dtype)}" for col, dtype in columns.items()
    ]
    column_defs.append("call_id TEXT")
    primary_key_def = f"PRIMARY KEY ([{primary_key}])"

    create_query = f"""
    CREATE TABLE IF NOT EXISTS [{table_name}] (
        {', '.join(column_defs)},
        {primary_key_def}
    )
    """
    cursor.execute(create_query)


def get_sqlite_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "REAL"
    else:
        return "TEXT"


def update_table_schema(
    cursor: sqlite3.Cursor, table_name: str, columns: List[Tuple[str, str]]
):
    existing_columns = set(
        row[1] for row in cursor.execute(f"PRAGMA table_info([{table_name}])")
    )
    for col_name, col_type in columns:
        if col_name not in existing_columns:
            cursor.execute(
                f"ALTER TABLE [{table_name}] ADD COLUMN [{col_name}] {col_type}"
            )


def get_existing_columns(cursor, table_name: str) -> List[str]:
    cursor.execute(f"PRAGMA table_info([{table_name}])")
    return [row[1] for row in cursor.fetchall()]


def reconstruct_nested_data(
    cursor: sqlite3.Cursor,
    table_name: str,
    df: pd.DataFrame,
    parent_keys: Optional[List[str]] = None,
) -> pd.DataFrame:
    if parent_keys is None:
        parent_keys = get_primary_key(cursor, table_name)

    if not parent_keys:
        print(
            f"Warning: No primary key or rowid found for table '{table_name}'. Skipping nested data reconstruction."
        )
        return df

    # Use the first primary key (or rowid) for joining
    parent_key = parent_keys[0]

    if parent_key not in df.columns:
        print(
            f"Warning: Column '{parent_key}' not found in DataFrame. Skipping nested data reconstruction."
        )
        return df

    # Get nested tables
    cursor.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '{table_name}_%'"
    )
    nested_tables = cursor.fetchall()

    for nested_table in nested_tables:
        nested_table_name = nested_table[0]
        nested_key = nested_table_name.split("_")[
            -1
        ]  # Assume the last part after '_' is the key name
        nested_parent_key = f"{table_name}_id"  # Use consistent naming

        try:
            cursor.execute(
                f"SELECT * FROM [{nested_table_name}] WHERE [{nested_parent_key}] IN ({','.join(['?']*len(df))})",
                df[parent_key].tolist(),
            )
            nested_rows = cursor.fetchall()
            if nested_rows:
                nested_columns = [description[0] for description in cursor.description]
                nested_df = pd.DataFrame(nested_rows, columns=nested_columns)
                reconstructed_nested_df = reconstruct_nested_data(
                    cursor, nested_table_name, nested_df, [nested_parent_key]
                )
                df[nested_key] = df[parent_key].map(
                    reconstructed_nested_df.groupby(nested_parent_key).apply(
                        lambda x: x.drop(
                            columns=[nested_parent_key, "call_id"]
                        ).to_dict("records")
                    )
                )
        except sqlite3.OperationalError as e:
            print(
                f"Warning: Error processing nested table '{nested_table_name}': {str(e)}"
            )

    # Remove internal columns
    df = df.drop(columns=["call_id"] + parent_keys, errors="ignore")
    return df


def get_primary_key(cursor: sqlite3.Cursor, table_name: str) -> Optional[List[str]]:
    try:
        cursor.execute(f"PRAGMA table_info([{table_name}])")
    except sqlite3.OperationalError as e:
        if "no such table" in str(e):
            return None
        else:
            raise

    primary_keys = []
    for row in cursor.fetchall():
        if row[5]:  # Any non-zero value in the 6th column indicates a primary key
            primary_keys.append(row[1])  # Append the name of the primary key column

    if not primary_keys:
        # If no primary key is found, check if there's a rowid
        try:
            cursor.execute(f"SELECT rowid FROM [{table_name}] LIMIT 1")
            if cursor.fetchone() is not None:
                return ["rowid"]
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                return None
            else:
                raise

    return primary_keys


def sanitize_sql_name(name: str) -> str:
    # Ensure the name starts with a letter or underscore
    if name and not name[0].isalpha() and name[0] != "_":
        name = "_" + name
    # Replace spaces with underscores and remove any characters that are not alphanumeric or underscore
    return "".join(c if c.isalnum() or c == "_" else "_" for c in name)


def create_tables_for_nested_data(
    cursor: sqlite3.Cursor, table_name: str, data: pd.DataFrame
):
    create_table(cursor, table_name, data)

    for col in data.columns:
        sample_value = data[col].iloc[0] if len(data) > 0 else None
        if isinstance(sample_value, (dict, list)):
            nested_table_name = f"{table_name}_{col}"
            if isinstance(sample_value, dict):
                nested_df = pd.DataFrame([sample_value])
            elif isinstance(sample_value, list):
                nested_df = pd.DataFrame(sample_value)
            create_tables_for_nested_data(cursor, nested_table_name, nested_df)


def insert_nested_data(
    cursor: sqlite3.Cursor, table_name: str, data: pd.DataFrame, call_id: str
):
    for _, row in data.iterrows():
        columns = list(row.index) + ["call_id"]
        values = list(row.values) + [call_id]
        placeholders = ", ".join(["?" for _ in columns])

        insert_query = f"INSERT OR REPLACE INTO [{table_name}] ({', '.join(columns)}) VALUES ({placeholders})"
        cursor.execute(insert_query, values)

    for col in data.columns:
        sample_value = data[col].iloc[0] if len(data) > 0 else None
        if isinstance(sample_value, (dict, list)):
            nested_table_name = f"{table_name}_{col}"
            if isinstance(sample_value, dict):
                nested_df = pd.DataFrame([row[col] for _, row in data.iterrows()])
            elif isinstance(sample_value, list):
                nested_df = pd.DataFrame([item for row in data[col] for item in row])
            insert_nested_data(cursor, nested_table_name, nested_df, call_id)
