from contextlib import contextmanager
import json
import logging
import sqlite3
from typing import Any, Dict, Hashable, List, Optional, Tuple, Union

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
    # Get all columns for the table
    cursor.execute(f"PRAGMA table_info([{table_name}])")
    columns = [row[1] for row in cursor.fetchall() if row[1] != "id"]

    placeholders = ",".join(["?" for _ in keys])
    cursor.execute(
        f"SELECT id, {', '.join(columns)} FROM [{table_name}] WHERE id IN ({placeholders})",
        keys,
    )
    cached_results = {row[0]: dict(zip(columns, row[1:])) for row in cursor.fetchall()}
    new_items = [item for item, key in zip(batch, keys) if key not in cached_results]
    new_keys = [key for key in keys if key not in cached_results]
    return cached_results, new_items, new_keys


def cache_result(
    cursor: sqlite3.Cursor, table_name: str, call_id: str, result: pd.DataFrame
) -> None:
    """Cache the result in the database."""
    create_tables_for_nested_data(cursor, table_name, result)
    insert_nested_data(cursor, table_name, result, call_id)


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


import pandas as pd
import numpy as np


def ensure_dataframe(result: Any) -> pd.DataFrame:
    """Ensure the result is a DataFrame."""
    if isinstance(result, pd.DataFrame):
        return result
    elif isinstance(result, dict):
        return pd.DataFrame([result])
    elif isinstance(result, list):
        if all(isinstance(item, dict) for item in result):
            return pd.DataFrame(result)
        else:
            try:
                return pd.DataFrame(result)
            except ValueError:
                raise ValueError(
                    "List items are not consistent for DataFrame conversion."
                )
    elif isinstance(result, str):
        try:
            json_result = json.loads(result)
            return ensure_dataframe(json_result)
        except json.JSONDecodeError:
            raise ValueError("String input is not valid JSON.")
    else:
        return pd.DataFrame({"result": [result]})


def infer_sqlite_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "REAL"
    elif pd.api.types.is_bool_dtype(dtype):
        return "INTEGER"  # SQLite doesn't have a boolean type
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TEXT"  # Store datetimes as ISO8601 strings
    elif pd.api.types.is_object_dtype(dtype):
        # For object dtypes, we need to check the actual content
        return "TEXT"  # Default to TEXT for object dtypes
    else:
        return "TEXT"  # Default to TEXT for any other types


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
            f"Warning: No primary key found for table '{table_name}'. Skipping nested data reconstruction."
        )
        return df

    # Use the first primary key for joining
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
    unique_columns = []
    for row in cursor.fetchall():
        if row[5]:  # Any non-zero value in the 6th column indicates a primary key
            primary_keys.append(row[1])  # Append the name of the primary key column
        if row[3]:  # Any non-zero value in the 4th column indicates a unique column
            unique_columns.append(row[1])  # Append the name of the unique column

    if not primary_keys and unique_columns:
        primary_keys = unique_columns

    return primary_keys if primary_keys else None


def cache_results(
    cursor: sqlite3.Cursor,
    table_name: str,
    keys: List[str],
    results: List[Dict[str, Any]],
) -> None:
    for key, result in zip(keys, results):
        # Get the column names from the result
        columns = list(result.keys())

        # Create the table if it doesn't exist
        create_table_if_not_exists(cursor, table_name, columns)

        # Prepare the INSERT statement
        placeholders = ", ".join(
            ["?" for _ in range(len(columns) + 1)]
        )  # +1 for the id
        column_names = ", ".join(["id"] + columns)

        # Prepare the values
        values = [str(key)] + [
            (
                json.dumps(result[col])
                if isinstance(result[col], (dict, list))
                else str(result[col]) if col in ["likes", "comments"] else result[col]
            )
            for col in columns
        ]

        # Debug: Print the schema of the table
        cursor.execute(f"PRAGMA table_info([{table_name}])")

        # Execute the INSERT statement
        cursor.execute(
            f"INSERT OR REPLACE INTO [{table_name}] ({column_names}) VALUES ({placeholders})",
            values,
        )


def create_table_if_not_exists(
    cursor: sqlite3.Cursor, table_name: str, columns: List[str]
):
    # Get existing columns
    cursor.execute(f"PRAGMA table_info([{table_name}])")
    existing_columns = set(row[1] for row in cursor.fetchall())

    # Create table if it doesn't exist
    if not existing_columns:
        column_defs = ", ".join([f"[{col}] TEXT" for col in columns])
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS [{table_name}] (id TEXT PRIMARY KEY, {column_defs})"
        )
    else:
        # Add any new columns
        for col in columns:
            if col not in existing_columns:
                cursor.execute(f"ALTER TABLE [{table_name}] ADD COLUMN [{col}] TEXT")


def sanitize_sql_name(name: str) -> str:
    # Ensure the name starts with a letter or underscore
    if name and not name[0].isalpha() and name[0] != "_":
        name = "_" + name
    # Replace spaces with underscores and remove any characters that are not alphanumeric or underscore
    return "".join(c if c.isalnum() or c == "_" else "_" for c in name)


def create_tables_for_nested_data(
    cursor: sqlite3.Cursor, table_name: str, data: pd.DataFrame
):
    columns = [
        (str(col), infer_sqlite_type(dtype)) for col, dtype in data.dtypes.items()
    ]
    create_table(cursor, table_name, columns)

    for col in data.columns:
        if col in ["id", "call_id"]:
            continue
        sample_value = data[col].iloc[0] if len(data) > 0 else None
        if isinstance(sample_value, (dict, list)):
            nested_table_name = f"{table_name}_{col}"
            nested_df = pd.DataFrame()
            if isinstance(sample_value, dict):
                nested_df = pd.json_normalize(data[col].dropna())
            elif isinstance(sample_value, list):
                nested_df = pd.DataFrame(data[col].explode().dropna().tolist())

            if not nested_df.empty:
                # Add a column to link back to the parent table
                parent_id_col = f"{table_name}_id"
                nested_df[parent_id_col] = (
                    data.index if "id" not in data.columns else data["id"]
                )

                create_tables_for_nested_data(cursor, nested_table_name, nested_df)

                # Create a foreign key relationship
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS [{nested_table_name}] (
                        [{parent_id_col}] INTEGER,
                        FOREIGN KEY ([{parent_id_col}]) REFERENCES [{table_name}]([{primary_keys[0]}])
                    )
                """
                )


def create_table(
    cursor: sqlite3.Cursor,
    table_name: str,
    columns: List[Tuple[Union[str, Hashable], str]],
):
    # Determine the primary key
    primary_keys = get_primary_key(cursor, table_name)
    if primary_keys:
        primary_key = ", ".join(f"[{pk}]" for pk in primary_keys)
        id_definition = f"PRIMARY KEY ({primary_key})"
    else:
        primary_key = "id"
        id_definition = "id INTEGER PRIMARY KEY AUTOINCREMENT"

    column_defs = [
        f"[{col}] {col_type if col != 'id' else 'TEXT PRIMARY KEY'}"
        for col, col_type in columns
        if primary_keys is None or col not in primary_keys
    ]
    if "id" not in [col for col, _ in columns]:
        column_defs.append("id TEXT PRIMARY KEY")  # Change to TEXT
    column_defs.append("call_id TEXT")

    # Ensure the primary key is correctly placed
    if primary_keys:
        column_defs = [
            (
                f"[{col}] {col_type} PRIMARY KEY"
                if col in primary_keys
                else f"[{col}] {col_type}"
            )
            for col, col_type in columns
        ]
        column_defs.append("call_id TEXT")

    create_query = f"""
    CREATE TABLE IF NOT EXISTS [{table_name}] (
        {', '.join(column_defs)}
    )
    """

    cursor.execute(create_query)


def insert_nested_data(
    cursor: sqlite3.Cursor, table_name: str, data: pd.DataFrame, call_id: str
):
    # Insert data into the main table
    data = data.copy()  # Create a copy to avoid modifying the original DataFrame
    data["call_id"] = call_id

    # Check if the table exists
    cursor.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    )
    table_exists = cursor.fetchone() is not None

    if not table_exists:
        # Create the table if it doesn't exist
        columns = [
            (str(col), infer_sqlite_type(dtype)) for col, dtype in data.dtypes.items()
        ]
        create_table(cursor, table_name, columns)

    primary_keys = get_primary_key(cursor, table_name)
    if primary_keys:
        data.to_sql(table_name, cursor.connection, if_exists="append", index=False)
    else:
        data.to_sql(table_name, cursor.connection, if_exists="append", index=False)

    # Get the last inserted id
    cursor.execute("SELECT last_insert_rowid()")
    last_id = cursor.fetchone()[0]

    # Handle nested data
    for col in data.columns:
        if primary_keys and col in primary_keys or col == "call_id":
            continue
        sample_value = data[col].iloc[0] if len(data) > 0 else None
        if isinstance(sample_value, (dict, list)):
            nested_table_name = f"{table_name}_{col}"
            if isinstance(sample_value, dict):
                nested_df = pd.json_normalize(data[col].dropna())
            elif isinstance(sample_value, list):
                nested_df = pd.DataFrame(data[col].explode().dropna().tolist())

            if not nested_df.empty:
                parent_id_col = f"{table_name}_id"
                if primary_keys:
                    nested_df[parent_id_col] = data[primary_keys[0]].repeat(
                        nested_df.groupby(level=0).size()
                    )
                else:
                    nested_df[parent_id_col] = range(
                        last_id - len(data) + 1, last_id + 1
                    ).repeat(nested_df.groupby(level=0).size())
                nested_df["call_id"] = call_id
                insert_nested_data(cursor, nested_table_name, nested_df, call_id)
