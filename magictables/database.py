from contextlib import contextmanager
import sqlite3
from typing import Any, List, Optional, Tuple

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


def get_sqlite_type(value: Any) -> str:
    if isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "REAL"
    elif isinstance(value, bool):
        return "INTEGER"  # SQLite doesn't have a boolean type, so we use INTEGER
    elif isinstance(value, str):
        return "TEXT"
    else:
        return "TEXT"  # Default to TEXT for complex types


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
    cursor: sqlite3.Cursor,
    table_name: str,
    data: pd.DataFrame,
    parent_table: Optional[str] = None,
    parent_key: Optional[str] = None,
) -> None:
    columns = []
    nested_tables = []
    primary_key = None

    for col in data.columns:
        sample_value = data[col].iloc[0] if len(data) > 0 else None
        if isinstance(sample_value, (str, int, float, bool)) or sample_value is None:
            if primary_key is None:
                primary_key = col
            columns.append((sanitize_sql_name(col), get_sqlite_type(sample_value)))
        elif isinstance(sample_value, (dict, list)):
            nested_tables.append((col, sample_value))

    print(
        f"Creating table: {table_name}, primary_key: {primary_key}, parent_table: {parent_table}, parent_key: {parent_key}"
    )
    create_table(
        cursor, sanitize_sql_name(table_name), parent_table, parent_key, primary_key
    )
    update_table_schema(cursor, sanitize_sql_name(table_name), columns)

    # Ensure all columns from the DataFrame are in the table
    existing_columns = get_existing_columns(cursor, sanitize_sql_name(table_name))
    for col in data.columns:
        if sanitize_sql_name(col) not in existing_columns:
            sample_value = data[col].iloc[0] if len(data) > 0 else None
            col_type = get_sqlite_type(sample_value)
            cursor.execute(
                f"ALTER TABLE [{sanitize_sql_name(table_name)}] ADD COLUMN [{sanitize_sql_name(col)}] {col_type}"
            )

    for nested_col, nested_data in nested_tables:
        nested_table_name = (
            f"{sanitize_sql_name(table_name)}_{sanitize_sql_name(nested_col)}"
        )
        if isinstance(nested_data, dict):
            nested_df = pd.DataFrame([nested_data])
        elif isinstance(nested_data, list):
            nested_df = pd.DataFrame(nested_data)
        else:
            continue
        # Ensure the nested table is created before getting its primary key
        create_table(cursor, nested_table_name)
        nested_primary_key = get_primary_key(cursor, nested_table_name)
        create_tables_for_nested_data(
            cursor,
            nested_table_name,
            nested_df,
            sanitize_sql_name(table_name),
            nested_primary_key[0] if nested_primary_key else f"{table_name}_id",
        )


def create_table(
    cursor: sqlite3.Cursor,
    table_name: str,
    parent_table: Optional[str] = None,
    parent_key: Optional[str] = None,
    primary_key: Optional[str] = None,
):
    if primary_key:
        primary_key_def = f"[{primary_key}] INTEGER PRIMARY KEY"
    else:
        primary_key_def = "id INTEGER PRIMARY KEY AUTOINCREMENT"

    columns = [primary_key_def, "call_id TEXT"]

    if parent_table and parent_key:
        columns.append(f"[{parent_key}] INTEGER")
        columns.append(
            f"FOREIGN KEY ([{parent_key}]) REFERENCES [{parent_table}]([{parent_key}])"
        )

    columns_def = ", ".join(columns)

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS [{table_name}] (
            {columns_def},
            UNIQUE(id)
        )
        """
    )


def insert_nested_data(
    cursor: sqlite3.Cursor,
    table_name: str,
    data: pd.DataFrame,
    call_id: str,
    parent_key: Optional[str] = None,
    parent_value: Optional[Any] = None,
) -> None:
    for _, row in data.iterrows():
        columns = ["call_id"] + ([parent_key] if parent_value is not None else [])
        values = [call_id] + ([parent_value] if parent_value is not None else [])

        for col, value in row.items():
            if isinstance(value, (str, int, float, bool)) or value is None:
                columns.append(sanitize_sql_name(col))
                values.append(value)

        print("columns", columns)
        print("values", values)

        placeholders = ", ".join(["?" for _ in columns])
        insert_query = f"INSERT OR REPLACE INTO [{sanitize_sql_name(table_name)}] ({', '.join(columns)}) VALUES ({placeholders})"
        try:
            cursor.execute(insert_query, values)
        except sqlite3.IntegrityError as e:
            print(f"Warning: {e}. Attempting to update existing row.")
            update_columns = [f"{col} = ?" for col in columns if col != "id"]
            update_values = [
                value for col, value in zip(columns, values) if col != "id"
            ]
            update_query = f"UPDATE [{sanitize_sql_name(table_name)}] SET {', '.join(update_columns)} WHERE id = ?"
            cursor.execute(update_query, update_values + [row["id"]])

        # Get the rowid of the inserted or updated row
        cursor.execute("SELECT last_insert_rowid()")
        rowid = cursor.fetchone()[0]

        for col, value in row.items():
            if isinstance(value, (dict, list)):
                nested_table_name = (
                    f"{sanitize_sql_name(table_name)}_{sanitize_sql_name(col)}"
                )
                if isinstance(value, dict):
                    nested_df = pd.DataFrame([value])
                elif isinstance(value, list):
                    nested_df = pd.DataFrame(value)
                else:
                    continue
                # Get the primary key of the nested table
                nested_primary_key = get_primary_key(cursor, nested_table_name)
                insert_nested_data(
                    cursor,
                    nested_table_name,
                    nested_df,
                    call_id,
                    (
                        nested_primary_key[0]
                        if nested_primary_key
                        else f"{table_name}_id"
                    ),  # Use table name as prefix for consistency
                    rowid,
                )
