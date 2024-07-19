import numpy as np
import functools
import json
import hashlib
import logging
import re
import sqlite3
import pandas as pd
import requests
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Union
from pandas.api.extensions import register_dataframe_accessor
import os
from dotenv import load_dotenv

from .database import get_connection, create_table, update_table_schema
from .schema_generator import get_table_schema, get_type_hint, update_generated_types
from .utils import (
    OPENAI_API_KEY,
    OPENAI_BASE_URL,
    OPENAI_MODEL,
    generate_ai_descriptions,
)

T = TypeVar("T")

# Configure logging
logging.basicConfig(level=logging.CRITICAL)


@register_dataframe_accessor("magic")
class MagicTableAccessor:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj
        self.func_name = None
        self.type_hint = None

    def set_func_name(self, func_name):
        self.func_name = func_name
        self.type_hint = get_type_hint(func_name) if func_name else None
        return self._obj

    def to_typed_dict(self):
        if self.type_hint:
            return [self.type_hint(**row) for _, row in self._obj.iterrows()]
        else:
            return self._obj.to_dict(orient="records")

    def get_column_types(self):
        with get_connection() as (conn, cursor):
            table_name = sanitize_sql_name(f"magic_{self.func_name}")
            schema = get_table_schema(cursor, table_name)
        return {
            col: get_pandas_type(
                type_info[0] if isinstance(type_info, (tuple, list)) else type_info
            )
            for col, type_info in schema.items()
            if col in self._obj.columns
        }


def create_magic_table(data, func_name=None):
    if isinstance(data, pd.DataFrame):
        df = data
    elif isinstance(data, list) and all(isinstance(item, dict) for item in data):
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        df = pd.DataFrame([data])
    else:
        raise ValueError(f"Unsupported data type: {type(data)}")

    # Get the schema information
    with get_connection() as (conn, cursor):
        table_name = sanitize_sql_name(f"magic_{func_name}")
        schema = get_table_schema(cursor, table_name)

    # Set the column types based on the schema
    for column, type_info in schema.items():
        if column in df.columns:
            df[column] = df[column].astype(get_pandas_type(type_info[0]))

    # Generate AI descriptions and update DataFrame metadata
    ai_descriptions = generate_ai_descriptions(table_name, df.columns.tolist())
    df.attrs["table_description"] = ai_descriptions["table_description"]
    df.attrs["column_descriptions"] = ai_descriptions["column_descriptions"]

    return df.magic.set_func_name(func_name)


def flatten_dict(d: Dict, parent_key: str = "", sep: str = "_") -> Dict:
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            items.append((new_key, json.dumps(v)))  # Convert list to JSON string
        else:
            items.append((new_key, v))
    return dict(items)


def get_pandas_type(sqlite_type):
    type_mapping = {
        "INTEGER": np.int64,
        "REAL": np.float64,
        "TEXT": object,
        "BLOB": object,
        "NULL": object,
    }
    # If sqlite_type is a tuple or list, use the first element
    if isinstance(sqlite_type, (tuple, list)):
        sqlite_type = sqlite_type[0]
    return type_mapping.get(sqlite_type, object)


def get_sqlite_type(value):
    if isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "REAL"
    elif isinstance(value, bool):
        return "INTEGER"  # SQLite doesn't have a boolean type, so we use INTEGER
    elif isinstance(value, str):
        return "TEXT"
    elif value is None:
        return "NULL"
    elif isinstance(value, (list, dict)):
        return "TEXT"  # We'll store complex types as JSON strings
    else:
        return "TEXT"  # Default to TEXT for unknown types


def create_tables_for_nested_data(
    cursor: sqlite3.Cursor,
    table_name: str,
    data: Dict[str, Any],
    parent_table: Optional[str] = None,
) -> Tuple[List[Tuple[str, str]], List[Tuple[str, Dict[str, Any]]]]:
    columns = []
    nested_tables = []

    for key, value in data.items():
        sanitized_key = sanitize_sql_name(key)
        if isinstance(value, (str, int, float, bool)) or value is None:
            columns.append((sanitized_key, get_sqlite_type(value)))
        elif isinstance(value, list) and value and isinstance(value[0], dict):
            nested_tables.append((sanitized_key, value[0]))

    create_table(cursor, sanitize_sql_name(table_name), parent_table)
    update_table_schema(cursor, sanitize_sql_name(table_name), columns)

    for nested_key, nested_data in nested_tables:
        nested_table_name = f"{sanitize_sql_name(table_name)}_{nested_key}"
        create_tables_for_nested_data(
            cursor, nested_table_name, nested_data, sanitize_sql_name(table_name)
        )

    return columns, nested_tables


def cache_results(
    cursor: sqlite3.Cursor,
    table_name: str,
    keys: List[str],
    results: List[Dict[str, Any]],
):
    for key, result in zip(keys, results):
        # Get existing columns
        cursor.execute(f"PRAGMA table_info([{table_name}])")
        existing_columns = set(row[1] for row in cursor.fetchall())

        # Add new columns if necessary
        for column in result.keys():
            if column not in existing_columns and column not in ["id", "timestamp"]:
                cursor.execute(f"ALTER TABLE [{table_name}] ADD COLUMN [{column}] TEXT")
        # Prepare the data for insertion
        columns = list(result.keys())
        placeholders = ", ".join(["?" for _ in columns])
        values = [
            json.dumps(v) if isinstance(v, (dict, list)) else v for v in result.values()
        ]

        # Store the response in the cache
        cursor.execute(
            f"""
            INSERT OR REPLACE INTO [{table_name}] (id, {', '.join(columns)})
            VALUES (?, {placeholders})
            """,
            (key, *values),
        )


def sanitize_sql_name(name):
    # Ensure the name starts with a letter or underscore
    if name and not name[0].isalpha() and name[0] != "_":
        name = "_" + name
    # Replace spaces with underscores and remove any characters that are not alphanumeric or underscore
    return "".join(
        c if c.isalnum() or c == "_" else "_" for c in name.replace(" ", "_")
    )


def mtable() -> Callable[[Callable[..., T]], Callable[..., pd.DataFrame]]:
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> pd.DataFrame:
            call_id = hashlib.md5(
                json.dumps((func.__name__, args, kwargs), sort_keys=True).encode()
            ).hexdigest()

            base_table_name = sanitize_sql_name(f"magic_{func.__name__}")

            with get_connection() as (conn, cursor):
                create_table(cursor, base_table_name)

                cursor.execute(
                    f"SELECT * FROM [{base_table_name}] WHERE call_id = ?",
                    (call_id,),
                )
                row = cursor.fetchone()

                if row:
                    # Result found in cache
                    print(f"Cache hit for {func.__name__}")  # Debug print
                    result = json.loads(row[3])  # Assuming 'data' is the 4th column
                else:
                    # Cache miss, call the function
                    print(f"Cache miss for {func.__name__}")  # Debug print
                    result = func(*args, **kwargs)

                    if result is not None:
                        df = create_magic_table(result, func.__name__)
                        # Update the schema based on the new data
                        update_table_schema(
                            cursor,
                            base_table_name,
                            [
                                (col, get_sqlite_type(df[col].iloc[0]))
                                for col in df.columns
                            ],
                        )
                        conn.commit()

                        # Generate AI descriptions and update types
                        ai_descriptions = generate_ai_descriptions(
                            base_table_name, df.columns.tolist()
                        )

                        return df
                    else:
                        raise ValueError(f"Function {func.__name__} returned None")

            return create_magic_table(result, func.__name__)

        return wrapper

    return decorator


def mai(
    batch_size: int = 10,
    mode: str = "generate",  # Can be "generate" or "augment"
) -> Callable[[Callable[..., Any]], Callable[..., pd.DataFrame]]:
    def decorator(func: Callable[..., Any]) -> Callable[..., pd.DataFrame]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> pd.DataFrame:
            # Call the original function
            result = func(*args, **kwargs)

            # Determine the input type and convert to a list of dicts if necessary
            if isinstance(result, pd.DataFrame):
                data = result.to_dict("records")
                is_dataframe = True
            elif isinstance(result, list):
                data = result
                is_dataframe = False
                result = pd.DataFrame(result)  # Convert list to DataFrame
            else:
                raise ValueError(
                    "The decorated function must return either a pandas DataFrame or a List"
                )

            table_name = f"ai_{func.__name__}"

            with get_connection() as (conn, cursor):
                create_table(cursor, table_name)

                ai_results = []
                for i in range(0, len(data), batch_size):
                    batch = data[i : i + batch_size]

                    # Generate keys for each item in the batch
                    keys = [
                        hashlib.md5(
                            json.dumps(item, sort_keys=True).encode()
                        ).hexdigest()
                        for item in batch
                    ]

                    # Check cache and process uncached items
                    cached_results, new_items, new_keys = check_cache_and_get_new_items(
                        cursor, table_name, batch, keys
                    )

                    if new_items:
                        # Call AI model for new items
                        new_results = call_ai_model(new_items)
                        cache_results(cursor, table_name, new_keys, new_results)
                        cached_results.update(dict(zip(new_keys, new_results)))

                    ai_results.extend([cached_results[key] for key in keys])

                conn.commit()

                # Generate AI descriptions and update types
                ai_descriptions = generate_ai_descriptions(
                    table_name, result.columns.tolist()
                )

            # Process the results based on the mode and input type
            if mode == "generate":
                final_result = ai_results
            elif mode == "augment":
                if is_dataframe:
                    for idx, ai_result in enumerate(ai_results):
                        for key, value in ai_result.items():
                            result.loc[idx, f"ai_{key}"] = value
                    final_result = result
                else:
                    final_result = [
                        {**original, **ai_result}
                        for original, ai_result in zip(data, ai_results)
                    ]
            else:
                raise ValueError("Invalid mode. Must be either 'generate' or 'augment'")

            return create_magic_table(final_result, func.__name__)

        return wrapper

    return decorator


# Helper functions for mai decorator
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


def call_ai_model(new_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
    }

    results = []
    for item in new_items:
        prompt = (
            f"You are the all knowing JSON generator. Given the function arguments, "
            f"Create a JSON object that populates the missing, or incomplete columns for the function call."
            f"The current keys are: {json.dumps(item)}\n, you MUST only use these keys in the JSON you respond."
            f"Respond with it wrapped in ```json code block with a flat unnested JSON"
        )

        data = {
            "model": OPENAI_MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            "response_format": {"type": "json_object"},
        }

        # Call OpenAI/OpenRouter API
        response = requests.post(
            url=OPENAI_BASE_URL, headers=headers, data=json.dumps(data)
        )

        if response.status_code != 200:
            raise Exception(
                f"OpenAI API request failed with status code {response.status_code}: {response.text}"
            )

        response_json = response.json()
        response_content = response_json["choices"][0]["message"]["content"]

        # Extract JSON from the response
        json_start = response_content.find("```json") + 7
        json_end = response_content.rfind("```")
        json_str = response_content[json_start:json_end].strip()

        result = json.loads(json_str)
        results.append(result)

    return results  # Move this line outside of the for loop
