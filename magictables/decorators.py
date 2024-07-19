import functools
import json
import hashlib
import logging
import sqlite3
import pandas as pd
import requests
from .database import get_connection, create_table, update_table_schema
from .schema_generator import get_type_hint, update_generated_types
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
    cast,
    Union,
    Literal,
    Hashable,
)

T = TypeVar("T")

# Configure logging
logging.basicConfig(level=logging.CRITICAL)


class ChainableMagicTable:
    def __init__(self, data: Union[pd.DataFrame, List[Dict[str, Any]]]):
        if isinstance(data, pd.DataFrame):
            self.df = data
        else:
            self.df = pd.DataFrame(data)

    def join(
        self,
        other: "ChainableMagicTable",
        how: str = "inner",
        on: Union[str, List[str]] = None,
    ) -> "ChainableMagicTable":
        self.df = self.df.merge(other.df, how=how, on=on)
        return self

    from pandas._typing import Axis

    def concat(
        self, other: "ChainableMagicTable", axis: Axis = 0
    ) -> "ChainableMagicTable":
        self.df = pd.concat([self.df, other.df], axis=axis)
        return self

    def apply(
        self, func: Callable[[pd.DataFrame], pd.DataFrame]
    ) -> "ChainableMagicTable":
        self.df = func(self.df)
        return self

    def to_dataframe(self) -> pd.DataFrame:
        return self.df

    def to_dict(
        self,
        orient: Literal[
            "dict", "list", "series", "split", "records", "index"
        ] = "records",
    ) -> Union[Dict[Hashable, Any], List[Dict[Hashable, Any]]]:
        return self.df.to_dict(orient)


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


def call_ai_model(
    new_items: List[Dict[str, Any]], api_key: str, base_url: str, model: str
) -> List[Dict[str, Any]]:
    headers = {
        "Authorization": f"Bearer {api_key}",
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
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            "response_format": {"type": "json_object"},
        }

        # Call OpenAI/OpenRouter API
        response = requests.post(url=base_url, headers=headers, data=json.dumps(data))

        if response.status_code != 200:
            raise Exception(
                f"OpenRouter API request failed with status code {response.status_code}: {response.text}"
            )

        response_json = response.json()
        response_data = re.search(
            r"```(?:json)?\s*([\s\S]*?)\s*```",
            response_json["choices"][0]["message"]["content"],
        )
        if response_data:
            response_data = json.loads(response_data.group(1).strip())
        else:
            raise ValueError("No JSON content found in the response")

        results.append(response_data)

    return results


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


def insert_nested_data(
    cursor: sqlite3.Cursor,
    table_name: str,
    data: Dict[str, Any],
    call_id: str,
    table_structure: Optional[
        Tuple[List[Tuple[str, str]], List[Tuple[str, Dict[str, Any]]]]
    ] = None,
) -> str:
    columns, nested_tables = table_structure if table_structure else ([], [])

    cursor.execute(f"PRAGMA table_info([{sanitize_sql_name(table_name)}])")
    existing_columns = [
        row[1] for row in cursor.fetchall() if row[1] not in ["id", "call_id"]
    ]

    # Check if entry already exists
    cursor.execute(
        f"SELECT * FROM [{sanitize_sql_name(table_name)}] WHERE call_id = ?", (call_id,)
    )
    existing_entry = cursor.fetchone()

    if existing_entry:
        # Update existing entry
        set_clause = ", ".join([f"[{col}] = ?" for col in existing_columns])
        update_query = f"UPDATE [{sanitize_sql_name(table_name)}] SET {set_clause} WHERE call_id = ?"
        values = [data.get(col, None) for col in existing_columns] + [call_id]
        cursor.execute(update_query, values)
    else:
        # Insert new entry
        columns = ["call_id"] + existing_columns
        placeholders = ", ".join(["?" for _ in columns])
        insert_query = f"INSERT INTO [{sanitize_sql_name(table_name)}] ({', '.join(columns)}) VALUES ({placeholders})"
        values = [call_id] + [data.get(col, None) for col in existing_columns]
        cursor.execute(insert_query, values)

    reference_id = call_id

    for nested_key, _ in nested_tables:
        if nested_key in data:
            nested_table_name = (
                f"{sanitize_sql_name(table_name)}_{sanitize_sql_name(nested_key)}"
            )
            for item in data[nested_key]:
                insert_nested_data(
                    cursor, nested_table_name, item, f"{call_id}_{nested_key}"
                )

    return reference_id


def reconstruct_nested_data(cursor, base_table_name: str, call_id: str) -> Dict:
    result = {}

    cursor.execute(
        f"SELECT * FROM [{sanitize_sql_name(base_table_name)}] WHERE call_id = ?",
        (call_id,),
    )
    row = cursor.fetchone()
    if not row:
        return result

    # Reconstruct the main table data
    result = json.loads(row[3])  # Assuming 'data' is the 4th column

    # Fetch and reconstruct nested data
    cursor.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '{sanitize_sql_name(base_table_name)}_%'"
    )
    nested_tables = [row[0] for row in cursor.fetchall()]

    for nested_table in nested_tables:
        key = nested_table[
            len(sanitize_sql_name(base_table_name)) + 1 :
        ]  # Remove base_table_name_ prefix
        cursor.execute(
            f"SELECT * FROM [{nested_table}] WHERE call_id LIKE ?",
            (f"{call_id}_%",),
        )
        nested_rows = cursor.fetchall()

        if nested_rows:
            result[key] = [
                json.loads(row[3]) for row in nested_rows
            ]  # Assuming 'data' is the 4th column

    return result


def convert_to_supported_type(value):
    if isinstance(value, (int, float, str, bytes, type(None))):
        return value
    elif isinstance(value, bool):
        return int(value)
    elif isinstance(value, (list, dict)):
        return json.dumps(value)
    else:
        return str(value)


def sanitize_sql_name(name):
    # Ensure the name starts with a letter or underscore
    if name and not name[0].isalpha() and name[0] != "_":
        name = "_" + name
    # Replace spaces with underscores and remove any characters that are not alphanumeric or underscore
    return "".join(
        c if c.isalnum() or c == "_" else "_" for c in name.replace(" ", "_")
    )


def mtable() -> Callable[[Callable[..., T]], Callable[..., ChainableMagicTable]]:
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> ChainableMagicTable:
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
                        if isinstance(result, pd.DataFrame):
                            return ChainableMagicTable(result)
                        elif isinstance(result, list) and all(
                            isinstance(item, dict) for item in result
                        ):
                            return ChainableMagicTable(result)
                        elif isinstance(result, dict):
                            return ChainableMagicTable([result])
                        else:
                            raise ValueError(
                                f"Unsupported return type from {func.__name__}: {type(result)}"
                            )
                    else:
                        raise ValueError(f"Function {func.__name__} returned None")

            return ChainableMagicTable(result)

        return wrapper

    return decorator


def mai(
    api_key: str,
    base_url: str = "https://openrouter.ai/api/v1/chat/completions",
    model: str = "mistralai/mistral-7b-instruct",
    batch_size: int = 10,
    mode: str = "generate",  # Can be "generate" or "augment"
) -> Callable[[Callable[..., Any]], Callable[..., ChainableMagicTable]]:
    def decorator(func: Callable[..., Any]) -> Callable[..., ChainableMagicTable]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> ChainableMagicTable:
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
                        new_results = call_ai_model(new_items, api_key, base_url, model)
                        cache_results(cursor, table_name, new_keys, new_results)
                        cached_results.update(dict(zip(new_keys, new_results)))

                    ai_results.extend([cached_results[key] for key in keys])

                conn.commit()
                update_generated_types(conn)

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

            # Add type assertion here
            if isinstance(final_result, pd.DataFrame):
                return ChainableMagicTable(final_result)
            else:
                return ChainableMagicTable(cast(List[Dict[str, Any]], final_result))

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
