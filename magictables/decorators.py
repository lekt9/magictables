import functools
import json
import hashlib
from typing import Any, Callable, Dict, List
from .database import get_connection, create_table, update_table_schema

import functools
import json
import hashlib
from typing import Any, Callable, Dict, List
from .database import get_connection, create_table, update_table_schema
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
import functools
import json
import hashlib
from typing import Any, Callable, Dict, List
from .database import get_connection, create_table, update_table_schema
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)

import functools
import json
import hashlib
from typing import Any, Callable, Dict, List
from .database import get_connection, create_table, update_table_schema


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


def create_tables_for_nested_data(
    cursor, base_table_name: str, data: Dict, parent_table: str = None
):
    tables = {}
    for key, value in data.items():
        if isinstance(value, dict):
            table_name = sanitize_sql_name(f"{base_table_name}_{key}")
            create_table(cursor, table_name)
            columns = [sanitize_sql_name(col) for col in value.keys()]
            columns.append(
                f"{sanitize_sql_name(base_table_name)}_id"
            )  # Always add parent table ID
            update_table_schema(cursor, table_name, columns)
            tables[key] = {
                "table_name": table_name,
                "nested_tables": create_tables_for_nested_data(
                    cursor, table_name, value, table_name
                ),
            }
        elif isinstance(value, list) and value and isinstance(value[0], dict):
            table_name = sanitize_sql_name(f"{base_table_name}_{key}")
            create_table(cursor, table_name)
            columns = [sanitize_sql_name(col) for col in value[0].keys()]
            columns.append(
                f"{sanitize_sql_name(base_table_name)}_id"
            )  # Always add parent table ID
            update_table_schema(cursor, table_name, columns)
            tables[key] = {
                "table_name": table_name,
                "is_list": True,
                "nested_tables": create_tables_for_nested_data(
                    cursor, table_name, value[0], table_name
                ),
            }
    return tables


def insert_nested_data(
    cursor,
    table_name: str,
    data: Dict,
    parent_id: str,
    table_structure: Dict,
):
    for key, value in data.items():
        if key in table_structure:
            nested_table_info = table_structure[key]
            nested_table_name = nested_table_info["table_name"]

            if "is_list" in nested_table_info and nested_table_info["is_list"]:
                for item in value:
                    insert_item(cursor, nested_table_name, item, table_name, parent_id)
                    nested_id = cursor.lastrowid
                    insert_nested_data(
                        cursor,
                        nested_table_name,
                        item,
                        nested_id,
                        nested_table_info["nested_tables"],
                    )
            else:
                insert_item(cursor, nested_table_name, value, table_name, parent_id)
                nested_id = cursor.lastrowid
                insert_nested_data(
                    cursor,
                    nested_table_name,
                    value,
                    nested_id,
                    nested_table_info["nested_tables"],
                )


def insert_item(
    cursor, table_name: str, item: Dict, parent_table_name: str, parent_id: str
):
    # Get existing columns
    cursor.execute(f"PRAGMA table_info([{table_name}])")
    existing_columns = set(row[1] for row in cursor.fetchall())

    # Determine new columns
    item_columns = set(sanitize_sql_name(col) for col in item.keys())
    new_columns = item_columns - existing_columns

    # Add new columns
    for col in new_columns:
        cursor.execute(f"ALTER TABLE [{table_name}] ADD COLUMN [{col}]")

    # Prepare column names and values
    columns = list(item_columns) + [f"{sanitize_sql_name(parent_table_name)}_id"]
    placeholders = ", ".join(["?" for _ in columns])
    values = [
        convert_to_supported_type(item.get(col, None)) for col in item_columns
    ] + [parent_id]

    # Enclose column names in square brackets
    column_names = ", ".join(f"[{col}]" for col in columns)

    # Insert the data
    cursor.execute(
        f"INSERT INTO [{table_name}] ({column_names}) VALUES ({placeholders})",
        values,
    )


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
    return "".join(c if c.isalnum() or c == "_" else "_" for c in name)


def mtable():
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            call_id = hashlib.md5(
                json.dumps((func.__name__, args, kwargs), sort_keys=True).encode()
            ).hexdigest()

            base_table_name = sanitize_sql_name(f"magic_{func.__name__}")

            with get_connection() as (conn, cursor):
                # Ensure the main table exists
                create_table(cursor, base_table_name)

                cursor.execute(
                    f"SELECT * FROM [{base_table_name}] WHERE id = ?", (call_id,)
                )
                row = cursor.fetchone()

                if row:
                    # Result found in cache
                    result = reconstruct_nested_data(cursor, base_table_name, call_id)
                else:
                    # Call the function
                    result = func(*args, **kwargs)

                    if result is None or (
                        isinstance(result, (dict, list)) and not result
                    ):
                        return result

                    # Create tables for nested data
                    table_structure = create_tables_for_nested_data(
                        cursor,
                        base_table_name,
                        result if isinstance(result, dict) else {"result": result},
                    )

                    # Flatten the main result for the base table
                    flattened_result = flatten_dict(
                        result if isinstance(result, dict) else {"result": result}
                    )

                    # Sanitize column names
                    sanitized_columns = [
                        sanitize_sql_name(col) for col in flattened_result.keys()
                    ]

                    # Update main table schema
                    update_table_schema(
                        cursor,
                        base_table_name,
                        sanitized_columns + ["table_structure"],
                    )

                    # Insert data into main table
                    columns = ["id"] + sanitized_columns + ["table_structure"]
                    placeholders = ", ".join(["?" for _ in columns])
                    values = (
                        [call_id]
                        + [
                            convert_to_supported_type(v)
                            for v in flattened_result.values()
                        ]
                        + [json.dumps(table_structure)]
                    )

                    cursor.execute(
                        f"INSERT OR REPLACE INTO [{base_table_name}] ({', '.join(columns)}) VALUES ({placeholders})",
                        values,
                    )

                    # Insert nested data
                    insert_nested_data(
                        cursor,
                        base_table_name,
                        result if isinstance(result, dict) else {"result": result},
                        call_id,
                        table_structure,
                    )

                    conn.commit()

                return result

        return wrapper

    return decorator


def reconstruct_nested_data(cursor, base_table_name: str, call_id: str) -> Dict:
    result = {}

    # Fetch the main table data
    cursor.execute(f"SELECT * FROM [{base_table_name}] WHERE id = ?", (call_id,))
    row = cursor.fetchone()
    if not row:
        return result

    # Reconstruct the main table data
    table_structure = None
    for idx, col in enumerate(cursor.description):
        col_name = col[0]
        if col_name == "table_structure":
            table_structure = json.loads(row[idx])
        elif col_name != "id":
            result[col_name] = row[idx]

    if not table_structure:
        return result

    # Reconstruct nested data
    for key, table_info in table_structure.items():
        nested_table_name = table_info["table_name"]
        is_list = table_info.get("is_list", False)

        cursor.execute(
            f"SELECT * FROM [{nested_table_name}] WHERE {base_table_name}_id = ?",
            (call_id,),
        )
        nested_rows = cursor.fetchall()

        if is_list:
            result[key] = []
            for nested_row in nested_rows:
                nested_item = {}
                for idx, col in enumerate(cursor.description):
                    col_name = col[0]
                    if col_name != f"{base_table_name}_id":
                        nested_item[col_name] = nested_row[idx]
                result[key].append(nested_item)
        elif nested_rows:
            result[key] = {}
            for idx, col in enumerate(cursor.description):
                col_name = col[0]
                if col_name != f"{base_table_name}_id":
                    result[key][col_name] = nested_rows[0][idx]

        # Recursively reconstruct deeper nested data
        if table_info["nested_tables"]:
            for nested_row in result[key] if is_list else [result[key]]:
                nested_id = nested_row["id"]
                nested_data = reconstruct_nested_data(
                    cursor, nested_table_name, nested_id
                )
                nested_row.update(nested_data)

    return result


def mchat(
    api_key: str,
    base_url: str = "https://openrouter.ai/api/v1/chat/completions",
    model: str = "mistralai/mistral-7b-instruct",
    batch_size: int = 10,
):
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if len(args) > 0 and isinstance(args[0], List):
                data = args[0]
            elif "data" in kwargs and isinstance(kwargs["data"], List):
                data = kwargs["data"]
            else:
                raise ValueError(
                    "The first argument or 'data' keyword argument must be a List"
                )

            table_name = f"ai_{func.__name__}"

            with get_connection() as (conn, cursor):
                create_table(cursor, table_name)

                # Process the data in batches
                results = []
                for i in range(0, len(data), batch_size):
                    batch = data[i : i + batch_size]
                    batch_dict = [
                        item if isinstance(item, dict) else {"input": item}
                        for item in batch
                    ]

                    # Generate keys for each item in the batch
                    keys = [
                        hashlib.md5(
                            json.dumps(item, sort_keys=True).encode()
                        ).hexdigest()
                        for item in batch_dict
                    ]

                    # Check which items are already in the cache
                    placeholders = ",".join(["?" for _ in keys])
                    cursor.execute(
                        f"SELECT id, response FROM [{table_name}] WHERE id IN ({placeholders})",
                        keys,
                    )
                    cached_results = {
                        row[0]: json.loads(row[1]) for row in cursor.fetchall()
                    }

                    # Process only uncached items
                    uncached_items = [
                        item
                        for item, key in zip(batch_dict, keys)
                        if key not in cached_results
                    ]

                    if uncached_items:
                        # Get AI-generated data for uncached items
                        ai_responses = parse_ai_response_batch(
                            func.__name__, uncached_items, api_key, base_url, model
                        )

                        # Update table schema and insert new data
                        for item, response in zip(uncached_items, ai_responses):
                            key = hashlib.md5(
                                json.dumps(item, sort_keys=True).encode()
                            ).hexdigest()
                            update_table_schema(cursor, table_name, response.keys())
                            columns = list(response.keys())
                            placeholders = ", ".join(["?" for _ in columns])
                            values = [response[col] for col in columns]
                            cursor.execute(
                                f"INSERT OR REPLACE INTO [{table_name}] (id, response) VALUES (?, ?)",
                                (key, json.dumps(response)),
                            )
                            cached_results[key] = response

                    conn.commit()

                    # Combine cached and new results
                    batch_results = [cached_results[key] for key in keys]
                    results.extend(batch_results)

                # Update generated types
                update_generated_types(conn)

                # Get the generated type hint
                type_hint = get_type_hint(func.__name__)

                # Convert results to the generated type if available
                if type_hint is not None:
                    results = [type_hint(**item) for item in results]

                # Call the original function with the results
                return func(results, *args[1:], **kwargs)

        return wrapper

    return decorator
