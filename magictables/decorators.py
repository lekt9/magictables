import functools
import json
import hashlib
import logging
import uuid
from typing import Any, Callable, Dict, List
from .database import get_connection, create_table, update_table_schema
from .schema_generator import update_generated_types, get_type_hint

# Configure logging
logging.basicConfig(level=logging.DEBUG)


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
    columns = ["id", "reference_id"]
    if parent_table:
        columns.extend([f"{parent_table}_reference_id", "local_id"])

    for key, value in data.items():
        if isinstance(value, dict) or (
            isinstance(value, list) and value and isinstance(value[0], dict)
        ):
            nested_table_name = sanitize_sql_name(f"{base_table_name}_{key}")
            create_table(cursor, nested_table_name)
            nested_columns = ["id", f"{base_table_name}_reference_id", "local_id"]
            if isinstance(value, dict):
                nested_data = value
            else:  # list of dicts
                nested_data = value[0]
            nested_columns.extend(
                [
                    sanitize_sql_name(col)
                    for col in nested_data.keys()
                    if not isinstance(nested_data[col], (dict, list))
                ]
            )
            update_table_schema(cursor, nested_table_name, nested_columns)
            tables[key] = {
                "table_name": nested_table_name,
                "is_list": isinstance(value, list),
                "nested_tables": create_tables_for_nested_data(
                    cursor, nested_table_name, nested_data, base_table_name
                ),
            }
        else:
            columns.append(sanitize_sql_name(key))

    update_table_schema(cursor, base_table_name, columns)
    return tables


def insert_nested_data(
    cursor,
    table_name: str,
    data: Dict,
    reference_id: str,
    table_structure: Dict,
    parent_reference_id: str = None,
):
    # Get existing columns
    cursor.execute(f"PRAGMA table_info([{table_name}])")
    existing_columns = set(row[1] for row in cursor.fetchall())

    # Prepare columns and values for insertion
    columns = ["reference_id"]
    values = [reference_id]
    if parent_reference_id:
        columns.extend([f"{table_name.rsplit('_', 1)[0]}_reference_id", "local_id"])
        values.extend([parent_reference_id, str(uuid.uuid4())])

    for key, value in data.items():
        if key not in table_structure:
            sanitized_key = sanitize_sql_name(key)
            if sanitized_key not in existing_columns:
                # Add new column if it doesn't exist
                cursor.execute(
                    f"ALTER TABLE [{table_name}] ADD COLUMN [{sanitized_key}]"
                )
                existing_columns.add(sanitized_key)
            columns.append(sanitized_key)
            values.append(value)

    # Insert non-nested data
    if columns:
        placeholders = ", ".join(["?" for _ in columns])
        column_names = ", ".join(f"[{col}]" for col in columns)
        cursor.execute(
            f"INSERT INTO [{table_name}] ({column_names}) VALUES ({placeholders})",
            values,
        )

    # Insert nested data
    for key, value in data.items():
        if key in table_structure:
            nested_table_info = table_structure[key]
            nested_table_name = nested_table_info["table_name"]

            if nested_table_info["is_list"]:
                for item in value:
                    insert_nested_data(
                        cursor,
                        nested_table_name,
                        item,
                        str(uuid.uuid4()),
                        nested_table_info["nested_tables"],
                        reference_id,
                    )
            else:
                insert_nested_data(
                    cursor,
                    nested_table_name,
                    value,
                    str(uuid.uuid4()),
                    nested_table_info["nested_tables"],
                    reference_id,
                )


def insert_item(
    cursor,
    table_name: str,
    item: Dict,
    parent_table_name: str,
    parent_reference_id: str,
) -> str:
    # Get existing columns
    cursor.execute(f"PRAGMA table_info([{table_name}])")
    existing_columns = set(row[1] for row in cursor.fetchall())

    # Prepare columns and values for insertion
    columns = ["reference_id"]
    values = [parent_reference_id]
    for key, value in item.items():
        if not isinstance(value, (dict, list)):
            sanitized_key = sanitize_sql_name(key)
            if sanitized_key not in existing_columns:
                # Add new column if it doesn't exist
                cursor.execute(
                    f"ALTER TABLE [{table_name}] ADD COLUMN [{sanitized_key}]"
                )
                existing_columns.add(sanitized_key)
            columns.append(sanitized_key)
            values.append(value)

    # Insert the data
    placeholders = ", ".join(["?" for _ in columns])
    column_names = ", ".join(f"[{col}]" for col in columns)
    cursor.execute(
        f"INSERT INTO [{table_name}] ({column_names}) VALUES ({placeholders})",
        values,
    )

    return cursor.lastrowid


def reconstruct_nested_data(
    cursor, base_table_name: str, reference_id: str, parent_reference_id: str = None
) -> Dict:
    result = {}

    # Fetch the main table data
    if parent_reference_id:
        cursor.execute(
            f"SELECT * FROM [{base_table_name}] WHERE {base_table_name.rsplit('_', 1)[0]}_reference_id = ? AND reference_id = ?",
            (parent_reference_id, reference_id),
        )
    else:
        cursor.execute(
            f"SELECT * FROM [{base_table_name}] WHERE reference_id = ?", (reference_id,)
        )
    row = cursor.fetchone()
    if not row:
        return result

    # Reconstruct the main table data
    for idx, col in enumerate(cursor.description):
        col_name = col[0]
        if col_name not in [
            "id",
            "reference_id",
            f"{base_table_name.rsplit('_', 1)[0]}_reference_id",
            "local_id",
        ]:
            result[col_name] = row[idx]

    # Fetch and reconstruct nested data
    cursor.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '{base_table_name}_%'"
    )
    nested_tables = [row[0] for row in cursor.fetchall()]

    for nested_table in nested_tables:
        key = nested_table[len(base_table_name) + 1 :]  # Remove base_table_name_ prefix
        cursor.execute(
            f"SELECT * FROM [{nested_table}] WHERE {base_table_name}_reference_id = ?",
            (reference_id,),
        )
        nested_rows = cursor.fetchall()

        if nested_rows:
            if len(nested_rows) > 1:  # It's a list
                result[key] = []
                for nested_row in nested_rows:
                    nested_item = {}
                    for idx, col in enumerate(cursor.description):
                        col_name = col[0]
                        if col_name not in [
                            "id",
                            "reference_id",
                            f"{base_table_name}_reference_id",
                            "local_id",
                        ]:
                            nested_item[col_name] = nested_row[idx]
                    result[key].append(nested_item)
            else:  # It's a single item
                result[key] = {}
                for idx, col in enumerate(cursor.description):
                    col_name = col[0]
                    if col_name not in [
                        "id",
                        "reference_id",
                        f"{base_table_name}_reference_id",
                        "local_id",
                    ]:
                        result[key][col_name] = nested_rows[0][idx]

            # Recursively reconstruct deeper nested data
            for nested_row in (
                result[key] if isinstance(result[key], list) else [result[key]]
            ):
                nested_reference_id = nested_row.get("reference_id")
                if nested_reference_id:
                    nested_data = reconstruct_nested_data(
                        cursor, nested_table, nested_reference_id, reference_id
                    )
                    nested_row.update(nested_data)

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
                    f"SELECT * FROM [{base_table_name}] WHERE reference_id = ?",
                    (call_id,),
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

                    # Insert nested data
                    insert_nested_data(
                        cursor,
                        base_table_name,
                        result if isinstance(result, dict) else {"result": result},
                        call_id,
                        table_structure,
                    )

                    conn.commit()

                    # Update generated types
                    update_generated_types(conn)

                # Get the generated type hint
                type_hint = get_type_hint(func.__name__)

                # Convert results to the generated type if available
                if type_hint is not None:
                    result = type_hint(**result)

                return result

        return wrapper

    return decorator


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
