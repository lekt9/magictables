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
            table_name = f"{base_table_name}_{key}"
            create_table(cursor, table_name)
            columns = list(value.keys())
            columns.append(f"{base_table_name}_id")  # Always add parent table ID
            update_table_schema(cursor, table_name, columns)
            tables[key] = {
                "table_name": table_name,
                "nested_tables": create_tables_for_nested_data(
                    cursor, table_name, value, table_name
                ),
            }
        elif isinstance(value, list) and value and isinstance(value[0], dict):
            table_name = f"{base_table_name}_{key}"
            create_table(cursor, table_name)
            columns = list(value[0].keys())
            columns.append(f"{base_table_name}_id")  # Always add parent table ID
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
    parent_id: str = None,
    table_structure: Dict = None,
):
    if not table_structure:
        return

    for key, value in data.items():
        if key in table_structure:
            nested_table_info = table_structure[key]
            nested_table_name = nested_table_info["table_name"]

            if "is_list" in nested_table_info and nested_table_info["is_list"]:
                for item in value:
                    columns = list(item.keys())
                    columns.append(f"{table_name}_id")  # Always add parent table ID
                    placeholders = ", ".join(["?" for _ in columns])
                    values = [
                        convert_to_supported_type(item[col]) for col in item.keys()
                    ]
                    values.append(parent_id)

                    cursor.execute(
                        f"INSERT INTO [{nested_table_name}] ({', '.join(columns)}) VALUES ({placeholders})",
                        values,
                    )

                    nested_id = cursor.lastrowid
                    insert_nested_data(
                        cursor,
                        nested_table_name,
                        item,
                        nested_id,
                        nested_table_info["nested_tables"],
                    )
            else:
                columns = list(value.keys())
                columns.append(f"{table_name}_id")  # Always add parent table ID
                placeholders = ", ".join(["?" for _ in columns])
                values = [convert_to_supported_type(value[col]) for col in value.keys()]
                values.append(parent_id)

                cursor.execute(
                    f"INSERT INTO [{nested_table_name}] ({', '.join(columns)}) VALUES ({placeholders})",
                    values,
                )

                nested_id = cursor.lastrowid
                insert_nested_data(
                    cursor,
                    nested_table_name,
                    value,
                    nested_id,
                    nested_table_info["nested_tables"],
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


def reconstruct_nested_data(
    cursor, base_table_name: str, call_id: str, related_tables: Dict
) -> Dict:
    result = {}
    for key, table_info in related_tables.items():
        table_name = table_info["table_name"]
        cursor.execute(
            f"SELECT * FROM [{table_name}] WHERE {base_table_name}_id = ?", (call_id,)
        )
        rows = cursor.fetchall()
        if table_info.get("is_list", False):
            result[key] = [
                dict(zip([c[0] for c in cursor.description], row)) for row in rows
            ]
            for item in result[key]:
                reconstruct_nested_data(
                    cursor, table_name, item["id"], table_info.get("nested_tables", {})
                )
        else:
            if rows:
                result[key] = dict(zip([c[0] for c in cursor.description], rows[0]))
                reconstruct_nested_data(
                    cursor,
                    table_name,
                    result[key]["id"],
                    table_info.get("nested_tables", {}),
                )
    return result


def mtable():
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            call_id = hashlib.md5(
                json.dumps((func.__name__, args, kwargs), sort_keys=True).encode()
            ).hexdigest()

            base_table_name = f"magic_{func.__name__}"

            with get_connection() as (conn, cursor):
                # Ensure the main table exists
                create_table(cursor, base_table_name)

                cursor.execute(
                    f"SELECT * FROM [{base_table_name}] WHERE id = ?", (call_id,)
                )
                row = cursor.fetchone()

                if row:
                    # Result found in cache
                    result = {}
                    for idx, col in enumerate(cursor.description):
                        col_name = col[0]
                        if col_name != "id" and col_name != "related_tables":
                            result[col_name] = row[idx]

                    # Fetch related data from other tables
                    related_tables = json.loads(row[-1])
                    nested_data = reconstruct_nested_data(
                        cursor, base_table_name, call_id, related_tables
                    )
                    result.update(nested_data)
                else:
                    # Call the function
                    result = func(*args, **kwargs)

                    if result is None or (
                        isinstance(result, (dict, list)) and not result
                    ):
                        return result

                    # Create tables for nested data
                    table_structure = create_tables_for_nested_data(
                        cursor, base_table_name, result
                    )

                    # Flatten the main result for the base table
                    flattened_result = flatten_dict(result)

                    # Update main table schema
                    update_table_schema(
                        cursor,
                        base_table_name,
                        list(flattened_result.keys()) + ["related_tables"],
                    )

                    # Insert data into main table
                    columns = (
                        ["id"] + list(flattened_result.keys()) + ["related_tables"]
                    )
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
                        cursor, base_table_name, result, call_id, table_structure
                    )

                    conn.commit()

                return result

        return wrapper

    return decorator


def reconstruct_nested_data(
    cursor, base_table_name: str, call_id: str, related_tables: Dict
) -> Dict:
    result = {}
    for key, table_info in related_tables.items():
        table_name = table_info["table_name"]
        cursor.execute(
            f"SELECT * FROM [{table_name}] WHERE {base_table_name}_id = ?", (call_id,)
        )
        rows = cursor.fetchall()
        if table_info.get("is_list", False):
            result[key] = []
            for row in rows:
                item = dict(zip([c[0] for c in cursor.description], row))
                nested_data = reconstruct_nested_data(
                    cursor, table_name, item["id"], table_info.get("nested_tables", {})
                )
                item.update(nested_data)
                result[key].append(item)
        else:
            if rows:
                result[key] = dict(zip([c[0] for c in cursor.description], rows[0]))
                nested_data = reconstruct_nested_data(
                    cursor,
                    table_name,
                    result[key]["id"],
                    table_info.get("nested_tables", {}),
                )
                result[key].update(nested_data)
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
