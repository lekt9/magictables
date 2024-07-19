import functools
import json
import hashlib
from typing import Any, Callable, TypeVar, Union, List, Dict, Tuple, Optional, cast
import pandas as pd
import sqlite3

import requests

from magictables.database import (
    create_table,
    create_tables_for_nested_data,
    get_cached_result,
    get_connection,
    insert_nested_data,
    sanitize_sql_name,
)
from magictables.schema_generator import get_type_hint, update_generated_types
from magictables.utils import (
    OPENAI_API_KEY,
    OPENAI_BASE_URL,
    OPENAI_MODEL,
    generate_ai_descriptions,
)

T = TypeVar("T")


def mtable() -> Callable[[Callable[..., Any]], Callable[..., pd.DataFrame]]:
    def decorator(func: Callable[..., Any]) -> Callable[..., pd.DataFrame]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> pd.DataFrame:
            call_id = hashlib.md5(
                json.dumps((func.__name__, args, kwargs), sort_keys=True).encode()
            ).hexdigest()

            base_table_name = sanitize_sql_name(f"magic_{func.__name__}")

            with get_connection() as (conn, cursor):
                # Check if result exists in cache
                result = get_cached_result(cursor, base_table_name, call_id)
                if result is not None:
                    print(f"Cache hit for {func.__name__}")
                    return pd.DataFrame(result)

                # Cache miss, call the function
                print(f"Cache miss for {func.__name__}")
                result = func(*args, **kwargs)

                if result is None:
                    return pd.DataFrame()  # Return an empty DataFrame if result is None

                # Convert result to DataFrame if it's not already
                if not isinstance(result, pd.DataFrame):
                    if isinstance(result, dict):
                        result = pd.DataFrame([result])
                    elif isinstance(result, list) and all(
                        isinstance(item, dict) for item in result
                    ):
                        result = pd.DataFrame(result)
                    else:
                        result = pd.DataFrame({"result": [result]})

                # Create tables and insert data
                create_tables_for_nested_data(cursor, base_table_name, result)
                insert_nested_data(cursor, base_table_name, result, call_id)

                conn.commit()

                # Update generated types
                update_generated_types(conn)

            return result

        return wrapper

    return decorator


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


def reconstruct_result(result: Any) -> pd.DataFrame:
    if isinstance(result, list) and all(isinstance(item, dict) for item in result):
        return pd.DataFrame(result)
    elif isinstance(result, dict):
        return pd.DataFrame([result])
    else:
        return pd.DataFrame({"result": [result]})


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


def mai(
    batch_size: int = 10,
    mode: str = "generate",  # Can be "generate" or "augment"
) -> Callable[[Callable[..., Any]], Callable[..., pd.DataFrame]]:
    def decorator(func: Callable[..., Any]) -> Callable[..., pd.DataFrame]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> pd.DataFrame:
            # Call the original function
            result = func(*args, **kwargs)

            # Determine the input type and convert to a DataFrame if necessary
            if isinstance(result, pd.DataFrame):
                data = result.to_dict("records")
                is_dataframe = True
            elif isinstance(result, list) and all(
                isinstance(item, dict) for item in result
            ):
                data = result
                is_dataframe = False
                result = pd.DataFrame(result)  # Convert list of dicts to DataFrame
            else:
                raise ValueError(
                    "The decorated function must return either a pandas DataFrame or a List of dictionaries"
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
                generate_ai_descriptions(table_name, result.columns.tolist())

            # Process the results based on the mode and input type
            if mode == "generate":
                final_result = pd.DataFrame(ai_results)
            elif mode == "augment":
                if is_dataframe:
                    for idx, ai_result in enumerate(ai_results):
                        for key, value in ai_result.items():
                            result.loc[idx, f"ai_{key}"] = value
                    final_result = result
                else:
                    final_result = pd.DataFrame(
                        [
                            {**original, **ai_result}
                            for original, ai_result in zip(data, ai_results)
                        ]
                    )
            else:
                raise ValueError("Invalid mode. Must be either 'generate' or 'augment'")

            return final_result

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
