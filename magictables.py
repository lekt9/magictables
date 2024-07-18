import re
import requests
import functools
from typing import Callable
import pandas as pd
import sqlite3
from hashlib import md5
import json

MAGIC_DB = "magic.db"


def mtable():
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Use the full function name as the table name
            table_name = f"{func.__module__}.{func.__name__}"

            # Create a unique key for the function call
            key = md5(f"{func.__name__}:{str(args)}:{str(kwargs)}".encode()).hexdigest()

            # Connect to the database
            conn = sqlite3.connect("magic.db")
            cursor = conn.cursor()

            # Check if the table exists, if not create it
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS [{table_name}] (
                    id TEXT PRIMARY KEY,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Check if the result is already in the cache
            cursor.execute(f"SELECT * FROM [{table_name}] WHERE id = ?", (key,))
            cached_result = cursor.fetchone()

            if cached_result:
                # If cached result exists, return it as a DataFrame
                columns = [description[0] for description in cursor.description]
                return pd.DataFrame([cached_result], columns=columns)
            else:
                # If not in cache, call the original function
                result = func(*args, **kwargs)

                # Convert the result to a DataFrame
                df = pd.json_normalize(result)

                # Update the table schema based on DataFrame columns
                existing_columns = set(
                    col[1]
                    for col in cursor.execute(
                        f"PRAGMA table_info([{table_name}])"
                    ).fetchall()
                )
                for column in df.columns:
                    if column not in existing_columns and column not in [
                        "id",
                        "timestamp",
                    ]:
                        cursor.execute(
                            f"ALTER TABLE [{table_name}] ADD COLUMN [{column}] TEXT"
                        )

                # Store the result in the cache
                columns = ", ".join(
                    [f"[{col}]" for col in df.columns if col not in ["id", "timestamp"]]
                )
                placeholders = ", ".join(
                    ["?" for _ in df.columns if _ not in ["id", "timestamp"]]
                )
                cursor.execute(
                    f"""
                    INSERT OR REPLACE INTO [{table_name}] (id, {columns})
                    VALUES (?, {placeholders})
                """,
                    (key, *df.iloc[0].drop(["id", "timestamp"], errors="ignore")),
                )

                conn.commit()
                conn.close()

                return df

        return wrapper

    return decorator


def mchat(
    api_key: str,
    base_url: str = "https://openrouter.ai/api/v1/chat/completions",
    model: str = "mistralai/mistral-7b-instruct",
):
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Use the full function name as the table name
            table_name = f"{func.__module__}.{func.__name__}"

            # Create a unique key for the function call
            key = md5(f"{func.__name__}:{str(args)}:{str(kwargs)}".encode()).hexdigest()

            # Connect to the database
            conn = sqlite3.connect(MAGIC_DB)
            cursor = conn.cursor()

            # Check if the table exists, if not create it
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS [{table_name}] (
                    id TEXT PRIMARY KEY,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Check if a response is already in the cache
            cursor.execute(f"SELECT * FROM [{table_name}] WHERE id = ?", (key,))
            cached_result = cursor.fetchone()

            if cached_result:
                # If cached result exists, use it
                columns = [description[0] for description in cursor.description]
                response_data = dict(zip(columns, cached_result))
                response_data.pop("id", None)
                response_data.pop("timestamp", None)
            else:
                # If not in cache, call the AI to generate new columns
                headers = {
                    "Authorization": f"Bearer {api_key}",
                }

                prompt = (
                    f"You are the all knowing JSON generator. Given the function arguments, "
                    f"Create a JSON object that populates the missing, or incomplete columns for the function call."
                    f"The function is: {func.__name__}\n"
                    f"The current keys are: {json.dumps(kwargs)}\n, you MUST only use these keys in the JSON you respond."
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

                # Call OpenRouter API
                response = requests.post(
                    url=base_url, headers=headers, data=json.dumps(data)
                )

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

                # Update the table schema based on response_data
                existing_columns = set(
                    col[1]
                    for col in cursor.execute(
                        f"PRAGMA table_info([{table_name}])"
                    ).fetchall()
                )
                for column in response_data.keys():
                    if column not in existing_columns and column not in [
                        "id",
                        "timestamp",
                    ]:
                        cursor.execute(
                            f"ALTER TABLE [{table_name}] ADD COLUMN [{column}] TEXT"
                        )

                # Prepare the data for insertion
                columns = list(response_data.keys())
                placeholders = ", ".join(["?" for _ in columns])
                values = [v for v in response_data.values()]

                # Store the response in the cache
                cursor.execute(
                    f"""
                    INSERT OR REPLACE INTO [{table_name}] (id, {', '.join(columns)})
                    VALUES (?, {placeholders})
                    """,
                    (key, *values),
                )

                conn.commit()

            # Merge AI-generated data with original kwargs
            merged_kwargs = {**kwargs, **response_data}

            # Call the original function with merged kwargs
            result = func(*args, **merged_kwargs)

            conn.close()

            # Return the result from the original function
            return result

        return wrapper

    return decorator
