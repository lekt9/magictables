import functools
from typing import Callable
import pandas as pd
from .database import get_connection, create_table, update_table_schema
from .utils import create_key, parse_ai_response, parse_ai_response_batch


def mtable():
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            table_name = f"{func.__module__}.{func.__name__}"
            key = create_key(func.__name__, args, kwargs)

            with get_connection() as (conn, cursor):
                create_table(cursor, table_name)
                cursor.execute(f"SELECT * FROM [{table_name}] WHERE id = ?", (key,))
                cached_result = cursor.fetchone()

                if cached_result:
                    columns = [description[0] for description in cursor.description]
                    return pd.DataFrame([cached_result], columns=columns)
                else:
                    result = func(*args, **kwargs)
                    df = pd.json_normalize(result)
                    update_table_schema(cursor, table_name, df.columns)

                    columns = ", ".join(
                        [
                            f"[{col}]"
                            for col in df.columns
                            if col not in ["id", "timestamp"]
                        ]
                    )
                    placeholders = ", ".join(
                        ["?" for _ in df.columns if _ not in ["id", "timestamp"]]
                    )
                    cursor.execute(
                        f"INSERT OR REPLACE INTO [{table_name}] (id, {columns}) VALUES (?, {placeholders})",
                        (key, *df.iloc[0].drop(["id", "timestamp"], errors="ignore")),
                    )
                    conn.commit()

                    return df

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
            if len(args) > 0 and isinstance(args[0], pd.DataFrame):
                df = args[0]
            elif "df" in kwargs and isinstance(kwargs["df"], pd.DataFrame):
                df = kwargs["df"]
            else:
                raise ValueError(
                    "The first argument or 'df' keyword argument must be a pandas DataFrame"
                )

            table_name = f"{func.__module__}.{func.__name__}"

            with get_connection() as (conn, cursor):
                create_table(cursor, table_name)

                # Process the DataFrame in batches
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i : i + batch_size]
                    batch_dict = batch.to_dict(orient="records")

                    # Generate keys for each row in the batch
                    keys = [create_key(func.__name__, (), row) for row in batch_dict]

                    # Check which rows are already in the cache
                    placeholders = ",".join(["?" for _ in keys])
                    cursor.execute(
                        f"SELECT id FROM [{table_name}] WHERE id IN ({placeholders})",
                        keys,
                    )
                    cached_keys = set(row[0] for row in cursor.fetchall())

                    # Process only uncached rows
                    uncached_rows = [
                        row
                        for row, key in zip(batch_dict, keys)
                        if key not in cached_keys
                    ]

                    if uncached_rows:
                        # Get AI-generated data for uncached rows
                        ai_responses = parse_ai_response_batch(
                            func.__name__, uncached_rows, api_key, base_url, model
                        )

                        # Update table schema and insert new data
                        for response in ai_responses:
                            update_table_schema(cursor, table_name, response.keys())
                            columns = list(response.keys())
                            placeholders = ", ".join(["?" for _ in columns])
                            values = [response[col] for col in columns]
                            cursor.execute(
                                f"INSERT OR REPLACE INTO [{table_name}] (id, {', '.join(columns)}) VALUES (?, {placeholders})",
                                (create_key(func.__name__, (), response), *values),
                            )

                    conn.commit()

                # Retrieve all data from the cache
                cursor.execute(f"SELECT * FROM [{table_name}]")
                cached_data = cursor.fetchall()
                columns = [description[0] for description in cursor.description]
                cached_df = pd.DataFrame(cached_data, columns=columns)

                # Merge cached data with original DataFrame
                result_df = pd.merge(
                    df,
                    cached_df.drop(["id", "timestamp"], axis=1),
                    left_index=True,
                    right_index=True,
                    how="left",
                )

                # Call the original function with the extended DataFrame
                result = func(result_df, *args[1:], **kwargs)

                return result

        return wrapper

    return decorator
