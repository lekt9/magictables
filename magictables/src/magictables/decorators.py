import functools
from typing import Callable
import pandas as pd
from .database import get_connection, create_table, update_table_schema
from .utils import create_key, parse_ai_response


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
):
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
                    response_data = dict(zip(columns, cached_result))
                    response_data.pop("id", None)
                    response_data.pop("timestamp", None)
                else:
                    response_data = parse_ai_response(
                        func.__name__, kwargs, api_key, base_url, model
                    )
                    update_table_schema(cursor, table_name, response_data.keys())

                    columns = list(response_data.keys())
                    placeholders = ", ".join(["?" for _ in columns])
                    values = [v for v in response_data.values()]

                    cursor.execute(
                        f"INSERT OR REPLACE INTO [{table_name}] (id, {', '.join(columns)}) VALUES (?, {placeholders})",
                        (key, *values),
                    )
                    conn.commit()

                merged_kwargs = {**kwargs, **response_data}
                result = func(*args, **merged_kwargs)

                return result

        return wrapper

    return decorator
