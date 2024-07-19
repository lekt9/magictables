import functools
import hashlib
import json
from typing import Any, Callable, TypeVar, Dict, List, Optional
import pandas as pd
import sqlite3

from magictables.database import (
    cache_results,
    cache_result,
    check_cache_and_get_new_items,
    create_table,
    create_tables_for_nested_data,
    get_cached_result,
    get_connection,
    infer_sqlite_type,
    insert_nested_data,
    sanitize_sql_name,
)
from magictables.schema_generator import update_generated_types
from magictables.utils import call_ai_model, generate_ai_descriptions

T = TypeVar("T")


def generate_call_id(func: Callable, *args: Any, **kwargs: Any) -> str:
    """Generate a unique ID for a function call."""
    call_data = (func.__name__, args, kwargs)
    return hashlib.md5(json.dumps(call_data, sort_keys=True).encode()).hexdigest()


def ensure_dataframe(result: Any) -> pd.DataFrame:
    """Ensure the result is a DataFrame."""
    if isinstance(result, pd.DataFrame):
        return result
    elif isinstance(result, dict):
        return pd.DataFrame([result])
    elif isinstance(result, list) and all(isinstance(item, dict) for item in result):
        return pd.DataFrame(result)
    else:
        return pd.DataFrame({"result": [result]})


def mtable() -> Callable[[Callable[..., Any]], Callable[..., pd.DataFrame]]:
    def decorator(func: Callable[..., Any]) -> Callable[..., pd.DataFrame]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> pd.DataFrame:
            call_id = generate_call_id(func, *args, **kwargs)
            table_name = sanitize_sql_name(f"magic_{func.__name__}")

            with get_connection() as (conn, cursor):
                cached_result = get_cached_result(cursor, table_name, call_id)
                if cached_result is not None:
                    print(f"Cache hit for {func.__name__}")
                    return pd.DataFrame(cached_result)

                print(f"Cache miss for {func.__name__}")
                result = func(*args, **kwargs)
                if result is None:
                    return pd.DataFrame()

                result_df = ensure_dataframe(result)
                columns = [
                    (str(col), infer_sqlite_type(dtype))
                    for col, dtype in result_df.dtypes.items()
                ]
                create_table(cursor, table_name, columns)
                cache_result(cursor, table_name, call_id, result_df)
                conn.commit()
                update_generated_types(conn)

            return result_df

        return wrapper

    return decorator


def mai(
    batch_size: int = 10, mode: str = "generate"
) -> Callable[[Callable[..., Any]], Callable[..., pd.DataFrame]]:
    def decorator(func: Callable[..., Any]) -> Callable[..., pd.DataFrame]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> pd.DataFrame:
            result = func(*args, **kwargs)
            result_df = ensure_dataframe(result)
            data = result_df.to_dict("records")

            table_name = f"ai_{func.__name__}"

            with get_connection() as (conn, cursor):
                columns = [
                    (str(col), infer_sqlite_type(dtype))
                    for col, dtype in result_df.dtypes.items()
                ]
                create_table(cursor, table_name, columns)
                ai_results = process_batches(cursor, table_name, data, batch_size)
                conn.commit()

                generate_ai_descriptions(table_name, result_df.columns.tolist())

            return combine_results(result_df, ai_results, mode)

        return wrapper

    return decorator


def process_batches(
    cursor: sqlite3.Cursor, table_name: str, data: List[Dict[str, Any]], batch_size: int
) -> List[Dict[str, Any]]:
    ai_results = []
    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        keys = [
            hashlib.md5(json.dumps(item, sort_keys=True).encode()).hexdigest()
            for item in batch
        ]
        cached_results, new_items, new_keys = check_cache_and_get_new_items(
            cursor, table_name, batch, keys
        )

        if new_items:
            new_results = call_ai_model(new_items)
            cache_results(cursor, table_name, new_keys, new_results)
            cached_results.update(dict(zip(new_keys, new_results)))

        ai_results.extend([cached_results[key] for key in keys])
    return ai_results


def combine_results(
    original_df: pd.DataFrame, ai_results: List[Dict[str, Any]], mode: str
) -> pd.DataFrame:
    if mode == "generate":
        return pd.DataFrame(ai_results)
    elif mode == "augment":
        for idx, ai_result in enumerate(ai_results):
            for key, value in ai_result.items():
                original_df.loc[idx, f"ai_{key}"] = value
        return original_df
    else:
        raise ValueError("Invalid mode. Must be either 'generate' or 'augment'")
