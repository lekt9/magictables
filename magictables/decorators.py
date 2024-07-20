import functools
import hashlib
import json
import logging
from typing import Any, Callable, Optional, Union, List, Dict, TypeVar, cast

import pandas as pd

from magictables.database import magic_db
from magictables.utils import ensure_dataframe, call_ai_model, generate_ai_descriptions

T = TypeVar("T", bound=Callable[..., Any])


def generate_call_id(func: Callable, *args: Any, **kwargs: Any) -> str:
    def default_serializer(obj):
        if isinstance(obj, pd.DataFrame):
            return obj.to_dict(orient="records")
        if hasattr(obj, "__dict__"):
            return obj.__dict__
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    call_data = (func.__name__, str(args), str(kwargs))
    try:
        return hashlib.md5(
            json.dumps(call_data, sort_keys=True, default=default_serializer).encode()
        ).hexdigest()
    except TypeError:
        # If serialization fails, use a simpler approach
        return hashlib.md5(str(call_data).encode()).hexdigest()


def generate_row_id(row: Union[Dict[str, Any], pd.Series]) -> str:
    if isinstance(row, pd.Series):
        row_dict = row.to_dict()
    else:
        row_dict = row
    row_data = json.dumps(row_dict, sort_keys=True, default=str)
    return hashlib.md5(row_data.encode()).hexdigest()


def mtable(func: Optional[Callable] = None) -> Callable[[T], T]:
    def decorator(f: T) -> T:
        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            call_id = generate_call_id(f, *args, **kwargs)
            table_name = f"magic_{f.__name__}"

            cached_result = magic_db.get_cached_result(table_name, call_id)
            if cached_result is not None and not cached_result.empty:
                print(f"Cache hit for {f.__name__}")
                return cached_result

            print(f"Cache miss for {f.__name__}")
            result = f(*args, **kwargs)
            result_df = ensure_dataframe(result)

            # Generate row IDs
            result_df["id"] = result_df.apply(generate_row_id, axis=1)

            magic_db.cache_results(table_name, result_df, call_id)

            return result_df

        return cast(T, wrapper)

    return decorator if func is None else decorator(func)


def mai(
    func: Optional[Callable] = None,
    *,
    batch_size: int = 10,
    mode: str = "generate",
    query: Optional[str] = None,
) -> Callable[[T], T]:
    def decorator(f: T) -> T:
        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            call_id = generate_call_id(f, *args, **kwargs)
            table_name = f"ai_{f.__name__}"

            logging.info(f"Checking cache for {f.__name__} with call_id: {call_id}")
            cached_result = magic_db.get_cached_result(table_name, call_id)
            if cached_result is not None and not cached_result.empty:
                logging.info(f"Cache hit for {f.__name__}")
                return cached_result

            logging.info(f"Cache miss for {f.__name__}")
            result = f(*args, **kwargs)
            result_df = ensure_dataframe(result)

            if result_df.empty:
                logging.warning(f"Empty DataFrame returned by {f.__name__}")
                return result_df

            # Generate row IDs
            result_df["id"] = result_df.apply(generate_row_id, axis=1)

            data = result_df.to_dict("records")
            logging.info(f"Processing batches for {f.__name__}")
            ai_results = process_batches(table_name, data, batch_size, query)

            if not ai_results:
                logging.warning(f"AI model returned no results for {f.__name__}")
                return result_df

            logging.info(f"Combining results for {f.__name__}")
            result_df = combine_results(result_df, ai_results, mode, query)

            if result_df.empty:
                logging.warning(
                    f"combine_results returned an empty DataFrame for {f.__name__}"
                )
                return result_df

            logging.info(f"Caching results for {f.__name__}")
            magic_db.cache_results(table_name, result_df, call_id)

            logging.info(f"Generating AI descriptions for {f.__name__}")
            generate_ai_descriptions(table_name, result_df.columns.tolist())

            return result_df

        return cast(T, wrapper)

    return decorator if func is None else decorator(func)


def process_batches(
    table_name: str,
    data: List[Dict[str, Any]],
    batch_size: int,
    query: Optional[str] = None,
) -> List[Dict[str, Any]]:
    ai_results = []
    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        new_results = call_ai_model(batch, query)
        ai_results.extend(new_results)
    return ai_results


def combine_results(
    original_df: pd.DataFrame,
    ai_results: List[Dict[str, Any]],
    mode: str,
    query: Optional[str],
) -> pd.DataFrame:
    if mode == "generate":
        new_rows = pd.DataFrame(ai_results)
        if new_rows.empty:
            logging.warning("AI generated an empty DataFrame in 'generate' mode")
            return original_df
        return pd.concat([original_df, new_rows], ignore_index=True)
    elif mode == "augment":
        result_df = original_df.copy()
        ai_results = ai_results[: len(result_df)]
        for i, ai_result in enumerate(ai_results):
            for key, value in ai_result.items():
                if key in result_df.columns:
                    if value is not None and pd.notna(value):
                        result_df.at[i, key] = value
                else:
                    result_df[key] = None
                    result_df.at[i, key] = value
        return result_df
    else:
        raise ValueError("Invalid mode. Must be either 'generate' or 'augment'")
