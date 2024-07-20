import functools
import hashlib
import json
from typing import Any, Callable, Optional, Union, List, Dict, TypeVar, cast

from magictables.database import magic_db
from magictables.utils import ensure_dataframe, call_ai_model, generate_ai_descriptions

T = TypeVar("T", bound=Callable[..., Any])

def generate_call_id(func: Callable, *args: Any, **kwargs: Any) -> str:
    call_data = (func.__name__, args, kwargs)
    return hashlib.md5(
        json.dumps(call_data, sort_keys=True, default=ensure_dataframe).encode()
    ).hexdigest()

def mtable(func: Optional[Callable] = None) -> Callable[[T], T]:
    def decorator(f: T) -> T:
        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            call_id = generate_call_id(f, *args, **kwargs)
            table_name = f"magic_{f.__name__}"

            cached_result = magic_db.get_cached_result(table_name, call_id)
            if cached_result is not None:
                print(f"Cache hit for {f.__name__}")
                return cached_result

            print(f"Cache miss for {f.__name__}")
            result = f(*args, **kwargs)
            result_df = ensure_dataframe(result)

            magic_db.insert_nested_data(table_name, result_df, call_id)

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

            cached_result = magic_db.get_cached_result(table_name, call_id)
            if cached_result is not None:
                print(f"Cache hit for {f.__name__}")
                return cached_result

            print(f"Cache miss for {f.__name__}")
            result = f(*args, **kwargs)
            result_df = ensure_dataframe(result)

            data = result_df.to_dict("records")
            ai_results = process_batches(table_name, data, batch_size, query)
            result_df = combine_results(result_df, ai_results, mode, query)

            magic_db.insert_nested_data(table_name, result_df, call_id)

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
                    result_df.at[i, key] = value
        return result_df
    else:
        raise ValueError("Invalid mode. Must be either 'generate' or 'augment'")