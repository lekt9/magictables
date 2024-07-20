import functools
import hashlib
import json
import logging
import importlib
from typing import Any, Callable, Optional, Type, Union, List, Dict, TypeVar, cast
import pandas as pd
from pandera import DataFrameModel
import polars as pl
from pandera.typing import DataFrame as PanderaDataFrame
from pandera.engines import polars_engine as pa
from magictables.database import magic_db
from magictables.utils import (
    ensure_dataframe,
    call_ai_model,
    generate_ai_descriptions,
    generate_call_id,
    generate_row_id,
    load_schema_class,
)

T = TypeVar("T", bound=Callable[..., Any])


def is_pandas_dataframe(df: Any) -> bool:
    return isinstance(df, pd.DataFrame)


def pandas_to_polars(df: pd.DataFrame) -> pl.DataFrame:
    return pl.from_pandas(df)


def polars_to_pandas(df: pl.DataFrame) -> pd.DataFrame:
    return df.to_pandas()


def mtable(func: Optional[Callable] = None) -> Callable[[T], T]:
    def decorator(f: T) -> T:
        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            call_id = generate_call_id(f, *args, **kwargs)
            table_name = f"magic_{f.__name__}"

            cached_result = magic_db.get_cached_result(table_name, call_id)
            if cached_result is not None and not cached_result.is_empty():
                print(f"Cache hit for {f.__name__}")
                return cached_result

            print(f"Cache miss for {f.__name__}")
            result = f(*args, **kwargs)

            input_was_pandas = is_pandas_dataframe(result)
            if input_was_pandas:
                result = pandas_to_polars(result)

            result_df = ensure_dataframe(result)

            # Generate row IDs
            result_df = result_df.with_columns(
                pl.struct(result_df.columns).map_elements(generate_row_id).alias("id")
            )

            magic_db.cache_results(table_name, result_df, call_id)

            # Apply Pandera schema validation
            schema_class = load_schema_class(table_name)
            if schema_class:
                result_df = schema_class.validate(result_df)

            if input_was_pandas:
                return polars_to_pandas(result_df)
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
            if cached_result is not None and not cached_result.is_empty():
                logging.info(f"Cache hit for {f.__name__}")
                return cached_result

            logging.info(f"Cache miss for {f.__name__}")
            result = f(*args, **kwargs)

            input_was_pandas = is_pandas_dataframe(result)
            if input_was_pandas:
                result = pandas_to_polars(result)

            result_df = ensure_dataframe(result)

            if result_df.is_empty():
                logging.warning(f"Empty DataFrame returned by {f.__name__}")
                return result_df

            # Generate row IDs
            result_df = result_df.with_columns(
                pl.struct(result_df.columns).map_elements(generate_row_id).alias("id")
            )

            data = result_df.to_dicts()
            logging.info(f"Processing batches for {f.__name__}")
            ai_results = process_batches(table_name, data, batch_size, query)

            if not ai_results:
                logging.warning(f"AI model returned no results for {f.__name__}")
                return result_df

            logging.info(f"Combining results for {f.__name__}")
            result_df = combine_results(result_df, ai_results, mode, query)

            if result_df.is_empty():
                logging.warning(
                    f"combine_results returned an empty DataFrame for {f.__name__}"
                )
                return result_df

            logging.info(f"Caching results for {f.__name__}")
            magic_db.cache_results(table_name, result_df, call_id)

            logging.info(f"Generating AI descriptions for {f.__name__}")
            generate_ai_descriptions(table_name, result_df.columns)

            # Apply Pandera schema validation
            schema_class = load_schema_class(table_name)
            if schema_class:
                result_df = schema_class.validate(result_df)

            if input_was_pandas:
                return polars_to_pandas(result_df)
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
    original_df: pl.DataFrame,
    ai_results: List[Dict[str, Any]],
    mode: str,
    query: Optional[str],
) -> pl.DataFrame:
    if mode == "generate":
        new_rows = pl.DataFrame(ai_results)
        if new_rows.is_empty():
            logging.warning("AI generated an empty DataFrame in 'generate' mode")
            return original_df
        return pl.concat([original_df, new_rows])
    elif mode == "augment":
        result_df = original_df.clone()
        ai_results = ai_results[: len(result_df)]
        for i, ai_result in enumerate(ai_results):
            for key, value in ai_result.items():
                if value is not None and not pl.Series([value]).is_null().any():
                    if key in result_df.columns:
                        # If the column already exists, update it
                        result_df = result_df.with_columns(
                            pl.when(pl.arange(0, len(result_df)) == i)
                            .then(pl.lit(value))
                            .otherwise(pl.col(key))
                            .alias(key)
                        )
                    else:
                        # If it's a new column, add it with a unique name
                        new_key = key
                        counter = 1
                        while new_key in result_df.columns:
                            new_key = f"{key}_{counter}"
                            counter += 1
                        result_df = result_df.with_columns(
                            pl.when(pl.arange(0, len(result_df)) == i)
                            .then(pl.lit(value))
                            .otherwise(pl.lit(None))
                            .alias(new_key)
                        )
        return result_df
    else:
        raise ValueError("Invalid mode. Must be either 'generate' or 'augment'")
