# decorators.py

import functools
from typing import Callable, TypeVar, Optional
from typing_extensions import ParamSpec
import polars as pl
from .database import magic_db
from .utils import (
    flatten_dataframe,
    generate_call_id,
    generate_row_id,
    ensure_dataframe,
    call_ai_model,
)

P = ParamSpec("P")
R = TypeVar("R")


def mtable(query: Optional[str] = None):
    def decorator(f: Callable[P, R]) -> Callable[P, pl.DataFrame]:
        @functools.wraps(f)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> pl.DataFrame:
            call_id = generate_call_id(f, *args, **kwargs)
            table_name = f"magic_{f.__name__}"

            # Check cache first
            cached_result = magic_db.get_cached_result(table_name, call_id)
            if cached_result is not None and not cached_result.is_empty():
                print(f"Cache hit for {f.__name__}")
                return cached_result
            else:
                print(f"Cache miss for {f.__name__}")
                # Execute the function
                result = f(*args, **kwargs)
                df = ensure_dataframe(result)

                # If query is provided, generate and apply mapping
                if query:
                    mapping = generate_mapping(df, query)
                    df = apply_mapping(df, mapping)
                    # Store the mapping in the database
                    magic_db.store_mapping(table_name, mapping)

                # Flatten the DataFrame to 1NF
                df = flatten_dataframe(df)

                # Generate row IDs
                df = df.with_columns(
                    pl.struct(df.columns).map_elements(generate_row_id).alias("id")
                )

                # Cache the flattened results
                magic_db.cache_results(table_name, df, call_id)

                return df

        wrapper.function_name = f.__name__
        wrapper.query = query  # Store the query for later use
        return wrapper

    return decorator


def generate_mapping(df: pl.DataFrame, query: str) -> dict:
    # Generate a schema of the current DataFrame
    current_schema = {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)}

    # Use AI to generate a mapping based on the query and current schema
    prompt = f"""
    Given the current DataFrame schema:
    {current_schema}
    
    And the following query:
    "{query}"
    
    Generate a mapping to transform the Polars DataFrame. The mapping should be a dictionary where:
    - Keys are the desired column names
    - Values are expressions using the current column names
    
    Return the mapping as a Python dictionary.
    """

    mapping_result = call_ai_model([{"role": "user", "content": prompt}])
    print("mapping", mapping_result)
    return mapping_result[0]["content"]


def apply_mapping(df: pl.DataFrame, mapping: dict) -> pl.DataFrame:

    for new_col, expr in mapping.items():
        # Check if the expression is just a column name
        if expr in df.columns:
            df = df.with_columns(pl.col(expr).alias(new_col))
        else:
            # If it's an expression, use pl.expr
            df = df.with_columns(pl.expr(expr).alias(new_col))

    # Keep only the columns specified in the mapping
    df = df.select(list(mapping.keys()))
    return df
