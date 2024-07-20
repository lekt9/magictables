import ast
import polars as pl
import functools
import logging
from typing import Any, Callable, Dict, TypeVar, Optional
from typing_extensions import ParamSpec
from .database import magic_db
from .utils import (
    apply_mapping,
    generate_call_id,
    generate_row_id,
    call_ai_model,
)

P = ParamSpec("P")
R = TypeVar("R")


def flatten_nested_structure(nested_structure):
    flattened_rows = []

    def flatten(obj, prefix=""):
        if isinstance(obj, dict):
            row = {}
            for key, value in obj.items():
                new_key = f"{prefix}_{key}" if prefix else key
                if isinstance(value, (dict, list)):
                    flatten(value, new_key)
                else:
                    row[new_key] = value
            if row:
                flattened_rows.append(row)
        elif isinstance(obj, list):
            for item in obj:
                flatten(item, prefix)
        else:
            flattened_rows.append({prefix: obj})

    flatten(nested_structure)
    return flattened_rows


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

                print("result", result)

                # Flatten the nested structure
                flattened_data = flatten_nested_structure(result)

                # Convert flattened data to DataFrame
                df = pl.DataFrame(flattened_data)

                logging.info(f"1df shape: {df.shape}")

                # If query is provided, generate and apply mapping
                if query:
                    next_function_params = getattr(f, "next_function_params", {})
                    mapping = generate_mapping(df, query, next_function_params)
                    logging.info(f"mapping: {mapping}")
                    df = apply_mapping(df, mapping)
                    magic_db.store_mapping(table_name, mapping)
                    logging.info(f"Stored mapping for {table_name}")

                # Generate row IDs
                df = df.with_columns(
                    pl.struct(df.columns).map_elements(generate_row_id).alias("id")
                )

                # Add function parameters as columns
                for arg_name, arg_value in kwargs.items():
                    if isinstance(arg_value, (int, float, str, bool)):
                        df = df.with_columns(pl.lit(arg_value).alias(arg_name))

                # Cache the flattened results
                magic_db.cache_results(table_name, df, call_id)

                return df

        wrapper.function_name = f.__name__
        wrapper.query = query
        wrapper.func_params = f.__annotations__
        return wrapper

    return decorator


def generate_mapping(
    df: pl.DataFrame, query: str, next_function_params: Dict[str, Any]
) -> dict:
    # Generate a schema of the current DataFrame
    current_schema = {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)}

    # Validate next function parameters against the current schema
    valid_params = {
        param: param_type
        for param, param_type in next_function_params.items()
        if param in current_schema
    }

    if len(valid_params) != len(next_function_params):
        missing_params = set(next_function_params) - set(valid_params)
        logging.warning(f"Missing parameters in DataFrame columns: {missing_params}")

    # Prepare the input data for the AI model
    input_data = {
        "current_schema": current_schema,
        "query": query,
        "next_function_params": next_function_params,
    }

    # Prepare the prompt for the AI model
    prompt = """
    Given the current DataFrame schema, the query, and the parameters required by the next function in the chain,
    generate a mapping to transform the Polars DataFrame. 
    The mapping should be a dictionary where:
    - Keys are the desired column names (matching the next function's parameters if possible)
    - Values are Python expressions using the current column names and Polars functions

    You can use any Polars functions or Python operations. For example:
    - "new_col": "pl.col('existing_col') * 2"
    - "combined": "pl.col('col1') + ' ' + pl.col('col2')"
    - "transformed": "pl.when(pl.col('col') > 10).then(pl.lit('High')).otherwise(pl.lit('Low'))"

    Ensure that the mapping includes all required parameters for the next function.
    If a required parameter is not directly available, try to derive it from existing columns.

    Return the mapping as a Python dictionary.
    """

    # Call the AI model
    ai_response = call_ai_model(input_data, prompt)

    if ai_response and isinstance(ai_response, dict) and "mapping" in ai_response:
        # Extract the mapping from the AI response
        mapping = ai_response["mapping"]
        # Validate and sanitize the mapping
        sanitized_mapping = {}
        for key, value in mapping.items():
            try:
                # Parse the expression to check for syntax errors
                ast.parse(value)
                # If parsing succeeds, add to sanitized mapping
                sanitized_mapping[key] = value
            except SyntaxError:
                logging.warning(f"Invalid Python expression for key '{key}': {value}")

        logging.info(f"AI-generated mapping: {sanitized_mapping}")
        return sanitized_mapping
    else:
        logging.warning("AI could not generate a suitable mapping.")
        return {}
