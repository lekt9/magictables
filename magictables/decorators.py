import ast
import polars as pl

import functools
import logging
from typing import Any, Callable, Dict, TypeVar, Optional
from typing_extensions import ParamSpec
import polars as pl
from .database import magic_db
from .utils import (
    apply_mapping,
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
                logging.info(f"1df shape: {df.shape}")

                # If query is provided, generate and apply mapping
                if query:
                    # Get the parameters of the next function in the chain
                    next_function_params = getattr(f, "next_function_params", {})
                    mapping = generate_mapping(df, query, next_function_params)
                    logging.info(f"mapping: {mapping}")
                    df = apply_mapping(df, mapping)
                    # Store the mapping in the database
                    magic_db.store_mapping(table_name, mapping)
                    logging.info(f"Stored mapping for {table_name}")

                # Flatten the DataFrame to 1NF
                df = flatten_dataframe(df)

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
        wrapper.query = query  # Store the query for later use
        wrapper.func_params = f.__annotations__  # Store the function parameters
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


# def apply_mapping(df: pl.DataFrame, mapping: Dict[str, Any]) -> pl.DataFrame:
#     logging.info(f"Current DataFrame columns: {df.columns}")

#     valid_mappings = {}
#     for new_col, expr in mapping.items():
#         try:
#             # Evaluate the string as a Polars expression
#             valid_mappings[new_col] = eval(expr)
#         except Exception as e:
#             logging.warning(
#                 f"Skipping invalid mapping for '{new_col}': {expr}. Error: {str(e)}"
#             )

#     if not valid_mappings:
#         logging.warning("No valid mappings found. Returning original DataFrame.")
#         return df

#     try:
#         return df.with_columns(
#             [expr.alias(new_col) for new_col, expr in valid_mappings.items()]
#         )
#     except Exception as e:
#         logging.error(f"Error applying mappings: {str(e)}")
#         return df
