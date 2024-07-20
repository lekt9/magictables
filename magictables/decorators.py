import functools
import logging
import json
from typing import Callable, Dict, TypeVar
from typing_extensions import ParamSpec
import polars as pl
from .database import magic_db
from .utils import (
    generate_call_id,
    generate_row_id,
    call_ai_model,
    transform_data_with_query,
    ensure_dataframe,
)

P = ParamSpec("P")
R = TypeVar("R")


def mtable(
    query: str = None,
    input_schema: Dict[str, str] = None,
    output_schema: Dict[str, str] = None,
):
    def decorator(f: Callable[P, R]) -> Callable[P, pl.DataFrame]:
        @functools.wraps(f)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> pl.DataFrame:
            call_id = generate_call_id(f, *args, **kwargs)
            table_name = f"magic_{f.__name__}"

            # Check cache first
            cached_result = magic_db.get_cached_result(table_name, call_id)
            if cached_result is not None and not cached_result.is_empty():
                print(f"Cache hit for {f.__name__}")
                raw_df = cached_result
            else:
                print(f"Cache miss for {f.__name__}")
                # Execute the function
                result = f(*args, **kwargs)
                raw_df = ensure_dataframe(result)

                # Generate row IDs
                raw_df = raw_df.with_columns(
                    pl.struct(raw_df.columns).map_elements(generate_row_id).alias("id")
                )

                # Cache the raw results
                magic_db.cache_results(table_name, raw_df, call_id)

            # Get or generate input schema
            if input_schema is None:
                input_schema_prompt = f"Generate an input schema for the function {f.__name__} with the following signature: {f.__annotations__}"
                input_schema_result = call_ai_model(
                    [{"role": "user", "content": input_schema_prompt}]
                )
                logging.debug(f"Input schema result: {input_schema_result}")

                content = input_schema_result[0].get("content", {})
                if isinstance(content, str):
                    try:
                        content = json.loads(content)
                    except json.JSONDecodeError:
                        logging.error(
                            f"Failed to parse input schema content: {content}"
                        )
                        content = {}

                generated_input_schema = content.get("input_schema", {})
                if not generated_input_schema:
                    logging.warning("Generated input schema is empty")
            else:
                generated_input_schema = input_schema

            # Get or generate output schema
            if output_schema is None:
                output_schema_prompt = f"Generate an output schema for the function {f.__name__} based on the following data sample: {raw_df.head().to_dict()}"
                output_schema_result = call_ai_model(
                    [{"role": "user", "content": output_schema_prompt}]
                )
                logging.debug(f"Output schema result: {output_schema_result}")

                content = output_schema_result[0].get("content", {})
                if isinstance(content, str):
                    try:
                        content = json.loads(content)
                    except json.JSONDecodeError:
                        logging.error(
                            f"Failed to parse output schema content: {content}"
                        )
                        content = {}

                generated_output_schema = content.get("output_schema", {})
                if not generated_output_schema:
                    logging.warning("Generated output schema is empty")
            else:
                generated_output_schema = output_schema

            # Transform data based on the query and generated output schema
            if query:
                output_df = transform_data_with_query(
                    raw_df, query, f.__name__, generated_output_schema
                )
            else:
                output_df = raw_df

            # # Apply Pandera schema validation TODO figure this shiz out
            # schema_class = load_schema_class(table_name)
            # if schema_class:
            #     try:
            #         output_df = schema_class.validate(output_df)
            #     except pa.errors.SchemaError as e:
            #         logging.error(f"Schema validation error: {str(e)}")
            #         # Handle the error as needed, e.g., return the original DataFrame
            #         return output_df
            # Store the schemas and function metadata
            wrapper.input_schema = generated_input_schema
            wrapper.output_schema = generated_output_schema
            wrapper.function_name = f.__name__

            return output_df

        wrapper.input_schema = input_schema
        wrapper.output_schema = output_schema
        wrapper.function_name = f.__name__

        return wrapper

    return decorator
