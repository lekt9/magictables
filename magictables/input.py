import polars as pl
from functools import wraps
from .database import magic_db
from .utils import generate_call_id, call_ai_model, generate_row_id

from typing import List, Union, Dict, Any

# Parsee imports
from parsee import Parsee
from parsee.readers import PDFReader, WebReader
from parsee.extractors import TextExtractor, TableExtractor


def extract_content(input_data: str, input_type: str) -> Dict[str, Any]:
    parsee = Parsee()

    if input_type == "pdf":
        reader = PDFReader(input_data)
    elif input_type in ["url", "html"]:
        reader = WebReader(input_data)
    else:
        raise ValueError(f"Unsupported input type: {input_type}")

    parsee.add_reader(reader)
    parsee.add_extractor(TextExtractor())
    parsee.add_extractor(TableExtractor())

    result = parsee.parse()

    return {
        "text": result.get("text", ""),
        "tables": result.get("tables", []),
        "metadata": result.get("metadata", {}),
    }


def chunk_text(text: str, max_chars: int = 4000) -> pl.DataFrame:
    lines = text.split("\n")
    df = pl.DataFrame({"line": lines})
    df = df.with_columns(
        [
            pl.col("line").str.len().alias("length"),
            pl.col("line").str.len().cumsum().alias("cum_length"),
        ]
    )
    df = df.with_columns(
        (pl.col("cum_length") / max_chars).cast(pl.Int64).alias("chunk_id")
    )
    return df.group_by("chunk_id").agg([pl.col("line").str.concat("\n").alias("chunk")])


def process_input(
    input_data: Union[str, List[str]], input_type: str, max_chars: int = 4000
) -> pl.DataFrame:
    if isinstance(input_data, list):
        df = pl.DataFrame({"input": input_data})
    else:
        df = pl.DataFrame({"input": [input_data]})

    df = df.with_columns(
        [
            pl.col("input")
            .map_elements(lambda x: extract_content(x, input_type))
            .alias("content")
        ]
    )

    df = df.with_columns(
        [
            pl.col("content").struct.field("text").alias("text"),
            pl.col("content").struct.field("tables").alias("tables"),
            pl.col("content").struct.field("metadata").alias("metadata"),
        ]
    )

    df = df.with_columns(
        [
            pl.col("text")
            .map_elements(lambda x: chunk_text(x, max_chars))
            .alias("chunks")
        ]
    )

    df = df.explode("chunks")

    df = df.with_columns(
        [
            pl.col("chunks")
            .map_elements(
                lambda x: call_ai_model({"text": x}, "Summarize the following text:")
            )
            .alias("summary")
        ]
    )

    return df


def msource(query: str = None, input_type: str = "auto", max_chars: int = 4000):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            input_data = args[0] if args else kwargs.get("input_data")
            if not input_data:
                raise ValueError("Input data not provided")

            # Automatically detect input type if set to "auto"
            if input_type == "auto":
                detected_type = detect_input_type(input_data)
            else:
                detected_type = input_type

            # Process input
            processed_df = process_input(input_data, detected_type, max_chars)

            # Generate call_id for each input
            processed_df = processed_df.with_columns(
                [
                    pl.struct(processed_df.columns)
                    .map_elements(lambda x: generate_call_id(func, x, **kwargs))
                    .alias("call_id")
                ]
            )

            table_name = f"magic_{func.__name__}"

            # Check cache
            cached_results_dict = magic_db.get_cached_results(
                [table_name], processed_df["call_id"].to_list()
            )

            # Extract cached results for the specific table
            cached_results = cached_results_dict.get(table_name, pl.DataFrame())

            # Process non-cached results
            non_cached_df = processed_df.filter(
                ~pl.col("call_id").is_in(cached_results["call_id"])
                if not cached_results.is_empty()
                else True
            )
            if non_cached_df.height > 0:
                # Call the wrapped function to get the desired output structure with example values
                output_structure = func(non_cached_df["summary"][0])

                # Generate content based on the output structure and example values
                if isinstance(output_structure, dict):
                    result_df = generate_dict_content(
                        non_cached_df, output_structure, query
                    )
                elif isinstance(output_structure, list):
                    result_df = generate_list_content(
                        non_cached_df, output_structure, query
                    )
                elif isinstance(output_structure, pl.DataFrame):
                    result_df = generate_dataframe_content(
                        non_cached_df, output_structure, query
                    )
                else:
                    raise ValueError("Unsupported output structure")

                # Generate row IDs
                result_df = result_df.with_columns(
                    [
                        pl.struct(result_df.columns)
                        .map_elements(generate_row_id)
                        .alias("id")
                    ]
                )

                # Add function parameters as columns
                for arg_name, arg_value in kwargs.items():
                    if isinstance(arg_value, (int, float, str, bool)):
                        result_df = result_df.with_columns(
                            pl.lit(arg_value).alias(arg_name)
                        )

                # Cache the results
                for row in result_df.iter_rows(named=True):
                    call_id = row["call_id"]
                    row_df = pl.DataFrame([row])
                    magic_db.cache_results(table_name, row_df, call_id)

                # Combine cached and new results
                final_df = pl.concat([cached_results, result_df])
            else:
                final_df = cached_results

            return final_df if isinstance(input_data, list) else final_df.row(0)

        wrapper.function_name = func.__name__
        wrapper.query = query
        wrapper.func_params = func.__annotations__
        return wrapper

    return decorator


def detect_input_type(input_data: str) -> str:
    if input_data.lower().endswith(".pdf"):
        return "pdf"
    elif input_data.startswith(("http://", "https://")):
        return "url"
    elif input_data.strip().startswith("<"):
        return "html"
    else:
        return "text"


def generate_dict_content(
    df: pl.DataFrame, structure: Dict[str, Any], query: str
) -> pl.DataFrame:
    example_str = ", ".join([f"{k}: {v}" for k, v in structure.items()])
    prompt = f"""
    Based on the following text, generate a dictionary with the same structure as this example: {example_str}.
    Use the example values as a guide for the format and type of information to extract.
    Query: {query}
    """
    return df.with_columns(
        [
            pl.col("summary")
            .map_elements(lambda x: call_ai_model({"text": x}, prompt))
            .alias("result")
        ]
    )


def generate_list_content(
    df: pl.DataFrame, structure: List[Dict[str, Any]], query: str
) -> pl.DataFrame:
    example_str = ", ".join([f"{k}: {v}" for k, v in structure[0].items()])
    prompt = f"""
    Based on the following text, generate a list of dictionaries. Each dictionary should have the same structure as this example: {example_str}.
    Use the example values as a guide for the format and type of information to extract.
    Query: {query}
    """
    return df.with_columns(
        [
            pl.col("summary")
            .map_elements(lambda x: call_ai_model({"text": x}, prompt))
            .alias("result")
        ]
    )


def generate_dataframe_content(
    df: pl.DataFrame, df_structure: pl.DataFrame, query: str
) -> pl.DataFrame:
    example_str = ", ".join(
        [f"{col}: {df_structure[col][0]}" for col in df_structure.columns]
    )
    prompt = f"""
    Based on the following text, generate data for a DataFrame with columns {df_structure.columns}.
    Use this example row as a guide for the format and type of information to extract: {example_str}
    Query: {query}
    """
    return df.with_columns(
        [
            pl.col("summary")
            .map_elements(lambda x: call_ai_model({"text": x}, prompt))
            .alias("result")
        ]
    )
