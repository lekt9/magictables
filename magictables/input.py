import polars as pl
from functools import wraps
from .database import magic_db
from .utils import generate_call_id, call_ai_model, ensure_dataframe, generate_row_id

import PyPDF2
import requests
from typing import List, Union


def extract_text_from_pdf(pdf_path):
    with open(pdf_path, "rb") as file:
        reader = PyPDF2.PdfReader(file)
        text = ""
        for page in reader.pages:
            text += page.extract_text()
    return text


def extract_text_from_url(url):
    response = requests.get(f"https://r.jina.ai/{url}")
    response.raise_for_status()
    return response.text


def process_input(
    input_data: Union[str, List[str]], input_type: str
) -> Union[str, List[str]]:
    if isinstance(input_data, list):
        return [process_input(item, input_type) for item in input_data]

    if input_type == "pdf":
        return extract_text_from_pdf(input_data)
    elif input_type == "url":
        return extract_text_from_url(input_data)
    else:
        raise ValueError("Unsupported input type")


def magic_input(query: str = None, input_type: str = "pdf"):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            input_data = args[0] if args else kwargs.get("input_data")
            if not input_data:
                raise ValueError("Input data not provided")

            is_batch = isinstance(input_data, list)

            if not is_batch:
                input_data = [input_data]

            results = []
            for single_input in input_data:
                call_id = generate_call_id(func, single_input, **kwargs)
                table_name = f"magic_{func.__name__}"

                # Check cache first
                cached_result = magic_db.get_cached_result(table_name, call_id)
                if cached_result is not None and not cached_result.is_empty():
                    print(f"Cache hit for {func.__name__}")
                    results.append(cached_result)
                else:
                    print(f"Cache miss for {func.__name__}")

                    # Process input
                    processed_text = process_input(single_input, input_type)

                    # Call the wrapped function to get the desired output structure with example values
                    output_structure = func(processed_text)

                    # Generate content based on the output structure and example values
                    if isinstance(output_structure, dict):
                        result = generate_dict_content(
                            processed_text, output_structure, query
                        )
                    elif isinstance(output_structure, list):
                        result = generate_list_content(
                            processed_text, output_structure, query
                        )
                    elif isinstance(output_structure, pl.DataFrame):
                        result = generate_dataframe_content(
                            processed_text, output_structure, query
                        )
                    else:
                        raise ValueError("Unsupported output structure")

                    # Convert result to DataFrame if it's not already
                    df = ensure_dataframe(result)

                    # Generate row IDs
                    df = df.with_columns(
                        pl.struct(df.columns).map_elements(generate_row_id).alias("id")
                    )

                    # Add function parameters as columns
                    for arg_name, arg_value in kwargs.items():
                        if isinstance(arg_value, (int, float, str, bool)):
                            df = df.with_columns(pl.lit(arg_value).alias(arg_name))

                    # Cache the results
                    magic_db.cache_results(table_name, df, call_id)

                    results.append(df)

            if is_batch:
                return pl.concat(results)
            else:
                return results[0]

        wrapper.function_name = func.__name__
        wrapper.query = query
        wrapper.func_params = func.__annotations__
        return wrapper

    return decorator


def generate_dict_content(text, structure, query):
    example_str = ", ".join([f"{k}: {v}" for k, v in structure.items()])
    prompt = f"""
    Based on the following text, generate a dictionary with the same structure as this example: {example_str}.
    Use the example values as a guide for the format and type of information to extract.
    Query: {query}
    """
    return call_ai_model({"text": text}, prompt)


def generate_list_content(text, structure, query):
    example_str = ", ".join([f"{k}: {v}" for k, v in structure[0].items()])
    prompt = f"""
    Based on the following text, generate a list of dictionaries. Each dictionary should have the same structure as this example: {example_str}.
    Use the example values as a guide for the format and type of information to extract.
    Query: {query}
    """
    return call_ai_model({"text": text}, prompt)


def generate_dataframe_content(text, df_structure, query):
    example_str = ", ".join(
        [f"{col}: {df_structure[col][0]}" for col in df_structure.columns]
    )
    prompt = f"""
    Based on the following text, generate data for a DataFrame with columns {df_structure.columns}.
    Use this example row as a guide for the format and type of information to extract: {example_str}
    Query: {query}
    """
    data = call_ai_model({"text": text}, prompt)
    return pl.DataFrame(data)
