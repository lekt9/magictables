import re
import ast
import dateparser
import polars as pl
from typing import Dict, Any, Union
import ast
import hashlib
import requests
import json
import logging

import os
import re
from dotenv import load_dotenv
import requests
from hashlib import md5
import polars as pl
import hashlib
import json
import logging
from typing import Any, Callable, Optional, List, Dict
import polars as pl
import json
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

load_dotenv()

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_BASE_URL = os.environ.get(
    "OPENAI_BASE_URL", "https://openrouter.ai/api/v1/chat/completions"
)
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")


def generate_schema(data: Union[pl.DataFrame, Dict[str, Any]]) -> Dict[str, str]:
    """
    Generate a schema representation of a Polars DataFrame or a dictionary.

    Args:
    data (Union[pl.DataFrame, Dict[str, Any]]): The input DataFrame or dictionary.

    Returns:
    Dict[str, str]: A dictionary where keys are column names and values are data types.
    """
    if isinstance(data, pl.DataFrame):
        schema = {}
        for col in data.columns:
            dtype = data[col].dtype
            if isinstance(dtype, pl.List):
                schema[col] = f"List[{dtype.inner}]"
            elif isinstance(dtype, pl.Struct):
                schema[col] = (
                    f"Struct{{{', '.join(f'{k}: {v}' for k, v in dtype.fields.items())}}}"
                )
            else:
                schema[col] = str(dtype)
        return schema
    elif isinstance(data, dict):
        return {key: type(value).__name__ for key, value in data.items()}
    else:
        raise ValueError("Input must be a Polars DataFrame or a dictionary")


def ensure_dataframe(result: Any) -> pl.DataFrame:
    if isinstance(result, pl.DataFrame):
        return result
    elif isinstance(result, dict):
        return pl.DataFrame([result])
    elif isinstance(result, list):
        if all(isinstance(item, dict) for item in result):
            return pl.DataFrame(result)
        else:
            try:
                return pl.DataFrame(result)
            except ValueError:
                raise ValueError(
                    "List items are not consistent for DataFrame conversion."
                )
    elif isinstance(result, str):
        try:
            json_result = json.loads(result)
            return ensure_dataframe(json_result)
        except json.JSONDecodeError:
            raise ValueError("String input is not valid JSON.")
    else:
        return pl.DataFrame({"result": [result]})


def call_ai_model(
    new_items: List[Dict[str, Any]], query: Optional[str] = None
) -> List[Dict[str, Any]]:
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
    }

    results = []
    for item in new_items:
        prompt = (
            f"You are the all knowing JSON generator. Given the function arguments, "
            f"Create a JSON object that populates the missing, or incomplete columns for the function call."
            f"The current keys are: {json.dumps(item)}\n, you MUST only use these keys in the JSON you respond."
            f"Respond with it wrapped in ```json code block with a flat unnested JSON"
        )
        if query:
            prompt += f"\n\nQuery: {query}"

        data = {
            "model": OPENAI_MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            "response_format": {"type": "json_object"},
        }

        logging.debug(f"Sending request to AI model with data: {data}")

        # Call OpenAI/OpenRouter API
        response = requests.post(
            url=OPENAI_BASE_URL, headers=headers, data=json.dumps(data)
        )

        if response.status_code != 200:
            error_message = f"OpenAI API request failed with status code {response.status_code}: {response.text}"
            logging.error(error_message)
            raise Exception(error_message)

        response_json = response.json()
        logging.debug(f"Raw AI model response: {response_json}")

        response_content = response_json["choices"][0]["message"]["content"]
        logging.debug(f"AI model response content: {response_content}")

        if "```json" in response_content:
            # Extract JSON from the response
            json_start = response_content.find("```json") + 7
            json_end = response_content.rfind("```")
            json_str = response_content[json_start:json_end].strip()
        else:
            json_str = response_content

        logging.debug(f"Extracted JSON string: {json_str}")

        try:
            result = json.loads(json_str)
            logging.debug(f"Parsed JSON result: {result}")
            results.append(result)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON response: {json_str}")
            logging.error(f"JSON decode error: {str(e)}")
            results.append(json_str)  # Append the raw string if parsing fails

    logging.debug(f"Final results from call_ai_model: {results}")
    return results


def create_key(func_name, args, kwargs):
    return md5(f"{func_name}:{str(args)}:{str(kwargs)}".encode()).hexdigest()


def parse_ai_response(func_name, kwargs, api_key, base_url, model):
    headers = {"Authorization": f"Bearer {api_key}"}
    prompt = (
        f"You are the all knowing JSON generator. Given the function arguments, "
        f"Create a JSON object that populates the missing, or incomplete columns for the function call."
        f"The function is: {func_name}\n"
        f"The current keys are: {json.dumps(kwargs)}\n, you MUST only use these keys in the JSON you respond."
        f"Respond with it wrapped in ```json code block with a flat unnested JSON"
    )
    data = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_object"},
    }

    response = requests.post(url=base_url, headers=headers, json=data)
    response.raise_for_status()

    response_json = response.json()
    response_data = re.search(
        r"```(?:json)?\s*([\s\S]*?)\s*```",
        response_json["choices"][0]["message"]["content"],
    )
    if response_data:
        return json.loads(response_data.group(1).strip())
    else:
        raise ValueError("No JSON content found in the response")


def parse_ai_response_batch(func_name, rows, api_key, base_url, model):
    headers = {"Authorization": f"Bearer {api_key}"}
    prompt = (
        f"You are the all knowing JSON generator. Given the function arguments, "
        f"Create a JSON object that populates the missing, or incomplete columns for each row in the function call."
        f"The function is: {func_name}\n"
        f"The current rows are: {json.dumps(rows)}\n"
        f"Respond with a JSON array of objects, where each object corresponds to a row in the input."
        f"Each object should only use the keys present in the input rows."
        f"Wrap the response in a ```json code block."
    )
    data = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_object"},
    }

    response = requests.post(url=base_url, headers=headers, json=data)
    response.raise_for_status()

    response_json = response.json()
    response_data = re.search(
        r"```(?:json)?\s*([\s\S]*?)\s*```",
        response_json["choices"][0]["message"]["content"],
    )
    if response_data:
        return json.loads(response_data.group(1).strip())
    else:
        raise ValueError("No JSON content found in the response")


def generate_ai_descriptions(table_name: str, columns: List[str]) -> Dict[str, Any]:
    """
    Generate AI descriptions for a table and its columns using OpenRouter API.
    If API key is not available or there's an error, return default descriptions.

    Args:
    table_name (str): The name of the table.
    columns (List[str]): List of column names in the table.

    Returns:
    Dict[str, Any]: A dictionary containing the table description and column descriptions.
    """
    if not OPENAI_API_KEY:
        logging.warning("OpenAI API key not found. Using default descriptions.")
        return generate_default_descriptions(table_name, columns)

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        # Generate table description
        table_description = get_ai_description(
            f"Describe the purpose and content of a database table named '{table_name}' in one sentence.",
            headers,
        )

        # Generate column descriptions
        column_descriptions = {}
        for column in columns:
            column_description = get_ai_description(
                f"Describe the purpose and content of a database column named '{column}' in the table '{table_name}' in one sentence.",
                headers,
            )
            column_descriptions[column] = column_description

        return {
            "table_description": table_description,
            "column_descriptions": column_descriptions,
        }

    except Exception as e:
        logging.error(f"Error generating AI descriptions: {str(e)}")
        return generate_default_descriptions(table_name, columns)


def get_ai_description(prompt: str, headers: Dict[str, str]) -> str:
    """Helper function to get AI description from OpenRouter API."""
    response = requests.post(
        OPENAI_BASE_URL,
        headers=headers,
        json={
            "model": OPENAI_MODEL,
            "messages": [
                {
                    "role": "system",
                    "content": "You are a helpful assistant that describes database tables and columns.",
                },
                {"role": "user", "content": prompt},
            ],
        },
    )
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"].strip()


def generate_default_descriptions(
    table_name: str, columns: List[str]
) -> Dict[str, Any]:
    """Generate default descriptions when AI generation is not available."""
    return {
        "table_description": f"Table containing data related to {table_name}.",
        "column_descriptions": {
            column: f"Column in {table_name} table." for column in columns
        },
    }


def generate_call_id(func: Callable, *args: Any, **kwargs: Any) -> str:
    def default_serializer(obj):
        if isinstance(obj, pl.DataFrame):
            return obj.to_dict(as_series=False)
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


def generate_row_id(row: Dict[str, Any]) -> str:
    row_data = json.dumps(row, sort_keys=True)
    return hashlib.md5(row_data.encode()).hexdigest()


def flatten_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    for col in df.columns:
        if df[col].dtype == pl.List:
            df = df.with_columns(pl.col(col).list.to_struct())
        if df[col].dtype == pl.Struct:
            df = df.with_columns(
                pl.col(col).struct.rename_fields(lambda f: f"{col}_{f}")
            )
            df = df.unnest(col)
    return df


def transform_data_with_query(
    df: pl.DataFrame, query: str, func_name: str, output_schema: Dict[str, str]
) -> pl.DataFrame:
    transformation_prompt = f"""
    Given the following DataFrame schema:
    {df.schema}

    And the following query:
    "{query}"

    Generate a single Polars expression to transform the DataFrame according to the query and the desired output schema:
    {output_schema}

    Respond with a single string containing a valid Polars expression.
    """

    transformation_result = call_ai_model(
        [{"role": "user", "content": transformation_prompt}]
    )
    content = transformation_result[0].get("content", {})
    if isinstance(content, dict):
        transformation = content.get("content", "")
    else:
        transformation = content if isinstance(content, str) else ""

    try:
        df = eval(transformation)
    except Exception as e:
        print(f"Error applying transformation '{transformation}': {str(e)}")

    # Ensure the output matches the specified schema
    for col, dtype in output_schema.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col).cast(getattr(pl, dtype)))
        else:
            df = df.with_columns(pl.col(col).cast(getattr(pl, dtype)))

    df = df.select(output_schema.keys())
    return df


def apply_stored_mapping(df: pl.DataFrame, mapping: Dict[str, Any]) -> pl.DataFrame:
    if "regex_patterns" in mapping:
        df = apply_regex_patterns(df, mapping["regex_patterns"])

    for transformation in mapping["transformations"]:
        df = apply_transformation(df, transformation)

    df = enforce_schema(df, mapping["output_schema"])
    return df


def apply_regex_patterns(df: pl.DataFrame, patterns: Dict[str, str]) -> pl.DataFrame:
    for col in df.columns:
        if df[col].dtype == pl.String:
            for pattern_name, pattern in patterns.items():
                new_col_name = f"{col}_{pattern_name}"
                df = df.with_columns(
                    pl.col(col).str.extract(pattern, 1).alias(new_col_name)
                )
    return df


def enforce_schema(df: pl.DataFrame, schema: Dict[str, str]) -> pl.DataFrame:
    for col, dtype in schema.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col).cast(getattr(pl, dtype)))
        else:
            df = df.with_columns(pl.col(col).cast(getattr(pl, dtype)))

    df = df.select(schema.keys())
    return df


def apply_transformation(df: pl.DataFrame, transformation: str) -> pl.DataFrame:
    print(f"Applying transformation: {transformation}")

    def safe_eval(expr: str, variables: Dict[str, Any]) -> Any:
        try:
            return eval(expr, {"__builtins__": {}}, variables)
        except Exception as e:
            print(f"Error evaluating expression '{expr}': {str(e)}")
            return None

    def fuzzy_to_iso8601(value):
        if pl.Series([value]).is_null().item():
            return None
        try:
            parsed_date = dateparser.parse(str(value))
            return parsed_date.isoformat() if parsed_date else None
        except Exception:
            return None

    if match := re.match(r"Convert '(.+)' to ISO 8601 format", transformation):
        col = match.group(1)
        df = df.with_columns(pl.col(col).map_elements(fuzzy_to_iso8601).alias(col))
    elif match := re.match(
        r"Convert '(.+)' and '(.+)' to ISO 8601 format", transformation
    ):
        col1, col2 = match.groups()
        df = df.with_columns(
            [
                pl.col(col1).map_elements(fuzzy_to_iso8601).alias(col1),
                pl.col(col2).map_elements(fuzzy_to_iso8601).alias(col2),
            ]
        )
    elif match := re.match(r"Convert '(.+)' to (.+)", transformation):
        col, dtype = match.groups()
        if dtype == "ISO 8601 format":
            df = df.with_columns(pl.col(col).map_elements(fuzzy_to_iso8601).alias(col))
        else:
            df = df.with_columns(pl.col(col).cast(getattr(pl, dtype)))
    elif match := re.match(r"Rename '(.+)' to '(.+)'", transformation):
        old_name, new_name = match.groups()
        df = df.rename({old_name: new_name})
    elif match := re.match(r"Calculate '(.+)' as (.+)", transformation):
        new_col, expr = match.groups()
        df = df.with_columns(pl.eval(expr).alias(new_col))
    elif match := re.match(r"Filter rows where (.+)", transformation):
        condition = match.group(1)
        df = df.filter(pl.eval(condition))
    elif match := re.match(r"Apply (.+) to column '(.+)'", transformation):
        func, col = match.groups()
        df = df.with_columns(
            pl.col(col).map_elements(lambda x: safe_eval(func, {"x": x})).alias(col)
        )
    elif match := re.match(r"Aggregate (.+) by (.+)", transformation):
        agg_expr, group_by = match.groups()
        agg_dict = ast.literal_eval(agg_expr)
        df = df.group_by(group_by).agg(
            [getattr(pl, agg_func)(pl.col(col)) for col, agg_func in agg_dict.items()]
        )
    elif transformation.startswith("Custom:"):
        code = transformation[7:]
        local_vars = {"df": df}
        exec(code, {"pl": pl}, local_vars)
        df = local_vars["df"]
    else:
        logger.warning(f"Unknown transformation: {transformation}")
        logger.debug(f"Current DataFrame: {df}")
    return df
