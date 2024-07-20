import re
import ast
import dateparser
import pandas as pd
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
from typing import Any, Callable, List, Dict
import polars as pl
import json
import logging
from litellm import completion

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

load_dotenv()

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_BASE_URL = os.environ.get(
    "OPENAI_BASE_URL", "https://openrouter.ai/api/v1/chat/completions"
)
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")


OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OLLAMA_API_KEY = os.getenv("OLLAMA_API_KEY")
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai")

os.environ["OR_SITE_URL"] = "https://magictables.ai"  # optional
os.environ["OR_APP_NAME"] = "MagicTables"  # optional


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


def ensure_dataframe(result):
    if isinstance(result, pl.DataFrame):
        return result
    elif isinstance(result, pd.DataFrame):
        return pl.from_pandas(result)
    elif isinstance(result, (list, dict)):
        flattened = flatten_nested_structure(result)
        return pl.DataFrame(flattened)
    else:
        raise ValueError(f"Unsupported result type: {type(result)}")


def call_ai_model(input_data: Dict[str, Any], prompt: str) -> Dict[str, Any]:
    api_key = None
    model = None

    if LLM_PROVIDER == "openai":
        api_key = OPENAI_API_KEY
        model = "openai/gpt-4o-mini"
    elif LLM_PROVIDER == "openrouter":
        api_key = OPENROUTER_API_KEY
        model = "openrouter/openai/gpt-4o-mini"
    elif LLM_PROVIDER == "ollama":
        api_key = OLLAMA_API_KEY
        model = "ollama/phi3:mini"
    else:
        raise ValueError(f"Unsupported LLM provider: {LLM_PROVIDER}")

    if not api_key:
        raise ValueError(
            f"API key for {LLM_PROVIDER} is not set in the environment variables."
        )

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    messages = [
        {
            "role": "system",
            "content": "You are a JSON generator. Generate JSON based on the given input data and prompt. Wrap it in a ```json code block, and NEVER send anything else",
        },
        {
            "role": "user",
            "content": f"Input data: {json.dumps(input_data)}\n\nPrompt: {prompt}\n\nGenerate a JSON response based on this input and prompt.",
        },
    ]

    try:
        response = completion(model=model, messages=messages)
        response_content = response.choices[0].message.content
        if "```json" in response_content:
            json_str = response_content.replace("```json", "```").split("```")[1]
        else:
            json_str = response_content

        result = json.loads(json_str)
        logging.debug(f"Parsed JSON result: {result}")
        return result
    except (KeyError, json.JSONDecodeError) as e:
        error_message = f"Failed to parse API response: {str(e)}"
        logging.error(error_message)
        raise Exception(error_message)
    except requests.exceptions.RequestException as e:
        error_message = f"API request failed: {str(e)}"
        logging.error(error_message)
        raise Exception(error_message)


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
    print("df", df)
    for col in df.columns:
        if df[col].dtype == pl.List:
            df = df.with_columns(pl.col(col).list.to_struct())
        if df[col].dtype == pl.Struct:
            df = df.with_columns(
                pl.col(col).struct.rename_fields(
                    [f"{col}_{f}" for f in df[col].struct.fields]
                )
            )
            df = df.unnest(col)
    return df


def generate_ai_mapping(
    df: pl.DataFrame, required_params: List[str]
) -> Dict[str, Union[str, List[str]]]:
    df_info = {
        "current_schema": {
            col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)
        },
        "query": f"Map columns to required parameters: {', '.join(required_params)}",
        "next_function_params": {param: "Unknown" for param in required_params},
    }

    prompt = """
    Given the current DataFrame schema, the query, and the parameters required by the next function in the chain,
    generate a mapping to transform the Polars DataFrame. 
    The mapping should be a dictionary where:
    - Keys are the required parameters
    - Values are either:
      a) A single Python expression using the current column names and Polars functions
      b) A list of potential column names, ordered by likelihood of being the correct match

    You can use any Polars functions or Python operations for expressions. For example:
    - "new_col": "pl.col('existing_col') * 2"
    - "combined": "pl.col('col1') + ' ' + pl.col('col2')"
    - "transformed": "pl.when(pl.col('col') > 10).then(pl.lit('High')).otherwise(pl.lit('Low'))"

    For lists of potential columns, simply provide the column names. For example:
    - "user_id": ["user_id", "id", "user_identifier"]

    Ensure that the mapping includes all required parameters for the next function.
    If a required parameter is not directly available, try to derive it from existing columns or provide a list of potential matches.

    Return the mapping as a Python dictionary.
    """

    ai_response = call_ai_model(df_info, prompt)

    if isinstance(ai_response, dict) and "mapping" in ai_response:
        mapping = ai_response["mapping"]
    else:
        mapping = {}

    logging.info(f"AI-generated mapping: {mapping}")
    return mapping


def transform_data_with_query(
    df: pl.DataFrame, query: str, func_name: str, output_schema: Dict[str, str]
) -> pl.DataFrame:
    df_info = {
        "schema": df.schema,
        "func_name": func_name,
        "output_schema": output_schema,
    }

    transformation_prompt = f"""
    Given the following DataFrame schema:
    {df.schema}

    And the following query:
    "{query}"

    Generate a single Polars expression to transform the DataFrame according to the query and the desired output schema:
    {output_schema}

    Respond with a single string containing a valid Polars expression.
    """

    transformation_result = call_ai_model([df_info], query=transformation_prompt)
    content = transformation_result[0]

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


def apply_mapping(
    df: pl.DataFrame, mapping: Dict[str, Union[str, List[str]]]
) -> pl.DataFrame:
    valid_mappings = {}
    for new_col, expr_or_cols in mapping.items():
        if isinstance(expr_or_cols, str):
            try:
                valid_mappings[new_col] = eval(expr_or_cols)
            except Exception as e:
                logging.warning(
                    f"Skipping invalid mapping for '{new_col}': {expr_or_cols}. Error: {str(e)}"
                )
        elif isinstance(expr_or_cols, list):
            for col in expr_or_cols:
                if col in df.columns:
                    valid_mappings[new_col] = pl.col(col)
                    break
            else:
                logging.warning(
                    f"No valid column found for '{new_col}' in {expr_or_cols}"
                )

    if not valid_mappings:
        return df

    try:
        return df.with_columns(
            [expr.alias(new_col) for new_col, expr in valid_mappings.items()]
        )
    except Exception as e:
        logging.error(f"Error applying mappings: {str(e)}")
        return df
