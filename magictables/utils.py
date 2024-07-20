import hashlib
import requests
import json
import logging
from typing import List, Dict, Any, Optional, Union

import os
import re
from dotenv import load_dotenv
import requests
from hashlib import md5
import polars as pl
import functools
import hashlib
import json
import logging
import importlib
from typing import Any, Callable, Optional, Type, Union, List, Dict, TypeVar, cast
from pandera import DataFrameModel
import polars as pl
from pandera.typing import DataFrame as PanderaDataFrame
from pandera.engines import polars_engine as pa
from magictables.database import magic_db

load_dotenv()

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_BASE_URL = os.environ.get(
    "OPENAI_BASE_URL", "https://openrouter.ai/api/v1/chat/completions"
)
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")


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

        # Call OpenAI/OpenRouter API
        response = requests.post(
            url=OPENAI_BASE_URL, headers=headers, data=json.dumps(data)
        )

        if response.status_code != 200:
            raise Exception(
                f"OpenAI API request failed with status code {response.status_code}: {response.text}"
            )

        response_json = response.json()
        response_content = response_json["choices"][0]["message"]["content"]
        if "```json" in response_content:
            # Extract JSON from the response
            json_start = response_content.find("```json") + 7
            json_end = response_content.rfind("```")
            json_str = response_content[json_start:json_end].strip()
        else:
            json_str = response_content

        result = json.loads(json_str)
        results.append(result)

    return results  # Move this line outside of the for loop


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


def load_schema_class(table_name: str) -> Optional[Type[DataFrameModel]]:
    try:
        module_name = "magictables.schemas"
        class_name = f"{table_name.capitalize()}Model"
        module = importlib.import_module(module_name)
        schema_class = getattr(module, class_name)
        return schema_class
    except (ImportError, AttributeError):
        logging.warning(f"Schema class {class_name} not found in module {module_name}.")
        return None
