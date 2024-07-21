# utils.py
import hashlib
import json
import os
from typing import Any, Dict
import requests
from dotenv import load_dotenv
import polars as pl

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


def generate_call_id(source_name: str, kwargs: dict) -> str:
    call_data = json.dumps({"source": source_name, "params": kwargs}, sort_keys=True)
    return hashlib.md5(call_data.encode()).hexdigest()


def to_dataframe(data):
    if isinstance(data, pl.DataFrame):
        return data
    elif isinstance(data, list):
        return pl.DataFrame(data)
    else:
        raise ValueError("Unsupported data type for conversion to DataFrame")


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
