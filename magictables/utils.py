# utils.py
import json
import os
from typing import Any, Dict, List
import aiohttp
from dotenv import load_dotenv

import logging
from litellm import acompletion


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

    if isinstance(nested_structure, dict):
        # Separate top-level items and nested items
        top_level_items = {
            k: v for k, v in nested_structure.items() if not isinstance(v, (dict, list))
        }
        nested_items = {
            k: v for k, v in nested_structure.items() if isinstance(v, (dict, list))
        }

        if nested_items:
            for key, value in nested_items.items():
                if isinstance(value, list):
                    for item in value:
                        row = top_level_items.copy()
                        if isinstance(item, dict):
                            row.update(item)
                        else:
                            row[key] = item
                        flattened_rows.append(row)
                elif isinstance(value, dict):
                    row = top_level_items.copy()
                    row.update(value)
                    flattened_rows.append(row)
        else:
            flattened_rows.append(top_level_items)
    elif isinstance(nested_structure, list):
        for item in nested_structure:
            flattened_rows.extend(flatten_nested_structure(item))
    else:
        flattened_rows.append(nested_structure)

    return flattened_rows


async def call_ai_model(
    input_data: List[Dict[str, Any]],
    prompt: str,
    model: str = None,
    return_json=True,
) -> Dict[str, Any] | str:
    api_key = None
    default_model = None

    if LLM_PROVIDER == "openai":
        api_key = OPENAI_API_KEY
        default_model = "openai/gpt-4o-mini"
    elif LLM_PROVIDER == "openrouter":
        api_key = OPENROUTER_API_KEY
        default_model = "openrouter/openai/gpt-4o-mini"
    elif LLM_PROVIDER == "ollama":
        api_key = OLLAMA_API_KEY
        default_model = "ollama/phi3:mini"
    else:
        raise ValueError(f"Unsupported LLM provider: {LLM_PROVIDER}")

    # Use the provided model if it's not None, otherwise use the default model
    model_to_use = model or default_model

    if not api_key:
        raise ValueError(
            f"API key for {LLM_PROVIDER} is not set in the environment variables."
        )

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    if return_json:
        system_content = "You are a JSON generator. Generate JSON based on the given input data and prompt. Wrap it in a ```json code block, and NEVER send anything else"
    else:
        system_content = "You are an AI assistant. Respond to the given input data and prompt with natural language. Do not use JSON formatting."

    messages = [
        {
            "role": "system",
            "content": system_content,
        },
        {
            "role": "user",
            "content": f"Input data: {json.dumps(input_data[:10])[:20000]}\n\nPrompt: {prompt}\n\nGenerate a {'JSON' if return_json else ''} response based on this input and prompt.",
        },
    ]

    try:
        response = await acompletion(model=model_to_use, messages=messages)
        response_content = response.choices[0].message.content

        if return_json:
            if "```json" in response_content:
                json_str = response_content.replace("```json", "```").split("```")[1]
            else:
                json_str = response_content

            try:
                result = json.loads(json_str, parse_constant=lambda x: x.strip())
                logging.debug(f"Parsed JSON result: {result}")
                return result
            except json.JSONDecodeError as e:
                logging.warning(
                    f"Failed to parse JSON. Returning raw response. Error: {str(e)}"
                )
                return response_content
        else:
            return response_content.strip()

    except aiohttp.ClientError as e:
        error_message = f"API request failed: {str(e)}"
        logging.error(error_message)
        raise Exception(error_message)
