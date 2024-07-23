# utils.py
import json
import os
import re
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

    if isinstance(nested_structure, dict) and "results" in nested_structure:
        top_level_info = {k: v for k, v in nested_structure.items() if k != "results"}
        for item in nested_structure["results"]:
            row = top_level_info.copy()
            row.update(item)
            flattened_rows.append(row)
    else:
        # Fallback for other structures, though this case shouldn't occur with your data
        flattened_rows.append(nested_structure)

    return flattened_rows


async def call_ai_model(
    input_data: List[Dict[str, Any]], prompt: str, model: str = None
) -> Dict[str, Any]:
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
        # print("input", messages)
        response = await acompletion(model=model_to_use, messages=messages)
        response_content = response.choices[0].message.content
        if "```json" in response_content:
            json_str = response_content.replace("```json", "```").split("```")[1]
        else:
            json_str = response_content
        # print("str", json_str)

        # Use json.loads() with a custom parser to handle newlines
        result = json.loads(json_str, parse_constant=lambda x: x.strip())
        # print("output", result)
        logging.debug(f"Parsed JSON result: {result}")
        return result
    except (KeyError, json.JSONDecodeError) as e:
        # If JSON parsing fails, try to extract the pandas_code directly
        try:
            pandas_code_match = re.search(
                r'"pandas_code":\s*"(.*?)"(?=\s*})', json_str, re.DOTALL
            )
            if pandas_code_match:
                pandas_code = pandas_code_match.group(1)
                # Unescape newlines and quotes
                pandas_code = pandas_code.replace("\\n", "\n").replace('\\"', '"')
                return {"pandas_code": pandas_code}
            else:
                raise ValueError("Could not extract pandas_code")
        except Exception as inner_e:
            error_message = f"Failed to parse API response: {str(e)}. Additional error: {str(inner_e)}"
            logging.error(error_message)
            raise Exception(error_message)
    except aiohttp.ClientError as e:
        error_message = f"API request failed: {str(e)}"
        logging.error(error_message)
        raise Exception(error_message)
