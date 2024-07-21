# utils.py
import hashlib
import json
import os
from typing import Any, Dict
import requests
from dotenv import load_dotenv
import polars as pl

load_dotenv()

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_BASE_URL = os.environ.get(
    "OPENAI_BASE_URL", "https://api.openai.com/v1/chat/completions"
)
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-3.5-turbo")


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
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY is not set in the environment variables.")

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    messages = [
        {
            "role": "system",
            "content": "You are a helpful assistant for data analysis and processing.",
        },
        {
            "role": "user",
            "content": f"Input data: {json.dumps(input_data)}\n\nPrompt: {prompt}\n\nProvide a JSON response based on this input and prompt.",
        },
    ]

    data = {
        "model": OPENAI_MODEL,
        "messages": messages,
        "temperature": 0.7,
        "max_tokens": 500,
    }

    try:
        response = requests.post(OPENAI_BASE_URL, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()["choices"][0]["message"]["content"]
        return json.loads(result)
    except (requests.exceptions.RequestException, json.JSONDecodeError, KeyError) as e:
        raise Exception(f"Error calling AI model: {str(e)}")
