import hashlib
import json
import logging
import os
from typing import Any, Dict

from dotenv import load_dotenv
from litellm import completion
import polars as pl
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


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


def generate_call_id(source_name: str, kwargs: dict) -> str:
    call_data = json.dumps({"source": source_name, "params": kwargs}, sort_keys=True)
    return hashlib.md5(call_data.encode()).hexdigest()


def to_dataframe(data, format="polars"):
    if isinstance(data, pl.DataFrame):
        if format.lower() == "polars":
            return data
        elif format.lower() == "pandas":
            return data.to_pandas()
    elif isinstance(data, list):
        if format.lower() == "polars":
            return pl.DataFrame(data)
        elif format.lower() == "pandas":
            return pl.DataFrame(data).to_pandas()
    else:
        raise ValueError("Unsupported data type for conversion to DataFrame")


def create_sqlalchemy_engine(connection_string):
    return create_engine(connection_string)


def create_sqlalchemy_session(engine):
    Session = sessionmaker(bind=engine)
    return Session()


def polars_to_sql(df: pl.DataFrame, table_name: str, engine, if_exists="replace"):
    pandas_df = df.to_pandas()
    pandas_df.to_sql(table_name, engine, if_exists=if_exists, index=False)


def sql_to_polars(query: str, engine) -> pl.DataFrame:
    with engine.connect() as connection:
        result = connection.execute(query)
        return pl.DataFrame(result.fetchall(), columns=result.keys())


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
