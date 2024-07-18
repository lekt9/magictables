import json
import re
import requests
from hashlib import md5


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
