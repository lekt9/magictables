# MagicTables

MagicTables is a Python library that provides easy-to-use decorators for caching API responses and augmenting function calls with AI-generated data. It's designed to simplify data retrieval and manipulation tasks in your Python projects, with a focus on working with DataFrames.

## Features

- Cache API responses in a local SQLite database
- Augment function calls with AI-generated data
- Extend DataFrames with AI-generated columns
- Easy-to-use decorators for quick integration
- Supports various AI models through OpenRouter API
- Simplifies ETL processes with post-transformations
- Batch processing for efficient handling of large datasets

## Installation

```bash
pip install magictables
```

## Quick Start

```python
import os
import pandas as pd
from magictables import mtable, mchat
import requests
from dotenv import load_dotenv

load_dotenv()

@mtable()
def get_github_user(username: str):
    response = requests.get(f"https://api.github.com/users/{username}")
    return response.json()

@mchat(
    api_key=os.getenv("OPENAI_API_KEY"),
    model="meta-llama/llama-3-70b-instruct",
    base_url="https://openrouter.ai/api/v1/chat/completions",
    batch_size=5
)
def extend_user_data(df: pd.DataFrame):
    return df

# Usage with @mtable
user_data = get_github_user("octocat")
print(user_data)

# Usage with @mchat
data = {
    'username': ['octocat', 'torvalds', 'gvanrossum'],
    'company': ['GitHub', 'Linux Foundation', 'Microsoft']
}
df = pd.DataFrame(data)
extended_df = extend_user_data(df)
print(extended_df)
```

## How It Works

### @mtable()

The `@mtable()` decorator caches the results of API calls or any function that returns JSON-serializable data. It stores the data in a local SQLite database, allowing for quick retrieval on subsequent calls with the same arguments.

### @mchat()

The `@mchat()` decorator uses AI to augment function calls with additional data. It now supports working with DataFrames:

1. It takes a DataFrame as input.
2. Processes each row of the DataFrame using the AI model.
3. Extends the DataFrame with AI-generated columns.
4. Caches the results for future use.

The decorator handles batch processing to efficiently work with large datasets and minimize API calls.

## Configuration

To use the `@mchat()` decorator, you need to set up an API key for OpenRouter:

1. Sign up for an account at [OpenRouter](https://openrouter.ai/)
2. Obtain your API key
3. Set the API key as an environment variable:

```bash
export OPENAI_API_KEY=your_api_key_here
```

## Advanced Usage

### Customizing AI Model and Batch Size

You can specify a different AI model and batch size when using the `@mchat()` decorator:

```python
@mchat(
    api_key=os.environ["OPENAI_API_KEY"],
    model="anthropic/claude-2",
    batch_size=10
)
def custom_model_function(df: pd.DataFrame):
    # Your function implementation
    return df
```

### Handling Complex Data

Both decorators can handle complex nested data structures. The `@mtable()` decorator will automatically flatten and store nested JSON, while the `@mchat()` decorator can work with nested input in DataFrames and generate appropriate output.

### Simplifying ETL Processes

MagicTables simplifies the ETL (Extract, Transform, Load) process by allowing post-transformations after generating or querying data. This approach offers several advantages:

1. Separation of concerns: The initial data retrieval or generation (Extract) is separated from the subsequent transformations, making the code more modular and easier to maintain.
2. Flexibility in data manipulation: You can apply various transformations to the data after it has been retrieved or generated, allowing for dynamic adjustments based on specific needs or conditions.
3. Reduced API calls: By caching the initial data and performing transformations on the cached results, you can reduce the number of API calls or expensive computations, improving performance and reducing costs.
4. Iterative development: You can easily experiment with different transformations without having to re-fetch or regenerate the data each time, speeding up the development process.
5. Consistency: Post-transformations ensure that the data is always processed in a consistent manner, regardless of whether it comes from the cache or a fresh API call.

Example of post-transformation with DataFrame:

```python
@mchat(api_key=os.environ["OPENAI_API_KEY"])
def extend_user_data(df: pd.DataFrame):
    # AI extends the DataFrame with additional columns
    return df

# Usage with post-transformation
data = {
    'username': ['octocat', 'torvalds', 'gvanrossum'],
    'company': ['GitHub', 'Linux Foundation', 'Microsoft']
}
df = pd.DataFrame(data)
extended_df = extend_user_data(df)

# Post-transformation
transformed_df = extended_df.assign(
    full_name=extended_df['first_name'] + ' ' + extended_df['last_name'],
    is_famous=extended_df['followers'] > 10000
)
print(transformed_df)
```

In this example, the `extend_user_data` function extends the input DataFrame with AI-generated columns. The post-transformation step then processes this data to create new fields or modify existing ones, demonstrating the flexibility and simplicity of the ETL process with MagicTables.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.