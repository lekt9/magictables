
# MagicTables

MagicTables is a Python library that provides easy-to-use decorators for caching API responses and augmenting function calls with AI-generated data. It's designed to simplify data retrieval and manipulation tasks in your Python projects.

## Features

- Cache API responses in a local SQLite database
- Augment function calls with AI-generated data
- Easy-to-use decorators for quick integration
- Supports various AI models through OpenRouter API
- Simplifies ETL processes with post-transformations

## Installation

```bash
pip install magictables
```

## Quick Start

```python
import os
from magictables import mtable, mchat
import requests

@mtable()
def get_github_user(username: str):
    response = requests.get(f"https://api.github.com/users/{username}")
    return response.json()

@mchat(api_key=os.environ["OPENROUTER_API_KEY"])
def generate_user_bio(name: str, location: str, company: str):
    return f"Bio for {name} from {location}, working at {company}"

# Usage
user_data = get_github_user("octocat")
print(user_data)

bio = generate_user_bio(name="John Doe", location="San Francisco", company="Tech Corp")
print(bio)
```

## How It Works

### @mtable()

The `@mtable()` decorator caches the results of API calls or any function that returns JSON-serializable data. It stores the data in a local SQLite database, allowing for quick retrieval on subsequent calls with the same arguments.

### @mchat()

The `@mchat()` decorator uses AI to augment function calls with additional data. It sends the function arguments to an AI model (via OpenRouter API) and merges the AI-generated data with the original function arguments before calling the function.

## Configuration

To use the `@mchat()` decorator, you need to set up an API key for OpenRouter:

1. Sign up for an account at [OpenRouter](https://openrouter.ai/)
2. Obtain your API key
3. Set the API key as an environment variable:

```bash
export OPENROUTER_API_KEY=your_api_key_here
```

## Advanced Usage

### Customizing AI Model

You can specify a different AI model when using the `@mchat()` decorator:

```python
@mchat(api_key=os.environ["OPENROUTER_API_KEY"], model="anthropic/claude-2")
def custom_model_function(arg1, arg2):
    # Your function implementation
```

### Handling Complex Data

Both decorators can handle complex nested data structures. The `@mtable()` decorator will automatically flatten and store nested JSON, while the `@mchat()` decorator can work with nested input and generate appropriate output.

### Simplifying ETL Processes

MagicTables simplifies the ETL (Extract, Transform, Load) process by allowing post-transformations after generating or querying data. This approach offers several advantages:

1. Separation of concerns: The initial data retrieval or generation (Extract) is separated from the subsequent transformations, making the code more modular and easier to maintain.

2. Flexibility in data manipulation: You can apply various transformations to the data after it has been retrieved or generated, allowing for dynamic adjustments based on specific needs or conditions.

3. Reduced API calls: By caching the initial data and performing transformations on the cached results, you can reduce the number of API calls or expensive computations, improving performance and reducing costs.

4. Iterative development: You can easily experiment with different transformations without having to re-fetch or regenerate the data each time, speeding up the development process.

5. Consistency: Post-transformations ensure that the data is always processed in a consistent manner, regardless of whether it comes from the cache or a fresh API call.

Example of post-transformation:

```python
@mtable()
def get_user_data(user_id: int):
    # Fetch user data from an API
    response = requests.get(f"https://api.example.com/users/{user_id}")
    return response.json()

# Usage with post-transformation
user_data = get_user_data(123)
transformed_data = {
    "full_name": f"{user_data['first_name']} {user_data['last_name']}",
    "email_domain": user_data['email'].split('@')[1],
    "is_adult": user_data['age'] >= 18
}
print(transformed_data)
```

In this example, the `get_user_data` function fetches and caches the raw user data. The post-transformation step then processes this data to create new fields or modify existing ones, demonstrating the flexibility and simplicity of the ETL process with MagicTables.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.