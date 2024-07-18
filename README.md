# MagicTables

MagicTables is a Python library that provides easy-to-use decorators for caching API responses and augmenting function calls with AI-generated data. It's designed to simplify data retrieval and manipulation tasks in your Python projects.

## Features

- Cache API responses in a local SQLite database
- Augment function calls with AI-generated data
- Easy-to-use decorators for quick integration
- Supports various AI models through OpenRouter API

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

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
```

This README provides a comprehensive overview of the MagicTables library, including its features, installation instructions, quick start guide, how it works, configuration steps, advanced usage examples, and information about contributing and licensing. You can use this as a starting point and expand on it as needed for your open-source project.