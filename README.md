
# MagicTables

MagicTables is a powerful Python library that creates a shadow queryable database for your captured data, simplifying data retrieval, caching, and analysis. It provides easy-to-use decorators for caching API responses and augmenting function calls with AI-generated data, while also offering advanced querying capabilities for your cached data.

## Features

- Create a shadow database that automatically captures and stores function results
- Cache API responses and function outputs in a local SQLite database
- Augment function calls with AI-generated data
- Perform complex queries on your captured data without modifying your original code
- Join data from different function calls for advanced analysis
- Easy-to-use decorators for quick integration
- Supports various AI models through OpenRouter API
- Simplifies ETL processes with post-transformations
- Query builder for constructing complex SQL queries without writing raw SQL

## Installation

```bash
pip install magictables
```

## Quick Start

```python
import os
import pandas as pd
from magictables import mtable, mchat, query_magic_db, QueryBuilder, execute_query
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

# Perform a complex query on the shadow database
query = (
    QueryBuilder()
    .select("username", "company", "followers")
    .from_table("magic_get_github_user")
    .where("followers > 1000")
    .order_by("followers DESC")
    .limit(5)
)

top_users = execute_query(query)
print("Top users:", top_users)
```

## How It Works

MagicTables creates a shadow queryable database that automatically captures and stores the results of your function calls. This allows you to perform complex queries and analysis on your data without modifying your original code or data sources.

### @mtable()

The `@mtable()` decorator automatically caches the results of API calls or any function that returns JSON-serializable data. It stores the data in a local SQLite database, allowing for quick retrieval on subsequent calls with the same arguments and enabling complex queries on the stored data.

### @mchat()

The `@mchat()` decorator uses AI to augment function calls with additional data. It takes a DataFrame as input, processes each row using the AI model, extends the DataFrame with AI-generated columns, and caches the results for future use. This allows you to enrich your data with AI-generated insights seamlessly.

### Query Capabilities

MagicTables provides powerful querying capabilities for your shadow database:

1. `query_magic_db()`: Execute custom SQL queries on your captured data.
2. `QueryBuilder`: Construct complex SQL queries using a fluent interface, without writing raw SQL.
3. `join_magic_tables()`: Easily join data from different function calls for advanced analysis.
4. `get_table_info()`: Retrieve schema information about all tables in your shadow database.

These features allow you to perform advanced data analysis, generate reports, and extract insights from your captured data without affecting your original data sources or application logic.

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

### Complex Querying

MagicTables allows you to perform complex queries on your shadow database:

```python
from magictables import query_magic_db, QueryBuilder, execute_query

# Execute a custom SQL query
custom_query = """
SELECT username, followers, company
FROM magic_get_github_user
WHERE followers > 1000
ORDER BY followers DESC
LIMIT 5
"""
top_users = query_magic_db(custom_query)
print("Top users:", top_users)

# Use QueryBuilder for a complex query
query = (
    QueryBuilder()
    .select("username", "company", "followers")
    .from_table("magic_get_github_user")
    .join("magic_extend_user_data", "magic_get_github_user.username = magic_extend_user_data.username")
    .where("followers > 1000")
    .order_by("followers DESC")
    .limit(5)
)

complex_result = execute_query(query)
print("Complex query result:", complex_result)
```

### Analyzing Data Across Multiple Functions

MagicTables makes it easy to analyze data across multiple function calls:

```python
from magictables import join_magic_tables

joined_data = join_magic_tables(
    "magic_get_github_user",
    "magic_extend_user_data",
    "username",
    ["magic_get_github_user.username", "magic_get_github_user.followers", "magic_extend_user_data.ai_generated_bio"]
)
print("Joined data:", joined_data)
```

## Why Use MagicTables?

1. **Seamless Data Capture**: Automatically create a queryable database from your function calls without changing your existing code.
2. **Improved Performance**: Cache expensive API calls and computations, reducing load on external services and speeding up your application.
3. **Advanced Analysis**: Perform complex queries and joins on your captured data, enabling deeper insights and analytics.
4. **AI-Powered Data Enrichment**: Easily augment your data with AI-generated insights using the `@mchat()` decorator.
5. **Simplified ETL**: Use the shadow database as an intermediate step in your ETL processes, making data transformations and loading more efficient.
6. **Rapid Prototyping**: Quickly experiment with different data analysis approaches without modifying your core application logic.
7. **Reduced Data Transfer**: Minimize data transfer between your application and databases by querying the local shadow database.

MagicTables empowers developers and data scientists to work with their data more efficiently, enabling advanced analytics and AI-driven insights without the need for complex data pipeline setups or modifications to existing codebases.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
