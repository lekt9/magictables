
# MagicTables

MagicTables is a powerful Python library designed for data scientists, API scrapers, and developers working on data pipelines and ETL processes. It creates a shadow queryable database that automatically captures and stores your data, simplifying retrieval, caching, and analysis. With easy-to-use decorators, MagicTables streamlines API integration, web scraping, and data enrichment tasks.

The library offers advanced querying capabilities for your cached data, supports AI-augmented data processing, and includes automatic type generation and hinting for improved code safety and IDE support. MagicTables is your all-in-one solution for efficient data wrangling, transformation, and exploration.

## Features

- Seamlessly create a shadow database that automatically captures and versions function results
- Effortlessly cache API responses and function outputs in a local SQLite database
- Augment your data pipeline with AI-generated insights
- Perform complex queries and data mining on your captured data without modifying your original code
- Join and analyze data across multiple functions for advanced data exploration
- Streamline your data engineering workflow with easy-to-use decorators
- Support for various AI models through OpenRouter API for data enrichment
- Simplify ETL processes with intelligent post-transformations
- Construct complex SQL queries using a fluent interface, without writing raw SQL
- Automatic type generation and hinting for improved data quality and IDE support
- Enhance data lineage and governance with built-in data cataloging
- Optimize your data ops and MLOps workflows with efficient data storage and retrieval

## Installation

```bash
pip install magictables
```

## Quick Start

```python
import os
from typing import List, Dict, Any
from magictables import mtable, mgen, query_magic_db, QueryBuilder, execute_query
import requests
from dotenv import load_dotenv

load_dotenv()

@mtable()
def get_github_user(username: str):
    response = requests.get(f"https://api.github.com/users/{username}")
    return response.json()

@mgen(
    api_key=os.getenv("OPENAI_API_KEY"),
    model="meta-llama/llama-3-70b-instruct",
    base_url="https://openrouter.ai/api/v1/chat/completions",
    batch_size=5
)
def extend_user_data(data: List[Dict[str, Any]]):
    return data

# Usage with @mtable
user_data = get_github_user("octocat")
print(user_data)
# IDE will provide type hints for user_data based on the generated type

# Usage with @mgen
data = [
    {'username': 'octocat', 'company': 'GitHub'},
    {'username': 'torvalds', 'company': 'Linux Foundation'},
    {'username': 'gvanrossum', 'company': 'Microsoft'}
]
extended_data = extend_user_data(data)
print(extended_data)
# IDE will provide type hints for extended_data based on the generated type

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
# IDE will provide type hints for top_users based on the generated type
```

## How It Works

MagicTables creates a shadow queryable database that automatically captures and stores the results of your function calls. This allows you to perform complex queries and analysis on your data without modifying your original code or data sources. Additionally, MagicTables generates type hints based on the structure of your data, providing improved code safety and IDE support.

### @mtable()

The `@mtable()` decorator automatically caches the results of API calls or any function that returns JSON-serializable data. It stores the data in a local SQLite database, allowing for quick retrieval on subsequent calls with the same arguments and enabling complex queries on the stored data. 

Type Generation:
- MagicTables analyzes the structure of the returned data and generates a corresponding type hint.
- The generated type is associated with the function name and can be used for static type checking and IDE autocompletion.

### @mgen()

The `@mgen()` decorator uses AI to augment function calls with additional data. It takes a list of dictionaries as input, processes each item using the AI model, extends the data with AI-generated fields, and caches the results for future use. This allows you to enrich your data with AI-generated insights seamlessly.

Type Generation:
- MagicTables analyzes the structure of the input data and the AI-generated fields to create a comprehensive type hint.
- The generated type includes both the original fields and the AI-generated fields, providing accurate type information for the extended data.

### Type Hinting and IDE Support

MagicTables automatically generates type hints based on the structure of your data. This provides several benefits:

1. Improved code safety: Static type checkers can catch potential type-related errors before runtime.
2. Better IDE support: Your IDE can provide accurate autocompletion and suggestions based on the generated types.
3. Self-documenting code: The generated types serve as documentation for the structure of your data.

Example of generated type:

```python
from typing import TypedDict, List

class GithubUser(TypedDict):
    username: str
    company: str
    followers: int
    public_repos: int

class ExtendedUserData(TypedDict):
    username: str
    company: str
    ai_generated_bio: str
    ai_estimated_contributions: int

# These types are automatically generated and associated with your functions
get_github_user: Callable[[str], GithubUser]
extend_user_data: Callable[[List[Dict[str, Any]]], List[ExtendedUserData]]
```

With these generated types, your IDE can provide accurate autocompletion and type checking for the results of `get_github_user` and `extend_user_data` functions.

### Query Capabilities

MagicTables provides powerful querying capabilities for your shadow database:

1. `query_magic_db()`: Execute custom SQL queries on your captured data.
2. `QueryBuilder`: Construct complex SQL queries using a fluent interface, without writing raw SQL.
3. `join_magic_tables()`: Easily join data from different function calls for advanced analysis.
4. `get_table_info()`: Retrieve schema information about all tables in your shadow database.

These features allow you to perform advanced data analysis, generate reports, and extract insights from your captured data without affecting your original data sources or application logic.

## Advanced Usage

### Customizing AI Model and Batch Size

You can specify a different AI model and batch size when using the `@mgen()` decorator:

```python
@mgen(
    api_key=os.environ["OPENAI_API_KEY"],
    model="anthropic/claude-2",
    batch_size=10
)
def custom_model_function(data: List[Dict[str, Any]]):
    # Your function implementation
    return data
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
4. **AI-Powered Data Enrichment**: Easily augment your data with AI-generated insights using the `@mgen()` decorator.
5. **Simplified ETL**: Use the shadow database as an intermediate step in your ETL processes, making data transformations and loading more efficient.
6. **Rapid Prototyping**: Quickly experiment with different data analysis approaches without modifying your core application logic.
7. **Reduced Data Transfer**: Minimize data transfer between your application and databases by querying the local shadow database.
8. **Type Safety**: Automatically generated type hints improve code safety and provide better IDE support.
9. **Data Lineage**: Keep track of data sources and transformations with built-in data cataloging.
10. **Flexible Integration**: Easily integrate with existing data pipelines and workflows.

MagicTables empowers data scientists and developers to work with their data more efficiently, enabling advanced analytics and AI-driven insights without the need for complex data pipeline setups or modifications to existing codebases.

## Best Practices

1. **Use Meaningful Function Names**: The function names are used to generate table names in the shadow database. Choose descriptive names to make querying easier.

2. **Batch Processing**: When working with large datasets, use the `batch_size` parameter in the `@mgen()` decorator to process data in smaller chunks, reducing memory usage.

3. **Error Handling**: Implement proper error handling in your decorated functions to ensure data integrity in the shadow database.

4. **Regular Maintenance**: Periodically clean up your shadow database to remove outdated or unnecessary data.

5. **Version Control**: Keep track of changes in your data structure by versioning your code and database schema.

6. **Security**: Ensure that sensitive data is properly encrypted or masked when stored in the shadow database.

7. **Monitoring**: Implement logging and monitoring to track the performance and usage of your MagicTables-enhanced functions.

## Contributing

Contributions to MagicTables are welcome! Whether it's bug reports, feature requests, or code contributions, we appreciate your input. Please feel free to submit a Pull Request or open an Issue on our GitHub repository.

Before contributing, please read our contribution guidelines (link to CONTRIBUTING.md) for more information on our development process and coding standards.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Thanks to all the contributors who have helped shape MagicTables. (me)
- Special thanks to the open-source community for providing the tools and libraries that make MagicTables possible. (also me, unless yall give me a helping hand)

Happy data wrangling with MagicTables!
