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
- Chain operations for complex data transformations
- Convert results to various formats, including JSON and dictionaries

## Installation

```bash
pip install magictables
```

## Quick Start

```python
import os
from typing import List, Dict, Any
from magictables import mtable, mai
import requests
from dotenv import load_dotenv

load_dotenv()

@mtable()
def get_github_user(username: str):
    response = requests.get(f"https://api.github.com/users/{username}")
    return response.json()

@mai(
    api_key=os.getenv("OPENAI_API_KEY"),
    model="meta-llama/llama-3-70b-instruct",
    base_url="https://openrouter.ai/api/v1/chat/completions",
    batch_size=5,
    mode="augment"
)
def extend_user_data(username: str, company: str):
    return {"username": username, "company": company}

# Usage with @mtable
user_data = get_github_user("octocat")
print(user_data)

# Usage with @mai
extended_data = extend_user_data(username="octocat", company="GitHub")
print(extended_data)

# Chaining operations
result = (
    get_github_user("octocat")
    .join(extend_user_data(username="octocat", company="GitHub"), on="username")
    .apply(lambda df: df[df['followers'] > 1000])
    .to_dataframe()
)
print(result)

# Convert to JSON
json_result = (
    get_github_user("octocat")
    .to_dict(orient="records")
)
print(json_result)
```

# Be SURE to always use load_dotenv() BEFORE importing magictables if you want to use ai documented dataframe descriptions!

## How It Works

MagicTables creates a shadow queryable database that automatically captures and stores the results of your function calls. This allows you to perform complex queries and analysis on your data without modifying your original code or data sources.

### @mtable()

The `@mtable()` decorator automatically caches the results of API calls or any function that returns JSON-serializable data. It stores the data in a local SQLite database, allowing for quick retrieval on subsequent calls with the same arguments and enabling complex queries on the stored data.

### @mai()

The `@mai()` decorator uses AI to augment function calls with additional data. It can work in two modes:

1. "generate" mode: Creates new data based on the input.
2. "augment" mode: Extends existing data with AI-generated fields.

This allows you to enrich your data with AI-generated insights seamlessly.

### Chaining Operations

MagicTables supports chaining operations for complex data transformations:

```python
result = (
    get_github_user("octocat")
    .join(extend_user_data(username="octocat", company="GitHub"), on="username")
    .apply(lambda df: df[df['followers'] > 1000])
    .to_dataframe()
)
```

### Data Conversion

You can easily convert your results to various formats:

```python
# Convert to DataFrame
df_result = get_github_user("octocat").to_dataframe()

# Convert to dictionary
dict_result = get_github_user("octocat").to_dict(orient="records")

# Convert to JSON string
json_result = get_github_user("octocat").to_json(orient="records")
```

## Advanced Usage

### Customizing AI Model and Batch Size

You can specify a different AI model, batch size, and mode when using the `@mai()` decorator:

```python
@mai(
    api_key=os.environ["OPENAI_API_KEY"],
    model="anthropic/claude-2",
    batch_size=10,
    mode="generate"
)
def generate_user_bio(username: str, company: str):
    return {"username": username, "company": company}
```

### Complex Data Transformations

MagicTables allows you to perform complex data transformations using chaining:

```python
result = (
    get_github_user("octocat")
    .join(extend_user_data(username="octocat", company="GitHub"), on="username")
    .apply(lambda df: df[df['followers'] > 1000])
    .apply(lambda df: df.assign(follower_ratio=df['followers'] / df['following']))
    .to_dataframe()
)
print(result)
```

### Working with Multiple Data Sources

You can easily combine data from multiple sources:

```python
@mtable()
def get_github_repos(username: str):
    response = requests.get(f"https://api.github.com/users/{username}/repos")
    return response.json()

combined_data = (
    get_github_user("octocat")
    .join(get_github_repos("octocat"), on="username")
    .join(extend_user_data(username="octocat", company="GitHub"), on="username")
    .to_dataframe()
)
print(combined_data)
```

## Why Use MagicTables?

1. **Seamless Data Capture**: Automatically create a queryable database from your function calls without changing your existing code.
2. **Improved Performance**: Cache expensive API calls and computations, reducing load on external services and speeding up your application.
3. **Advanced Analysis**: Perform complex queries and joins on your captured data, enabling deeper insights and analytics.
4. **AI-Powered Data Enrichment**: Easily augment your data with AI-generated insights using the `@mai()` decorator.
5. **Simplified ETL**: Use the shadow database as an intermediate step in your ETL processes, making data transformations and loading more efficient.
6. **Rapid Prototyping**: Quickly experiment with different data analysis approaches without modifying your core application logic.
7. **Reduced Data Transfer**: Minimize data transfer between your application and databases by querying the local shadow database.
8. **Type Safety**: Automatically generated type hints improve code safety and provide better IDE support.
9. **Data Lineage**: Keep track of data sources and transformations with built-in data cataloging.
10. **Flexible Integration**: Easily integrate with existing data pipelines and workflows.

MagicTables empowers data scientists and developers to work with their data more efficiently, enabling advanced analytics and AI-driven insights without the need for complex data pipeline setups or modifications to existing codebases.

## Best Practices

1. **Use Meaningful Function Names**: The function names are used to generate table names in the shadow database. Choose descriptive names to make querying easier.
2. **Batch Processing**: When working with large datasets, use the `batch_size` parameter in the `@mai()` decorator to process data in smaller chunks, reducing memory usage.
3. **Error Handling**: Implement proper error handling in your decorated functions to ensure data integrity in the shadow database.
Certainly! I'll continue with the rest of the README, including more best practices, contributing guidelines, and additional information:

```markdown
4. **Regular Maintenance**: Periodically clean up your shadow database to remove outdated or unnecessary data.
5. **Version Control**: Keep track of changes in your data structure by versioning your code and database schema.
6. **Security**: Ensure that sensitive data is properly encrypted or masked when stored in the shadow database.
7. **Monitoring**: Implement logging and monitoring to track the performance and usage of your MagicTables-enhanced functions.
8. **Optimize Chaining**: When chaining operations, try to perform filtering and data reduction steps early in the chain to improve performance.
9. **Use Type Hints**: Leverage the automatically generated type hints to improve code safety and IDE support.

## Advanced Features

### Custom SQL Queries

While MagicTables provides high-level abstractions, you can still execute custom SQL queries when needed:

```python
from magictables import execute_raw_sql

custom_query = """
SELECT username, followers, company
FROM magic_get_github_user
WHERE followers > 1000
ORDER BY followers DESC
LIMIT 5
"""
result = execute_raw_sql(custom_query)
print(result)
```

### Data Versioning

MagicTables automatically versions your data. You can access specific versions of your data:

```python
historical_data = get_github_user("octocat").version("2023-06-01").to_dataframe()
print(historical_data)
```

### Data Lineage

Track the origin and transformations of your data:

```python
lineage = get_github_user("octocat").get_lineage()
print(lineage)
```

### Custom Caching Strategies

You can customize the caching behavior for each decorated function:

```python
@mtable(cache_strategy="time_based", max_age=3600)  # Cache for 1 hour
def get_github_user_time_based(username: str):
    response = requests.get(f"https://api.github.com/users/{username}")
    return response.json()
```

## Performance Considerations

1. **Indexing**: MagicTables automatically creates indexes on frequently queried columns. You can also manually specify indexes:

```python
@mtable(indexes=["username", "followers"])
def get_github_user(username: str):
    # ...
```

2. **Batch Processing**: When working with large datasets, use batch processing to reduce memory usage:

```python
@mai(batch_size=100)
def process_large_dataset(data: List[Dict[str, Any]]):
    # ...
```

3. **Caching Strategy**: Choose appropriate caching strategies based on your data's update frequency and query patterns.

## Integration with Other Tools

MagicTables can be easily integrated with other popular data science and analytics tools:

### Pandas Integration

```python
import pandas as pd

df = pd.DataFrame(get_github_user("octocat").to_dict(orient="records"))
```

### Visualization with Matplotlib

```python
import matplotlib.pyplot as plt

data = get_github_user("octocat").to_dataframe()
plt.bar(data['username'], data['followers'])
plt.show()
```

### Export to Various Formats

```python
# Export to CSV
get_github_user("octocat").to_csv("github_user_data.csv")

# Export to Excel
get_github_user("octocat").to_excel("github_user_data.xlsx")

# Export to Parquet
get_github_user("octocat").to_parquet("github_user_data.parquet")
```

## Error Handling and Debugging

MagicTables provides detailed error messages and debugging information:

```python
from magictables import set_debug_mode

set_debug_mode(True)

# Now all operations will provide detailed debug information
```

You can also catch and handle specific MagicTables exceptions:

```python
from magictables import MagicTablesError

try:
    result = get_github_user("nonexistent_user")
except MagicTablesError as e:
    print(f"An error occurred: {e}")
```

## Contributing

Contributions to MagicTables are welcome! Whether it's bug reports, feature requests, or code contributions, we appreciate your input. Please follow these steps to contribute:

1. Fork the repository on GitHub.
2. Create a new branch for your feature or bug fix.
3. Write your code and tests.
4. Ensure all tests pass and the code follows our style guidelines.
5. Submit a pull request with a clear description of your changes.

Before contributing, please read our [contribution guidelines](CONTRIBUTING.md) for more information on our development process and coding standards.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Thanks to all the contributors who have helped shape MagicTables. (me)
- Special thanks to the open-source community for providing the tools and libraries that make MagicTables possible. (still just me hehe)


Happy data wrangling with MagicTables!