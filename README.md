
# MagicTables

MagicTables is a Python library that simplifies ETL processes by leveraging graph databases under the hood, providing intelligent caching, easy-to-use interfaces for data retrieval and transformation, and automatic relationship identification between data sources.

## The Problem

Data scientists spend too much time on repetitive ETL tasks, taking focus away from actual analysis.

## The Solution

MagicTables addresses this by:

1. Using a graph database under the hood for efficient data storage and relationship mapping
2. Automatically caching retrieved data for easy reuse
3. Providing a simple, Polars DataFrame-like interface for querying and transforming data
4. Intelligently identifying relationships between different data sources
5. Offering AI-driven data parsing and analysis capabilities

## Key Features

- Graph database-powered caching system with automatic relationship mapping
- Simple API for declarative data source definitions and transformations
- Polars DataFrame-like interface for high-performance data manipulation
- Support for various data sources (APIs, databases, files, websites, PDFs)
- Intelligent web scraping with automatic selector generation
- AI-assisted data joining and analysis leveraging graph relationships
- Native integration with Polars and easy conversion to Pandas

## Installation

```bash
pip install magictables
```

## Quick Start

Here's an example demonstrating how MagicTables simplifies data retrieval, transformation, and analysis:

```python
from magictables import Source, Chain
import polars as pl

# Define data sources
github_api = Source.api("GitHub API")
github_api.add_route(
    "users", 
    "https://api.github.com/users/{username}", 
    "Get user data"
)
github_api.add_route(
    "repos", 
    "https://api.github.com/users/{username}/repos", 
    "Get user repositories"
)

stackoverflow_api = Source.api("Stack Overflow API")
stackoverflow_api.add_route(
    "users",
    "https://api.stackexchange.com/2.3/users/{user_ids}",
    "Get Stack Overflow user data"
)

# Create a Chain (which uses the graph database under the hood)
dev_profile = Chain()
dev_profile.add(github_api.route("users"))
dev_profile.add(github_api.route("repos"))
dev_profile.add(stackoverflow_api.route("users"))

# Define an analysis query
dev_profile.analyze("Summarize the developer's profile including GitHub and Stack Overflow data")

# Fetch and process data (automatically cached in graph database)
result = dev_profile.execute(username="octocat", user_ids="1234567")

# Work with the result as a Polars DataFrame
print(result.head())
print(result.columns)

# Perform transformations using Polars expressions
transformed = result.with_columns(pl.sum("repo_stars").alias("total_stars"))
filtered = transformed.filter(pl.col("total_stars") > 1000)

# The result is a Polars DataFrame
polars_df = filtered

# Convert to Pandas if needed
pandas_df = filtered.to_pandas()

# Subsequent calls will use cached data and identified relationships from the graph database
cached_result = dev_profile.execute(username="octocat", user_ids="1234567")

# Leverage graph relationships for complex queries
related_repos = dev_profile.get_related("octocat", relationship="OWNS_REPO")
print("Repositories owned by octocat:")
print(related_repos)

# Add a new data source for GitHub issues
github_api.add_route(
    "issues",
    "https://api.github.com/repos/{username}/{repo}/issues",
    "Get repository issues"
)

# Add the new source to the chain
dev_profile.add(github_api.route("issues"))

# Execute the chain with the new source
result_with_issues = dev_profile.execute(username="octocat", repo="Hello-World", user_ids="1234567")

# Analyze the data using AI capabilities
dev_profile.analyze("Identify the most active contributors based on GitHub and Stack Overflow data")
active_contributors = dev_profile.get_analysis_result()
print("Most active contributors:")
print(active_contributors)

# Perform more complex transformations
repo_summary = result_with_issues.groupby("repo_name").agg([
    pl.count("issue_id").alias("total_issues"),
    pl.mean("issue_comments").alias("avg_comments_per_issue"),
    pl.max("repo_stars").alias("stars")
]).sort("total_issues", descending=True)

print("Repository summary:")
print(repo_summary)

# Export the results
repo_summary.write_csv("repo_summary.csv")
print("Results exported to repo_summary.csv")

# Demonstrate caching and performance
import time

start_time = time.time()
fresh_result = dev_profile.execute(username="octocat", repo="Hello-World", user_ids="1234567")
fresh_execution_time = time.time() - start_time

start_time = time.time()
cached_result = dev_profile.execute(username="octocat", repo="Hello-World", user_ids="1234567")
cached_execution_time = time.time() - start_time

print(f"Fresh execution time: {fresh_execution_time:.2f} seconds")
print(f"Cached execution time: {cached_execution_time:.2f} seconds")
print(f"Performance improvement: {fresh_execution_time / cached_execution_time:.2f}x")

# Demonstrate error handling
try:
    invalid_result = dev_profile.execute(username="invalid_user", repo="Invalid-Repo", user_ids="0")
except Exception as e:
    print(f"Error handled: {str(e)}")

# Show how to update the cache
dev_profile.invalidate_cache("octocat")
updated_result = dev_profile.execute(username="octocat", repo="Hello-World", user_ids="1234567")
print("Cache updated with fresh data")

# Demonstrate how to use the library with a different data source (e.g., CSV)
csv_source = Source.csv("Local CSV")
csv_source.add_route("sales", "path/to/sales_data.csv", "Read sales data")

sales_chain = Chain()
sales_chain.add(csv_source.route("sales"))

sales_data = sales_chain.execute()
print("Sales data summary:")
print(sales_data.describe())

# Show how to combine data from different sources
combined_chain = Chain()
combined_chain.add(github_api.route("users"))
combined_chain.add(csv_source.route("sales"))

combined_data = combined_chain.execute(username="octocat")
print("Combined data columns:")
print(combined_data.columns)

# Demonstrate the AI-powered data joining capability
joined_data = combined_chain.smart_join(
    "Join GitHub user data with sales data based on common fields or patterns"
)
print("AI-joined data preview:")
print(joined_data.head())
```

In this example:
- Data sources are defined declaratively
- The MagicTable object initializes and manages the underlying graph database
- Data fetching, caching, and relationship management happen in the graph database
- Results are presented as Polars DataFrames for high-performance manipulation
- Complex relationships can be queried using simplified methods that leverage the graph structure
- Transformations and filtering use Polars expressions for optimized operations
- Data can be easily converted to Pandas DataFrames if needed

## Why MagicTables?

- **Graph-Powered**: Utilizes a graph database under the hood for efficient data storage and relationship mapping
- **Simplicity**: Clean, declarative API with Polars DataFrame-like interface
- **Performance**: Leverages Polars for high-speed data operations and graph databases for complex relationships
- **Efficiency**: Automatic caching and relationship identification reduce redundant data fetches
- **Flexibility**: Works with various data sources and integrates easily with existing workflows
- **Intelligence**: AI-driven capabilities for data parsing, joining, and analysis, enhanced by graph relationships
- **Familiarity**: Works natively with Polars and easily converts to Pandas for broad compatibility

MagicTables lets data scientists focus on analysis rather than data preparation, making the entire process faster and more enjoyable with the power of graph databases and Polars DataFrames.
