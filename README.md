
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
from magictables import MagicTable, Source
import polars as pl

# Define data sources
github_api = Source.api("GitHub API")
github_api.add_route("users", "https://api.github.com/users/{username}")
github_api.add_route("repos", "https://api.github.com/users/{username}/repos")

stackoverflow_api = Source.api("Stack Overflow API")
stackoverflow_api.add_route("users", "https://api.stackexchange.com/2.3/users/{user_ids}")

# Create a MagicTable (initializes graph database under the hood)
dev_profile = MagicTable("Developer Profile")
dev_profile.add_source(github_api.route("users"))
dev_profile.add_source(github_api.route("repos"))
dev_profile.add_source(stackoverflow_api.route("users"))

# Fetch and process data (automatically cached in graph database)
result = dev_profile.fetch(username="octocat", user_ids="1234567")

# Work with the result as a Polars DataFrame
print(result.head())
print(result.columns)

# Perform transformations using Polars expressions
transformed = result.with_columns(
    pl.sum("repo_stars").alias("total_stars")
)
filtered = transformed.filter(pl.col("total_stars") > 1000)

# The result is a Polars DataFrame
polars_df = filtered

# Convert to Pandas if needed
pandas_df = filtered.to_pandas()

# Subsequent calls will use cached data and identified relationships from the graph database
cached_result = dev_profile.fetch(username="octocat", user_ids="1234567")

# Leverage graph relationships for complex queries (abstracted for simplicity)
related_data = dev_profile.get_related("octocat", relationship="CONTRIBUTED_TO")
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
