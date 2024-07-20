
# MagicTables

MagicTables is a powerful Python library designed for data scientists, API scrapers, and developers working on data pipelines and ETL processes. It creates a shadow queryable database that automatically captures and stores your data, simplifying retrieval, caching, and analysis. With easy-to-use decorators, MagicTables streamlines API integration, web scraping, and data enrichment tasks.

## Features

- Seamlessly create a shadow database that automatically captures and versions function results
- Effortlessly cache API responses and function outputs in a local SQLite database
- Augment your data pipeline with AI-generated insights
- Perform complex queries and data mining on your captured data without modifying your original code
- Join and analyze data across multiple functions for advanced data exploration
- Streamline your data engineering workflow with easy-to-use decorators
- Support for various AI models through OpenRouter API for data enrichment
- Simplify ETL processes with intelligent post-transformations
- Construct SQL queries using a fluent interface, without writing raw SQL
- Enhance data lineage and governance with built-in data cataloging
- Optimize your data ops and MLOps workflows with efficient data storage and retrieval
- Chain operations for complex data transformations
- Convert results to various formats, including JSON and pandas DataFrames

## Installation

```bash
pip install magictables
```

## Quick Start

```python
import logging
import os
from typing import List, Dict, Any
from dotenv import load_dotenv
import requests
import pandas as pd
from magictables import mtable, mai
from magictables.query import query

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load environment variables
load_dotenv()

# Ensure you have set the OPENAI_API_KEY in your .env file
# OPENAI_API_KEY=your_api_key_here

@mtable()
def fetch_github_repos(username: str) -> List[Dict[str, Any]]:
    """Fetch GitHub repositories for a given user."""
    url = f"https://api.github.com/users/{username}/repos"
    response = requests.get(url)
    repos = response.json()
    return [
        {
            "id": repo["id"],
            "name": repo["name"],
            "description": repo["description"],
            "stars": repo["stargazers_count"],
            "forks": repo["forks_count"],
            "language": repo["language"],
        }
        for repo in repos
    ]

@mtable()
def process_repo_data(repos: pd.DataFrame) -> pd.DataFrame:
    """Process and filter repository data."""
    return (
        repos[repos["stars"] > 0]
        .assign(popularity_score=lambda df: df["stars"] + df["forks"])
        .rename(columns={"name": "repo_name"})[
            ["id", "repo_name", "description", "language", "popularity_score"]
        ]
    )

@mai(batch_size=5, mode="augment", query="add a haiku for the repo")
def generate_repo_summary(repos: pd.DataFrame) -> pd.DataFrame:
    """Generate AI summaries for repositories."""
    return repos

# Main execution
if __name__ == "__main__":
    # Fetch and process data
    username = "lekt9"
    raw_repos = fetch_github_repos(username)
    processed_repos = process_repo_data(raw_repos)
    repos_with_summary = generate_repo_summary(processed_repos)

    logging.info("Repositories with AI-generated summaries:")
    logging.info(repos_with_summary)

    # Demonstrate caching
    logging.info("\nDemonstrating caching (should be faster):")
    cached_repos = fetch_github_repos(username)
    logging.info(f"Cached repos fetched for {username}")

    # Query examples
    # Fetch all results
    all_results = query("ai_generate_repo_summary").all()
    logging.info("All results: %s", all_results)

    # Filter results
    python_projects = query("ai_generate_repo_summary").filter(language="Python").all()
    logging.info("Python projects: %s", python_projects)

    # Order results
    top_projects = (
        query("ai_generate_repo_summary").order_by("-popularity_score").limit(5).all()
    )
    logging.info("Top 5 projects by popularity: %s", top_projects)

    # Count results
    project_count = (
        query("ai_generate_repo_summary").filter(popularity_score={">=": 2}).count()
    )
    logging.info(
        "Number of projects with popularity score greater than or equal to 2: %d",
        project_count,
    )

    # Get first result
    first_project = query("ai_generate_repo_summary").first()
    logging.info("First project in the database: %s", first_project)
```

## How It Works

### @mtable()

The `@mtable()` decorator automatically caches the results of API calls or any function that returns JSON-serializable data. It stores the data in a local SQLite database, allowing for quick retrieval on subsequent calls with the same arguments and enabling complex queries on the stored data.

### @mai()

The `@mai()` decorator uses AI to augment function calls with additional data. It can work in two modes:

1. "generate" mode: Creates new data based on the input.
2. "augment" mode: Extends existing data with AI-generated fields.

This allows you to enrich your data with AI-generated insights seamlessly.

### query()

The `query()` function provides a fluent interface for constructing SQL queries without writing raw SQL. It allows you to chain methods like `filter()`, `order_by()`, and `limit()` to build and execute queries on the shadow database.

## Advanced Usage

### Customizing AI Model and Batch Size

```python
@mai(
    api_key=os.environ["OPENAI_API_KEY"],
    model="openai/gpt-4o-mini",
    batch_size=20,
    mode="generate",
    query="Generate a detailed description of the repository"
)
def generate_repo_description(repos: pd.DataFrame) -> pd.DataFrame:
    return repos
```

### Working with Nested Data

MagicTables automatically handles nested data structures, storing them in separate tables and reconstructing them when queried.

### Complex Queries

```python
results = (
    query("ai_generate_repo_summary")
    .filter(language="Python", popularity_score={">=": 10})
    .order_by("-popularity_score")
    .limit(5)
    .all()
)
```

## Best Practices

1. Use meaningful function names as they are used to generate table names in the shadow database.
2. Implement proper error handling in your decorated functions to ensure data integrity.
3. Use batch processing with the `batch_size` parameter in `@mai()` for large datasets.
4. Regularly maintain and clean up your shadow database to remove outdated data.
5. Leverage the query builder for complex data analysis tasks.
6. Use environment variables for sensitive information like API keys.
7. Implement logging to track the execution flow and debug issues more easily.

## Contributing

Contributions to MagicTables are welcome! Please read our contribution guidelines for more information on our development process and coding standards.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Thanks to all the contributors who have helped shape MagicTables.
- Special thanks to the open-source community for providing the tools and libraries that make MagicTables possible.

Happy data wrangling with MagicTables!
