# example.py
import logging
from magictables import Source, Chain
import polars as pl

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Define data sources
github_api = Source.api("GitHub API")
github_api.add_route(
    "users", "https://api.github.com/users/{username}", "Get user data"
)
github_api.add_route(
    "repos", "https://api.github.com/users/{username}/repos", "Get user repositories"
)

stackoverflow_api = Source.api("Stack Overflow API")
stackoverflow_api.add_route(
    "users",
    "https://api.stackexchange.com/2.3/users/{user_ids}",
    "Get Stack Overflow user data",
)

# Create a Chain
dev_profile = Chain()
dev_profile.add(github_api, "Fetch GitHub user data and repositories")
dev_profile.add(
    stackoverflow_api, "Fetch Stack Overflow user data and combine with GitHub data"
)

# Define an analysis query
dev_profile.analyze(
    "Summarize the developer's profile including GitHub and Stack Overflow data"
)

# Fetch and process data (automatically cached in graph database)
result = dev_profile.execute(
    username="octocat",
    user_ids="1234567",
    route_name="users",  # Specify the route name for each API call
)

# Work with the result as a Polars DataFrame
print(result.head())
print(result.columns)

# Perform transformations using Polars expressions
transformed = result.with_columns(pl.col("public_repos").alias("repo_count"))
filtered = transformed.filter(pl.col("repo_count") > 10)

# The result is a Polars DataFrame
polars_df = filtered

# Convert to Pandas if needed
pandas_df = filtered.to_pandas()

# Subsequent calls will use cached data and identified relationships from the graph database
cached_result = dev_profile.execute(
    username="octocat", user_ids="1234567", route_name="users"
)

print(cached_result)
