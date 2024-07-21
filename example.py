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
github_api.add_route(
    "issues",
    "https://api.github.com/repos/{username}/{repo}/issues",
    "Get repository issues",
)

stackoverflow_api = Source.api("Stack Overflow API")
stackoverflow_api.add_route(
    "search",
    "https://api.stackexchange.com/2.3/users?order=desc&sort=reputation&inname={username}&site=stackoverflow",
    "Search Stack Overflow users by name",
)

# Create a Chain
dev_profile = Chain()
dev_profile.add(github_api, "Fetch GitHub user data", "users")
dev_profile.add(github_api, "Fetch GitHub user repositories", "repos")
dev_profile.add(stackoverflow_api, "Search Stack Overflow users", "search")
dev_profile.add(github_api, "Fetch GitHub issues for repositories", "issues")

# Create an input DataFrame with initial data
input_data = pl.DataFrame(
    {
        "username": ["octocat", "torvalds"],
    }
)

# Fetch and process data (automatically cached in graph database)
result = dev_profile.execute(input_data=input_data)

# Work with the result as a Polars DataFrame
print("Final result:")
print(result.head())
print(result.columns)
result.write_csv("Tallied Github and Stack overflow table")
