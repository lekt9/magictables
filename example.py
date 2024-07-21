import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
from magictables import Source, Chain, Input
import polars as pl

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

# Create an Input object
input_handler = Input()

# Create a Chain (which uses the graph database under the hood)
dev_profile = Chain()
dev_profile.add(github_api)
dev_profile.add(stackoverflow_api)

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

# Leverage graph relationships for complex queries
related_data = dev_profile.get_related("octocat", "CONTRIBUTED_TO")
print(related_data)

# Use smart_join for complex joining operations
smart_joined_data = dev_profile.smart_join(
    "Join GitHub and Stack Overflow data for users with more than 1000 reputation"
)
print(smart_joined_data)

# Example of using Input class for direct API calls
github_user_data = input_handler.from_api("GitHub API", "users", username="octocat")
print(github_user_data)

# Example of web scraping using Input class
web_data = input_handler.from_web("https://example.com")
print(web_data)

# Example of search using Input class
search_results = input_handler.from_search("Python data science libraries")
print(search_results)

# Example of reading from CSV
csv_data = input_handler.from_csv("path/to/your/data.csv")
print(csv_data)

# Example of batch processing
batch_data = input_handler.batch_process(
    github_api.execute,
    batch_size=10,
    data=["user1", "user2", "user3", "user4", "user5"],
    route_name="users",
)
print(batch_data)
