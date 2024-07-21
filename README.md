
# MagicTables

MagicTables is a Python library designed to streamline the entire ETL (Extract, Transform, Load) process for data scientists. It provides an intuitive interface for data retrieval from various sources, including APIs, databases, web scraping, and document parsing (including PDFs). MagicTables leverages AI for automated data joining, processing, and optimization, creating a shadow database to efficiently manage and cache your data, eliminating the need to worry about data storage and retrieval.

## Features

- Automatic data caching and retrieval optimization
- Declarative data source definitions with AI-powered content parsing
- Intelligent static mapping with automatic fallback to dynamic generation
- Automatic derivation of CSS selectors and XPath expressions for web scraping
- Batch retrieval for efficient processing and format identification
- Flexible API route definitions
- Easy integration of various data sources (APIs, databases, files, websites, PDFs, etc.)
- Intuitive data fetching operations, including intelligent web scraping and PDF parsing
- AI-driven data joining, processing, and format derivation
- Automatic shadow database creation and management
- Seamless conversion to Polars or Pandas DataFrames
- Transparent data flow explanations

## Installation

```bash
pip install magictables
```

## Quick Start

Here's an example demonstrating declarative queries, automatic caching, and batch processing:

```python
from magictables import Source, Chain, Explain, to_dataframe

# Define data sources with declarative queries
github_api = Source.api("GitHub API")
github_api.add_route("users", "https://api.github.com/users/{username}", 
                     query="Get user data including name, bio, and public repos count")
github_api.add_route("repos", "https://api.github.com/users/{username}/repos", 
                     query="Get user's repositories with star counts and languages used")

github_web = Source.web("https://github.com/{username}", 
                        query="Extract user's contribution graph data for the last year")

user_pdf = Source.pdf("/path/to/user_reports/", 
                      query="Extract user's project summaries and completion dates")

# Create a data processing chain
user_analysis = Chain()
user_analysis.add(github_api.route("users"))
user_analysis.add(github_api.route("repos"))
user_analysis.add(github_web)
user_analysis.add(user_pdf)

# AI-driven data joining and analysis
user_analysis.analyze("Combine API user data, web-scraped contribution data, and PDF report data")

# Execute the chain with batch processing (data is automatically cached)
result = user_analysis.execute(usernames=["octocat", "torvalds", "gvanrossum"], batch_size=10)

# Convert to Polars DataFrame (default)
df = to_dataframe(result)
print(df)

# Subsequent calls will use cached data when available
cached_result = user_analysis.execute(usernames=["octocat", "torvalds", "gvanrossum"], batch_size=10)
```

In this example:
- Data sources are defined with their queries.
- Data is automatically cached in the shadow database for future use.
- Static mapping is used by default, with automatic fallback to dynamic generation if needed.
- Web scraping automatically derives CSS selectors or XPath expressions as part of the mapping process.
- The `execute` method supports batch processing and uses cached data when available.

## Advanced Usage

### Multiple Data Sources with Complex Queries and Batch Processing

```python
from magictables import Source, Chain, Explain, to_dataframe

# Define sources with declarative queries
github_api = Source.api("GitHub API")
github_api.add_route("repos", "https://api.github.com/users/{username}/repos", 
                     query="Get user's repositories with star counts and languages used")

local_db = Source.sql("sqlite:///local_data.db", 
                      query="Extract user's historical activity data")

csv_file = Source.csv("/path/to/additional_data.csv", 
                      query="Parse user engagement metrics from CSV")

tech_news = Source.web("https://technews.com", 
                       query="Extract latest AI and data science news headlines and summaries")

user_pdfs = Source.pdf("/path/to/pdf/directory", 
                       query="Extract project details and outcomes from all user PDFs")

# Create and execute chain
user_analysis = Chain()
user_analysis.add(github_api.route("repos"))
user_analysis.add(local_db)
user_analysis.add(csv_file)
user_analysis.add(tech_news)
user_analysis.add(user_pdfs)
user_analysis.analyze("Combine all user data sources, relevant tech news, and PDF data, focusing on AI and data science contributions")

# Execute the chain with batch processing (data is automatically cached)
result = user_analysis.execute(usernames=["octocat", "torvalds", "gvanrossum"], 
                               date_range=("2023-01-01", "2023-12-31"),
                               batch_size=20)

# Convert to Polars DataFrame
df = to_dataframe(result)
print(df)
```

### Explaining AI-Driven Data Flow and Parsing

```python
explanation = Explain(user_analysis)
print(explanation.summary())
print(explanation.data_flow())
print(explanation.parsing_logic())
print(explanation.shadow_db_schema())
```

The `Explain` class provides insights into the static mapping, dynamic generation decisions, and caching strategies made by the AI.

## Automatic Data Caching and Retrieval

MagicTables automatically caches data in its shadow database, optimizing data retrieval and reducing the need for repeated API calls or web scraping. This feature is central to MagicTables' functionality:

1. When data is first retrieved, it's automatically stored in the shadow database.
2. Subsequent queries first check the cache for available data.
3. If cached data is available and still valid, it's used instead of making new API calls or scraping web pages.
4. Cache invalidation is handled intelligently based on data source type and user-defined rules.
5. Users can control caching behavior and clear the cache when needed.

This approach significantly speeds up data retrieval and reduces the load on external data sources.

## Shadow Database Management

MagicTables automatically creates and manages a shadow database to optimize data retrieval and caching. Static mappings and retrieved data are stored for reuse, improving performance for subsequent queries.

```python
from magictables import ShadowDB

# Get the shadow database instance
shadow_db = ShadowDB.get_instance()

# Query the shadow database for cached data
result = shadow_db.query("SELECT * FROM cached_data WHERE source = ?", ("github_api",))

# Clear specific cache
shadow_db.clear_cache("github_users")

# Clear all cache
shadow_db.clear_all_cache()
```

## Intelligent Web Scraping

MagicTables uses AI to automatically derive the most appropriate CSS selectors or XPath expressions for web scraping. This process is part of the intelligent static mapping feature:

1. When a web source is defined, MagicTables analyzes the target webpage's structure.
2. Based on the query provided, it identifies the most likely HTML elements containing the requested data.
3. It generates optimal CSS selectors or XPath expressions to extract this data.
4. These selectors are stored in the shadow database for future use, improving performance for subsequent queries.
5. If the webpage structure changes, MagicTables can dynamically regenerate appropriate selectors.

This approach eliminates the need for manual selector specification, making web scraping more robust and adaptable to changes in webpage structure.

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for more details.

## License

MagicTables is released under the MIT License. See the [LICENSE](LICENSE) file for more details.
