
# MagicTables

MagicTables is an advanced Python library designed to streamline data preparation and exploration for data scientists. It provides a declarative approach to data handling, allowing you to focus on analysis and model development without getting bogged down in data engineering tasks.

## Core Features

- **API to DataFrame Conversion**: Instantly convert API responses into usable DataFrames.
- **Declarative Data Preparation**: Define your data requirements, not the implementation details.
- **Dynamic Data Chaining**: Easily combine data from multiple sources with automatic type handling.
- **Natural Language Queries**: Use plain English to transform and query your data.
- **Hybrid Storage System**: Flexible storage options with a hybrid approach to caching and querying.
- **Intelligent Data Retrieval**: Dynamically query and retrieve relevant data based on context.
- **High-Performance Engine**: Powered by Polars for fast data processing.
- **Automatic Data Type Handling**: Intelligent conversion and standardization of data types, including dates and complex structures.
- **Smart Caching**: Efficient caching of data, queries, and transformations to speed up repeated analyses.
- **Automatic Key Column Identification**: Intelligently identifies suitable key columns for joining datasets.
- **Schema Detection**: Automatically detects and provides database schema, saving time on manual inspection.
- **AI-Assisted Operations**: Leverages AI for generating API descriptions and pandas code for complex transformations.

## Installation

```bash
pip install magictables
```

## Usage Examples

### Fetching Data from an API

```python
from magictables import MagicTable
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("TMDB_API_KEY")

async def fetch_popular_movies():
    popular_movies = await MagicTable.from_api(
        f"https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}"
    )
    print(popular_movies)

asyncio.run(fetch_popular_movies())
```

Output:
```
Fetching popular movies...
shape: (20, 17)
┌─────────────┬─────────────┬────────────┬────────────┬───┬──────┬────────────┬───────┬────────────┐
│ overview    ┆ original_la ┆ original_t ┆ total_page ┆ … ┆ page ┆ id         ┆ adult ┆ vote_count │
│ ---         ┆ nguage      ┆ itle       ┆ s          ┆   ┆ ---  ┆ ---        ┆ ---   ┆ ---        │
│ str         ┆ ---         ┆ ---        ┆ ---        ┆   ┆ f64  ┆ f64        ┆ f64   ┆ f64        │
│             ┆ str         ┆ str        ┆ f64        ┆   ┆      ┆            ┆       ┆            │
╞═════════════╪═════════════╪════════════╪════════════╪═══╪══════╪════════════╪═══════╪════════════╡
│ As storm    ┆ 2024-01-23T ┆ Twisters   ┆ 45217.0    ┆ … ┆ 1.0  ┆ 718821.0   ┆ 0.0   ┆ 169.0      │
│ season inte ┆ 00:00:00    ┆            ┆            ┆   ┆      ┆            ┆       ┆            │
│ nsifies, t… ┆             ┆            ┆            ┆   ┆      ┆            ┆       ┆            │
│ A young     ┆ es          ┆ Goyo       ┆ 45217.0    ┆ … ┆ 1.0  ┆ 1.19161e6  ┆ 0.0   ┆ 32.0       │
│ autistic    ┆             ┆            ┆            ┆   ┆      ┆            ┆       ┆            │
│ museum      ┆             ┆            ┆            ┆   ┆      ┆            ┆       ┆            │
│ guide …     ┆             ┆            ┆            ┆   ┆      ┆            ┆       ┆            │
│ ...         ┆ ...         ┆ ...        ┆ ...        ┆ … ┆ ...  ┆ ...        ┆ ...   ┆ ...        │
└─────────────┴─────────────┴────────────┴────────────┴───┴──────┴────────────┴───────┴────────────┘
```

### Chaining API Calls

```python
async def fetch_movie_details():
    popular_movies = await MagicTable.from_api(
        f"https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}"
    )
    
    movie_details = await popular_movies.chain(
        api_url=f"https://api.themoviedb.org/3/movie/{{id}}?api_key={API_KEY}",
    )
    print(movie_details)

asyncio.run(fetch_movie_details())
```

Output:
```
Chaining API calls for movie details...
shape: (20, 30)
┌────────────┬────────────┬───────────┬───────────┬───┬─────────┬───────────┬──────────┬───────────┐
│ overview   ┆ original_l ┆ original_ ┆ total_pag ┆ … ┆ runtime ┆ spoken_la ┆ status   ┆ tagline   │
│ ---        ┆ anguage    ┆ title     ┆ es        ┆   ┆ ---     ┆ nguages   ┆ ---      ┆ ---       │
│ str        ┆ ---        ┆ ---       ┆ ---       ┆   ┆ i64     ┆ ---       ┆ str      ┆ str       │
│            ┆ str        ┆ str       ┆ f64       ┆   ┆         ┆ list[stru ┆          ┆           │
│            ┆            ┆           ┆           ┆   ┆         ┆ ct[3]]    ┆          ┆           │
╞════════════╪════════════╪═══════════╪═══════════╪═══╪═════════╪═══════════╪══════════╪═══════════╡
│ As storm   ┆ 2024-01-23 ┆ Twisters  ┆ 45217.0   ┆ … ┆ 122     ┆ [{"Englis ┆ Released ┆ Chase.    │
│ season int ┆ T00:00:00  ┆           ┆           ┆   ┆         ┆ h","en"," ┆          ┆ Ride.     │
│ ensifies,  ┆            ┆           ┆           ┆   ┆         ┆ English"} ┆          ┆ Survive.  │
│ t…         ┆            ┆           ┆           ┆   ┆         ┆ ]         ┆          ┆           |
│ ...        ┆ ...        ┆ ...       ┆ ...       ┆ … ┆ ...     ┆ ...       ┆ ...      ┆ ...       │
└────────────┴────────────┴───────────┴───────────┴───┴─────────┴───────────┴──────────┴───────────┘
```

### Natural Language Transformation

```python
async def analyze_high_rated_movies():
    popular_movies = await MagicTable.from_api(
        f"https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}"
    )
    
    movie_details = await popular_movies.chain(
        api_url=f"https://api.themoviedb.org/3/movie/{{id}}?api_key={API_KEY}",
    )
    
    result = await movie_details.transform(
        "Find popular movies with a vote average greater than 7.5"
    )
    print(result)

asyncio.run(analyze_high_rated_movies())
```

Output:
```
Querying across chained data...
query 65898df8aeebe4b88058a9e8cbd22822
shape: (3, 3)
┌─────────────────────────┬──────────────┬─────────────────────┐
│ title                   ┆ vote_average ┆ release_date        │
│ ---                     ┆ ---          ┆ ---                 │
│ str                     ┆ f64          ┆ datetime[ns]        │
╞═════════════════════════╪══════════════╪═════════════════════╡
│ Deadpool & Wolverine    ┆ 7.8          ┆ 2024-07-24 00:00:00 │
│ Furiosa: A Mad Max Saga ┆ 7.648        ┆ 2024-05-22 00:00:00 │
│ Inside Out 2            ┆ 7.643        ┆ 2024-06-11 00:00:00 │
└─────────────────────────┴──────────────┴─────────────────────┘
```

### Comprehensive Example

Here's a more comprehensive example that demonstrates multiple features of MagicTables:

```python
from magictables import MagicTable
import os
from dotenv import load_dotenv
import asyncio

load_dotenv()
API_KEY = os.getenv("TMDB_API_KEY")

async def main():
    # Create a MagicTable instance
    mt = MagicTable()

    # Fetch popular movies
    popular_movies = await MagicTable.from_api(
        f"https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}"
    )

    # Chain API calls for movie details
    movie_details = await popular_movies.chain(
        api_url=f"https://api.themoviedb.org/3/movie/{{id}}?api_key={API_KEY}",
    )

    # Use natural language transformation
    result = await movie_details.transform(
        "Find popular movies with a vote average greater than 7.5"
    )
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
```

This example demonstrates:
1. API data fetching
2. API chaining for detailed information
3. Natural language querying
4. Automatic data type handling and caching (behind the scenes)

The output of this comprehensive example would be similar to the output of the Natural Language Transformation example shown above.
### Converting to Pandas DataFrame

MagicTables uses Polars DataFrames internally for high-performance data processing. However, you can easily convert the results to pandas DataFrames when needed. Here's how you can do it:

```python
import pandas as pd
from magictables import MagicTable
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("TMDB_API_KEY")

async def convert_to_pandas():
    # Fetch movie data
    popular_movies = await MagicTable.from_api(
        f"https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}"
    )
    
    # Perform a transformation
    high_rated_movies = await popular_movies.transform(
        "Find movies with a vote average greater than 7.5"
    )
    
    # Convert to pandas DataFrame
    pandas_df = high_rated_movies.to_pandas()
    
    # Now you can use pandas operations
    print(pandas_df.head())
    print(pandas_df.describe())

asyncio.run(convert_to_pandas())
```

Output:
```
                 title  vote_average release_date
0  Deadpool & Wolverine          7.8   2024-07-24
1    Furiosa: A Mad Max          7.6   2024-05-22
2         Inside Out 2          7.6   2024-06-11

       vote_average
count      3.000000
mean       7.666667
std        0.115470
min        7.600000
25%        7.600000
50%        7.600000
75%        7.700000
max        7.800000
```

In this example, we:
1. Fetch movie data using MagicTables
2. Perform a transformation to find high-rated movies
3. Convert the result to a pandas DataFrame using the `to_pandas()` method
4. Use pandas operations like `head()` and `describe()` on the converted DataFrame

This allows you to seamlessly integrate MagicTables with existing pandas-based workflows or libraries that expect pandas DataFrames as input.


## Advanced Features

### Automatic Data Type Handling

MagicTables automatically handles various data types, including dates, numbers, and complex data structures like lists and dictionaries. This feature saves time and reduces errors in data preprocessing.

### Smart Caching

The library implements an intelligent caching system that stores data, queries, and transformations. This significantly speeds up repeated analyses and reduces the load on external data sources.

### Automatic Key Column Identification

When joining datasets, MagicTables attempts to automatically identify the most suitable key columns, simplifying the process of merging related data.

### Schema Detection

MagicTables can automatically detect and provide the schema of your data, giving you a quick overview of your dataset's structure without manual inspection.

### AI-Assisted Operations

The library leverages AI to generate API descriptions and even pandas code for complex transformations based on natural language queries, further simplifying the data science workflow.

## Why MagicTables?

1. **Flexible Data Handling**: Utilize a hybrid approach for data storage and retrieval, combining the benefits of in-memory processing and optional persistent storage.
2. **Faster Iteration**: Reduce time spent on data preparation, allowing more focus on model development and analysis.
3. **No Data Engineering Bottleneck**: Perform complex data operations without relying on data engineering support.
4. **Intuitive Interface**: Use natural language for data transformations and queries.
5. **Performance**: Leverages efficient caching and data processing techniques without requiring a dedicated database.
6. **Flexibility**: Works with various data sources (APIs, databases, files) seamlessly.
7. **Reduced Boilerplate**: Eliminate repetitive code for data fetching, cleaning, and transformation.
8. **Exploratory Freedom**: Quickly test ideas and hypotheses without complex setup.
You're absolutely right. Adding information about Neo4j configuration and required environment variables is crucial for users to properly set up and use MagicTables. Let's add a new section to the README.md file to address this. Here's a suggested addition:

## Configuration

### Environment Variables

MagicTables requires several environment variables to be set for proper functionality. You can set these in a `.env` file in your project root or in your system environment.

Required environment variables:

- `NEO4J_URI`: The URI of your Neo4j database (e.g., `bolt://localhost:7687`)
- `NEO4J_USER`: Your Neo4j username
- `NEO4J_PASSWORD`: Your Neo4j password
- `OPENAI_API_KEY`: Your OpenAI API key for natural language processing features
- `JINA_API_KEY`: Your Jina AI API key for embedding generation

Optional environment variables:

- `OPENAI_BASE_URL`: Custom base URL for OpenAI API (default: https://openrouter.ai/api/v1/chat/completions)
- `OPENAI_MODEL`: Specific OpenAI model to use (default: gpt-4o-mini)
- `LLM_PROVIDER`: The LLM provider to use (options: "openai", "openrouter", "ollama"; default: "openai")
- `OPENROUTER_API_KEY`: Your OpenRouter API key (if using OpenRouter as LLM provider)
- `OLLAMA_API_KEY`: Your Ollama API key (if using Ollama as LLM provider)

### Neo4j Configuration

If you want to store data persistently, you need to set up a Neo4j database. Here are the steps to configure Neo4j:

1. Install Neo4j: Download and install Neo4j from the [official website](https://neo4j.com/download/).

2. Start Neo4j: Start your Neo4j server either through the Neo4j Desktop application or via command line.

3. Create a new database or use an existing one.

4. Set the environment variables:
   - Set `NEO4J_URI` to the URI of your Neo4j instance (e.g., `bolt://localhost:7687`)
   - Set `NEO4J_USER` to your Neo4j username (default is usually "neo4j")
   - Set `NEO4J_PASSWORD` to your Neo4j password

5. Ensure your Neo4j database is running before using MagicTables.

Example `.env` file:

```
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
OPENAI_API_KEY=your_openai_api_key
JINA_API_KEY=your_jina_api_key
LLM_PROVIDER=openai
```

Make sure to add `.env` to your `.gitignore` file to avoid exposing sensitive information.

## Fallback Behavior

When Neo4j is not configured or unavailable, MagicTables uses a hybrid driver that falls back to in-memory storage with JSON-based caching. This allows basic functionality to continue working, but with some limitations:

1. Data persistence is limited to what can be efficiently stored in JSON format.
2. Complex graph operations that rely on Neo4j's capabilities are not available.
3. Performance may degrade for large datasets compared to using Neo4j.
4. Cross-session persistence is limited to the data that can be saved in the cache file.

To get the full benefits of MagicTables, including efficient caching, complex graph operations, and robust cross-session persistence, it's recommended to set up and use Neo4j as described in the Configuration section.


## Contributing

Contributions are welcome. Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

MagicTables is released under the GNU General Public License v3.0 (GPL-3.0). See the [LICENSE](LICENSE) file for details.