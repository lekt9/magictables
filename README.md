
# MagicTables

MagicTables is an advanced Python library that revolutionizes data preparation and exploration for data scientists. By leveraging a graph-based architecture and AI-powered features, MagicTables provides a declarative approach to data handling, allowing you to focus on analysis and model development rather than complex data engineering tasks.

## Key Features

- **API to DataFrame Conversion**: Seamlessly convert API responses into usable DataFrames.
- **Declarative Data Preparation**: Define your data requirements using natural language.
- **Dynamic Data Chaining**: Easily combine data from multiple sources with automatic type handling.
- **Natural Language Queries**: Transform and query your data using plain English.
- **Graph-Based Architecture**: Utilize a powerful graph database for efficient data linking and querying.
- **AI-Powered Operations**: Leverage AI for generating API descriptions, pandas code, and complex transformations.
- **Intelligent Caching**: Speed up repeated analyses with smart caching of data, queries, and transformations.
- **Automatic Schema Detection**: Save time with automatic database schema detection and visualization.

## Requirements

- Python 3.9+
- Neo4j Database (for full functionality)
- OpenAI API key (for AI-powered features)

## Installation

```bash
pip install magictables
```

## Quick Start

```python
from magictables import MagicTable
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("TMDB_API_KEY")

async def fetch_movie_details():
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

asyncio.run(fetch_movie_details())
```

## Why MagicTables?

1. **Simplified Data Workflow**: Reduce time spent on data preparation and focus more on analysis.
2. **Intuitive Interface**: Use natural language for data transformations and queries.
3. **Powerful Backend**: Leverage graph database capabilities for complex data operations.
4. **AI Integration**: Utilize OpenAI's embeddings and language models for advanced NLP capabilities.
5. **Flexibility**: Work seamlessly with various data sources (APIs, databases, files).
6. **Performance**: Benefit from efficient caching and data processing techniques.
7. **Reduced Boilerplate**: Eliminate repetitive code for data fetching, cleaning, and transformation.
8. **Data Lineage**: Easily track and visualize the flow of your data processing pipeline.

## Graph-Based Architecture

MagicTables uses a graph database (Neo4j) under the hood, providing several advantages:

1. **Efficient Data Linking**: Naturally represent relationships between datasets, API calls, and transformations.
2. **Flexible Querying**: Perform complex relationship-based queries with ease.
3. **Rich Metadata Management**: Store and query detailed information about your data operations.
4. **Intelligent Caching**: Reuse previous computations to speed up repeated analyses.
5. **Semantic Understanding**: Enable natural language querying and context-based data retrieval.

## AI-Powered Features

MagicTables integrates OpenAI's technology to provide advanced capabilities:

1. **Embeddings**: Create vector representations of data, queries, and API descriptions for semantic similarity searches.
2. **Language Models**: Generate API descriptions, create database queries from natural language, and produce code for complex transformations.

## Configuration

MagicTables requires several environment variables to be set for full functionality:

- `NEO4J_URI`: The URI of your Neo4j database
- `NEO4J_USER`: Your Neo4j username
- `NEO4J_PASSWORD`: Your Neo4j password
- `OPENAI_API_KEY`: Your OpenAI API key

Optional environment variables for customization:

- `OPENAI_BASE_URL`: Custom base URL for OpenAI API
- `OPENAI_MODEL`: Specific OpenAI model to use
- `LLM_PROVIDER`: The LLM provider to use (options: "openai", "openrouter", "ollama")
- `EMBEDDING_PROVIDER`: The provider to use for embeddings
- `EMBEDDING_MODEL`: The specific model to use for embeddings

## Advanced Usage

For more advanced usage examples, including complex data transformations, chaining multiple API calls, and leveraging the graph-based architecture, please refer to our [documentation](https://magictables.readthedocs.io).

## Contributing

Contributions are welcome! Please see our [Contributing Guide](CONTRIBUTING.md) for more details.

## License

MagicTables is released under the GNU General Public License v3.0 (GPL-3.0). See the [LICENSE](LICENSE) file for details.
