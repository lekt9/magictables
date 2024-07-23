
# MagicTables Quickstart Guide

MagicTables is a powerful library that extends Polars DataFrames with AI-assisted data manipulation and API integration capabilities. This quickstart guide will help you get up and running with MagicTables.

## Installation

First, install MagicTables using pip:

```bash
pip install magictables
```

## Setup

Before using MagicTables, you need to set up your environment variables. Create a `.env` file in your project directory with the following content:

```
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=your_neo4j_username
NEO4J_PASSWORD=your_neo4j_password
OPENAI_API_KEY=your_openai_api_key
JINA_API_KEY=your_jina_api_key
```

Replace the values with your actual credentials.

## Basic Usage

Here's a simple example to get you started with MagicTables:

```python
import asyncio
from magictables import MagicTable
from dotenv import load_dotenv
import os

load_dotenv()  # Load environment variables from .env file

async def main():
    # Create a MagicTable instance
    mt = MagicTable()

    # Fetch data from an API
    api_key = os.getenv("YOUR_API_KEY")
    movies = await MagicTable.from_api(f"https://api.example.com/movies?api_key={api_key}")

    # Chain API calls to get more details
    movie_details = await movies.chain(
        api_url=f"https://api.example.com/movie/{{id}}?api_key={api_key}",
    )

    # Transform data using natural language
    popular_movies = await movie_details.transform(
        "Find popular movies with a vote average greater than 7.5"
    )

    print(popular_movies)

if __name__ == "__main__":
    asyncio.run(main())
```

## Key Features

1. **API Integration**: Easily fetch data from APIs and create MagicTables.
2. **Chaining**: Chain API calls to enrich your data.
3. **Natural Language Transformations**: Use plain English to transform your data.
4. **Caching**: MagicTables caches results for improved performance.

## Next Steps

- Explore more advanced transformations and joins using natural language queries.
- Use the `clear_all_data()` method to reset your database when needed.
- Check out the full API reference for more detailed information on available methods.

For more information, refer to the full documentation and API reference.
