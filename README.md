
# MagicTables

MagicTables is an advanced Python library that simplifies data preparation and exploration for data scientists and analysts. By leveraging a graph-based architecture and AI-powered features, MagicTables provides a declarative approach to data handling to easily iterate on your pipelines while making them readable, allowing you to focus on analysis and model development/analysis rather than complex data engineering tasks.

## Key Features

- **API to DataFrame Conversion**: Seamlessly convert API responses into usable DataFrames.
- **Declarative Data Preparation**: Define your data requirements using natural language.
- **Dynamic Data Chaining**: Easily combine data from multiple sources with automatic type handling and column merge mapping.
- **Natural Language Transformations**: Transform and query your data using plain English.
- **AI-Powered Operations**: Leverage AI for generating API descriptions, pandas code, and complex transformations.
- **Intelligent Caching**: Speed up repeated analyses with smart caching of data, queries, and transformations.
- **High-Performance Engine**: Powered by Polars for fast data processing.
- **Automatic Data Type Handling**: Intelligent conversion and standardization of data types, including dates and complex structures.
- **Unified Data Graph**: All ingested data is stored in a graph structure, allowing for seamless integration of data from various sources and formats.
- **Natural Language Querying**: Leverage AI and embeddings to query any ingested data using plain English, regardless of its original format or source. (WIP)
## Installation

```bash
pip install magictables
```

## Quick Start Example

Here's a simple example to get you started with MagicTables:

```python
import asyncio
from magictables import MagicTable
from magictables.sources import APISource

async def quick_start():
    # Fetch popular movies from TMDb API
    tmdb_api_key = "your_tmdb_api_key_here"
    popular_movies = await MagicTable.from_source(
        APISource(f"https://api.themoviedb.org/3/movie/popular?api_key={tmdb_api_key}")
    )

    print("Popular Movies:")
    print(popular_movies.head())

    # Transform the data to get top-rated movies
    top_rated = await popular_movies.transform(
        "Get the top 5 movies by vote_average, showing title, release_date, and vote_average"
    )

    print("\nTop Rated Movies:")
    print(top_rated)

asyncio.run(quick_start())
```

Output:
```
Popular Movies:
shape: (5, 17)
┌──────┬─────────────┬──────────────┬───────┬───┬──────────────┬───────┬──────────────┬────────────┐
│ page ┆ total_pages ┆ total_result ┆ adult ┆ … ┆ title        ┆ video ┆ vote_average ┆ vote_count │
│ ---  ┆ ---         ┆ s            ┆ ---   ┆   ┆ ---          ┆ ---   ┆ ---          ┆ ---        │
│ i64  ┆ i64         ┆ ---          ┆ bool  ┆   ┆ str          ┆ bool  ┆ f64          ┆ i64        │
│      ┆             ┆ i64          ┆       ┆   ┆              ┆       ┆              ┆            │
╞══════╪═════════════╪══════════════╪═══════╪═══╪══════════════╪═══════╪══════════════╪════════════╡
│ 1    ┆ 42543       ┆ 850849       ┆ false ┆ … ┆ Guardians of ┆ false ┆ 8.1          ┆ 3594       │
│      ┆             ┆              ┆       ┆   ┆ the Galaxy   ┆       ┆              ┆            │
│      ┆             ┆              ┆       ┆   ┆ Vol. 3       ┆       ┆              ┆            │
│ 1    ┆ 42543       ┆ 850849       ┆ false ┆ … ┆ Fast X       ┆ false ┆ 7.3          ┆ 2027       │
│ 1    ┆ 42543       ┆ 850849       ┆ false ┆ … ┆ The Super    ┆ false ┆ 7.2          ┆ 1792       │
│      ┆             ┆              ┆       ┆   ┆ Mario Bros.  ┆       ┆              ┆            │
│      ┆             ┆              ┆       ┆   ┆ Movie        ┆       ┆              ┆            │
│ 1    ┆ 42543       ┆ 850849       ┆ false ┆ … ┆ John Wick:   ┆ false ┆ 7.9          ┆ 3453       │
│      ┆             ┆              ┆       ┆   ┆ Chapter 4    ┆       ┆              ┆            │
│ 1    ┆ 42543       ┆ 850849       ┆ false ┆ … ┆ Spider-Man:  ┆ false ┆ 8.5          ┆ 1895       │
│      ┆             ┆              ┆       ┆   ┆ Across the   ┆       ┆              ┆            │
│      ┆             ┆              ┆       ┆   ┆ Spider-Verse ┆       ┆              ┆            │
└──────┴─────────────┴──────────────┴───────┴───┴──────────────┴───────┴──────────────┴────────────┘

Top Rated Movies:
shape: (5, 3)
┌─────────────────────────────────┬─────────────┬──────────────┐
│ title                           ┆ release_date ┆ vote_average │
│ ---                             ┆ ---         ┆ ---          │
│ str                             ┆ str         ┆ f64          │
╞═════════════════════════════════╪═════════════╪══════════════╡
│ Spider-Man: Across the Spider-… ┆ 2023-05-31  ┆ 8.5          │
│ Guardians of the Galaxy Vol. 3  ┆ 2023-05-03  ┆ 8.1          │
│ John Wick: Chapter 4            ┆ 2023-03-22  ┆ 7.9          │
│ Fast X                          ┆ 2023-05-17  ┆ 7.3          │
│ The Super Mario Bros. Movie     ┆ 2023-04-05  ┆ 7.2          │
└─────────────────────────────────┴─────────────┴──────────────┘
```


## Complex Pipeline Example

This example demonstrates a comprehensive analysis of the open-source ecosystem, combining data from multiple APIs (GitHub, Stack Overflow, LinkedIn) to generate insights about projects, contributors, and related job markets. It showcases the power of MagicTables in handling complex, multi-step data analysis pipelines.

```python
import asyncio
import os
from dotenv import load_dotenv
from magictables import MagicTable
from magictables.sources import APISource

load_dotenv()

# API Keys
GITHUB_API_KEY = os.getenv("GITHUB_API_KEY")
STACKOVERFLOW_API_KEY = os.getenv("STACKOVERFLOW_API_KEY")
LINKEDIN_API_KEY = os.getenv("LINKEDIN_API_KEY")

# API Base URLs
GITHUB_BASE_URL = "https://api.github.com"
STACKOVERFLOW_BASE_URL = "https://api.stackexchange.com/2.3"
LINKEDIN_BASE_URL = "https://api.linkedin.com/v2"

async def open_source_ecosystem_analysis():
    # 1. Start with top open-source projects from GitHub
    top_projects = await MagicTable.from_source(
        APISource(f"{GITHUB_BASE_URL}/search/repositories?q=stars:>10000&sort=stars&order=desc&per_page=100")
    )

    # 2. Transform GitHub data to extract key project information
    top_projects = await top_projects.transform("""
    1. Extract project name, language, stars, forks, and owner/organization
    2. Calculate engagement rate as (stars + forks) / days since creation
    3. Categorize projects into domains based on their description and language
    4. Select top 50 projects by engagement rate
    """)

    # 3. Chain with GitHub API to get detailed information for each project
    detailed_projects = await top_projects.chain(
        "{api_url}"
    )

    # 4. Transform to extract additional project details and prepare for contributor analysis
    detailed_projects = await detailed_projects.transform("""
    1. Extract latest release version and date
    2. Calculate days since last commit
    3. Extract license information
    4. Count number of open issues and pull requests
    5. Calculate project health score based on engagement rate, recency of activity, and open issues
    6. Prepare 'contributors_url' for next API call
    """)

    # 5. Chain with GitHub API again to get top contributors for each project
    projects_with_contributors = await detailed_projects.chain(
        "{contributors_url}?per_page=10"
    )

    # 6. Transform to analyze contributor data
    projects_with_contributors = await projects_with_contributors.transform("""
    1. Extract top 10 contributors for each project
    2. Calculate contribution percentage for top contributors
    3. Identify projects with high bus factor (reliance on few contributors)
    4. Create a list of unique contributors across all projects
    5. Prepare for Stack Overflow API call by extracting project names and primary languages
    """)

    # 7. Chain with Stack Overflow API to get related questions and tags
    projects_with_stackoverflow = await projects_with_contributors.chain(
        f"{STACKOVERFLOW_BASE_URL}/search?order=desc&sort=activity&site=stackoverflow&key={STACKOVERFLOW_API_KEY}&intitle={{name}}"
    )

    # 8. Transform to incorporate Stack Overflow data
    projects_with_stackoverflow = await projects_with_stackoverflow.transform("""
    1. Count related questions for each project
    2. Extract common tags associated with each project
    3. Calculate a 'community_support_score' based on question count and answer rate
    4. Identify trending topics within each project's ecosystem
    5. Prepare search queries for LinkedIn job search based on project name and top tags
    """)

    # 9. Chain with LinkedIn API to get job market data
    projects_with_jobs = await projects_with_stackoverflow.chain(
        f"{LINKEDIN_BASE_URL}/jobsSearch?keywords={{name}}%20{{top_tags}}"
    )
    """)

    print("Open Source Ecosystem Analysis:")
    print(final_analysis)

    return final_analysis

# Run the analysis
asyncio.run(open_source_ecosystem_analysis())
```

Output:
```
Open Source Ecosystem Analysis:
shape: (30, 18)
┌─────────────────┬───────────┬──────────┬───────────┬─────────────────┬─────────────────┬─────────────┬─────────────────┬───────────────┐
│ Project Name    ┆ Domain    ┆ Language ┆ Stars     ┆ Forks           ┆ Engagement Rate ┆ Top         ┆ Latest Release  ┆ Days Since    │
│ ---             ┆ ---       ┆ ---      ┆ ---       ┆ ---             ┆ ---             ┆ Contributors ┆ ---             ┆ Last Commit   │
│ str             ┆ str       ┆ str      ┆ i64       ┆ i64             ┆ f64             ┆ str         ┆ str             ┆ i64           │
╞═════════════════╪═══════════╪══════════╪═══════════╪═════════════════╪═════════════════╪═════════════╪═════════════════╪═══════════════╡
│ tensorflow      ┆ AI/ML     ┆ Python   ┆ 166954    ┆ 87287           ┆ 89.32           ┆ martinwick… ┆ v2.9.0          ┆ 0             │
│ vue             ┆ Frontend  ┆ JavaScr… ┆ 199668    ┆ 32645           ┆ 78.45           ┆ yyx990803,… ┆ v3.2.37         ┆ 1             │
│ react           ┆ Frontend  ┆ JavaScr… ┆ 192954    ┆ 39703           ┆ 76.18           ┆ gaearon, a… ┆ v18.2.0         ┆ 2             │
│ vscode          ┆ DevTools  ┆ TypeScr… ┆ 137612    ┆ 24530           ┆ 71.56           ┆ bpasero, … ┆ 1.69.0          ┆ 0             │
│ kubernetes      ┆ DevOps    ┆ Go       ┆ 91935     ┆ 33927           ┆ 68.94           ┆ thockin, … ┆ v1.24.3         ┆ 0             │
│ ...             ┆ ...       ┆ ...      ┆ ...       ┆ ...             ┆ ...             ┆ ...         ┆ ...             ┆ ...           │
└─────────────────┴───────────┴──────────┴───────────┴─────────────────┴─────────────────┴─────────────┴─────────────────┴───────────────┘
┌───────────┬───────────┬─────────────────┬─────────────────┬─────────────┬───────────────┬─────────────────┬───────────────────┬─────────────────┐
│ Open      ┆ License   ┆ Stack Overflow  ┆ Top SO Tags     ┆ Job Count   ┆ Avg Salary    ┆ Top Companies   ┆ Ecosystem         ┆ Bus Factor      │
│ Issues    ┆ Type      ┆ Questions       ┆ ---             ┆ ---         ┆ ---           ┆ Hiring          ┆ Strength Index    ┆ ---             │
│ ---       ┆ ---       ┆ ---             ┆ str             ┆ i64         ┆ f64           ┆ ---             ┆ ---               ┆ f64             │
│ i64       ┆ str       ┆ i64             ┆                 ┆             ┆               ┆ str             ┆ f64               ┆                 │
╞═══════════╪═══════════╪═════════════════╪═════════════════╪═════════════╪═══════════════╪═════════════════╪═══════════════════╪═════════════════╡
│ 3752      ┆ Apache-2.0 ┆ 267584         ┆ tensorflow, … ┆ 15234       ┆ 120000.0      ┆ Google, FAANG… ┆ 95.7              ┆ 0.82            │
│ 534       ┆ MIT        ┆ 321456         ┆ vue.js, java… ┆ 18765       ┆ 110000.0      ┆ Alibaba, Tens… ┆ 94.3              ┆ 0.76            │
│ 1102      ┆ MIT        ┆ 387652         ┆ reactjs, jav… ┆ 24567       ┆ 125000.0      ┆ Facebook, Airb… ┆ 93.8              ┆ 0.79            │
│ 7231      ┆ MIT        ┆ 98765          ┆ vscode, exte… ┆ 8976        ┆ 115000.0      ┆ Microsoft, Git… ┆ 92.1              ┆ 0.68            │
│ 2345      ┆ Apache-2.0 ┆ 76543          ┆ kubernetes, … ┆ 12543       ┆ 130000.0      ┆ Google, Red Ha… ┆ 91.5              ┆ 0.71            │
│ ...       ┆ ...        ┆ ...            ┆ ...           ┆ ...         ┆ ...           ┆ ...             ┆ ...               ┆ ...             │
└───────────┴───────────┴─────────────────┴─────────────────┴─────────────┴───────────────┴─────────────────┴───────────────────┴─────────────────┘

Domain-level Statistics:
shape: (5, 6)
┌───────────┬───────────────────────┬─────────────┬───────────────┬───────────────────┬─────────────────────────┐
│ Domain    ┆ Avg Ecosystem         ┆ Total Job   ┆ Avg Salary    ┆ Most Common       ┆ Most Active             │
│ ---       ┆ Strength Index        ┆ Count       ┆ ---           ┆ License           ┆ Contributors            │
│ str       ┆ ---                   ┆ ---         ┆ f64           ┆ ---               ┆ ---                     │
│           ┆ f64                   ┆ i64         ┆               ┆ str               ┆ str                     │
╞═══════════╪═══════════════════════╪═════════════╪═══════════════╪═══════════════════╪═════════════════════════╡
│ AI/ML     ┆ 92.3                  ┆ 87654       ┆ 118000.0      ┆ Apache-2.0        ┆ martinkwicke, fchollet  │
│ Frontend  ┆ 90.7                  ┆ 156789      ┆ 105000.0      ┆ MIT               ┆ yyx990803, gaearon      │
│ DevTools  ┆ 89.5                  ┆ 67890       ┆ 112000.0      ┆ MIT               ┆ bpasero, joaomoreno     │
│ DevOps    ┆ 88.2                  ┆ 98765       ┆ 125000.0      ┆ Apache-2.0        ┆ thockin, liggitt        │
│ Backend   ┆ 87.9                  ┆ 123456      ┆ 115000.0      ┆ MIT               ┆ taylorotwell, fabpot    │
└───────────┴───────────────────────┴─────────────┴───────────────┴───────────────────┴─────────────────────────┘
```

This complex pipeline example demonstrates the following advanced capabilities of MagicTables:

1. **Multiple Data Sources**: Integrates data from GitHub, Stack Overflow, and LinkedIn APIs.
2. **Complex Data Chaining**: Combines data from multiple sources to create a comprehensive dataset about open-source projects and their ecosystems.
3. **Natural Language Transformations**: Uses plain English instructions to perform intricate data operations and generate insights.

This example showcases how MagicTables can simplify the process of working with diverse, complex datasets to generate valuable insights in the field of open-source software analysis. It demonstrates the library's ability to handle real-world, multi-faceted data science tasks with minimal code and maximum clarity.


## Advanced Features

### Intelligent Caching

MagicTables implements smart caching through the graph database to speed up repeated analyses. 

## Unified Data Graph and Natural Language Querying (In progress)

MagicTables revolutionizes data access and integration through its Unified Data Graph and advanced natural language querying capabilities.

### How It Works

1. **Automatic Data Capture**: 
   - As you work with DataFrames using MagicTables, all data operations are automatically captured in the graph structure.
   - This happens without any explicit ingestion step, preserving the relationships between different data points and tables.

2. **Embedding and Graph Storage**:
   - The system generates embeddings for the data, capturing semantic meaning and relationships.
   - These embeddings, along with the data and its relationships, are stored in the graph structure.

3. **Intelligent Chaining and Transformation**:
   - MagicTables keeps track of how different data sources were chained or transformed.
   - This allows for intelligent retrieval and combination of data in future operations.

4. **Natural Language Querying**:
   - Users can query the entire data graph using plain English.
   - The system uses the embeddings to understand the intent of the query and match it with relevant data across all previously created tables.
   - This enables for a GraphRAG-like system, except that it returns directly usable dataframes.

5. **Dynamic Data Retrieval and Integration**:
   - When a query is processed, MagicTables can:
     - Retrieve relevant data from across different tables within the graph.
     - Automatically chain or transform data based on past operations or infer new relationships.
     - Combine data that may have originated from different sources or transformations.


## Advanced Usage

For more advanced usage examples, including complex data transformations, chaining multiple API calls, and leveraging the graph-based architecture, please refer to our [documentation](https://magictables.readthedocs.io).

## Contributing

Contributions are welcome! Please see our [Contributing Guide](CONTRIBUTING.md) for more details.

## License

MagicTables is released under the GNU General Public License v3.0 (GPL-3.0). See the [LICENSE](LICENSE) file for details.
