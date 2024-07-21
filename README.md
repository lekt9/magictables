# MagicTables

MagicTables is a Python library designed to streamline data science workflows by eliminating CSV hell and simplifying data management. It leverages SQLite for robust local data storage and provides an intuitive interface for data scientists to focus on analysis rather than data engineering.

## Core Features

1. **Automated Data Management**
   - Automatic SQLite database creation and management
   - Seamless conversion between DataFrames and database tables
   - Eliminate CSV files: work directly with structured data in SQLite

2. **API Reverse Engineering**
   - Automatically create local database schemas from API responses
   - Cache API data locally for faster access and reduced API calls

3. **Intelligent Caching**
   - LRU cache for function results with customizable size
   - Automatic query optimization for SQLite database

4. **DataFrame Integration**
   - Seamless conversion between SQLite tables and Polars/Pandas DataFrames
   - Lazy evaluation for large dataset operations

5. **Decorator-Based Workflow**
   - `@mtable`: Automatically cache function results in SQLite
   - `@msource`: Process various input sources (API, PDF, web) and structure data in SQLite
   - `@mchain`: Chain multiple data processing steps with automatic data passing through SQLite

## Quick Start

```python
from magictables import mtable, create_chain, msource
import requests
import polars as pl

@msource(input_type="api")
def fetch_user_data(api_url):
    response = requests.get(api_url)
    return response.json()  # Automatically stored in SQLite

@mtable(query="Process user data and count users by city")
def process_user_data(user_data):
    df = pl.DataFrame(user_data)
    return df.groupby("city").agg(pl.count("id").alias("user_count"))

def main():
    user_analysis_chain = create_chain(
        fetch_user_data,
        process_user_data
    )

    # Usage
    result = user_analysis_chain.execute({"api_url": "https://api.example.com/users"})
    print(result)

    # Data is automatically cached in SQLite. Subsequent calls use cached data:
    result_cached = user_analysis_chain.execute({"api_url": "https://api.example.com/users"})

if __name__ == "__main__":
    main()
```

## Premium Features

1. **AI-Powered Data Joining and Augmentation**
   - Intelligent interface for analyzing multiple datasets and suggesting optimal joining strategies
   - AI-driven identification of potential key columns for joining
   - Fuzzy matching algorithms for joining datasets based on similar but not identical values

2. **Knowledge Graph Integration**
   - Automatic building and updating of knowledge graphs representing relationships between different entities and datasets
   - Visual exploration of the knowledge graph to understand data relationships and potential feature interactions

3. **Semantic Understanding and Entity Resolution**
   - Natural language processing to understand the semantic meaning of columns and data
   - Advanced entity resolution techniques to identify and link related entities across datasets

4. **Automated Feature Engineering from Joined Data**
   - Automatic suggestion and generation of new features based on relationships and patterns discovered in combined datasets

5. **Comprehensive Data Lineage and Version Control**
   - Detailed tracking of the entire process of data joining and augmentation
   - Advanced version control for augmented datasets

6. **Collaborative Data Enrichment**
   - Tools for teams to collaboratively work on data joining and augmentation tasks
   - Knowledge and insight sharing capabilities

7. **API for Automated Data Enrichment**
   - Powerful API that allows users to automatically enrich their datasets with relevant external data

## Why MagicTables?

- Eliminate CSV file management: Work directly with structured data in SQLite
- Reduce API calls: Intelligent caching of API responses
- Simplify data pipelines: Easy-to-use decorators for complex workflows
- Improve reproducibility: Automatic versioning of datasets in SQLite
- Enhance collaboration: Shared access to structured data and APIs
- Accelerate data preparation: AI-powered joining and feature engineering (Premium)
- Gain deeper insights: Knowledge graph integration and visual exploration (Premium)
- Ensure data quality: Advanced lineage tracking and semantic understanding (Premium)

## Installation

```bash
pip install magictables
```

For detailed documentation and tutorials, visit [docs.magictables.ai](https://docs.magictables.ai)
