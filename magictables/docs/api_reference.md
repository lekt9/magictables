
# MagicTables API Reference

This document provides a detailed reference for the MagicTables library API.

## MagicTable Class

The `MagicTable` class is the main interface for working with data in MagicTables. It extends the `pl.DataFrame` class from the Polars library.

### Constructor

```python
MagicTable(*args, **kwargs)
```

Creates a new MagicTable instance. It accepts the same arguments as a Polars DataFrame.

### Class Methods

#### from_api

```python
@classmethod
async def from_api(cls, api_url: str, params: Optional[Dict[str, Any]] = None, include_embedding=False) -> "MagicTable"
```

Fetches data from an API and creates a MagicTable instance.

- `api_url`: The URL of the API endpoint.
- `params`: Optional parameters to be sent with the API request.
- `include_embedding`: Whether to include embeddings in the result.

#### from_polars

```python
@classmethod
async def from_polars(cls, df: pl.DataFrame, label: str) -> "MagicTable"
```

Creates a MagicTable instance from a Polars DataFrame.

- `df`: The Polars DataFrame to convert.
- `label`: A label for the data.

### Instance Methods

#### chain

```python
async def chain(self, api_url: Union[str, Dict[str, str]], key: Optional[str] = None, params: Optional[Dict[str, Any]] = None, expand: bool = False) -> "MagicTable"
```

Chains API calls based on the current data.

- `api_url`: The URL template for the API call, or a dictionary mapping column names to URL templates.
- `key`: The column to use as the key for chaining (optional).
- `params`: Additional parameters for the API call (optional).
- `expand`: Whether to expand list results into separate rows.

#### transform

```python
async def transform(self, natural_query: str) -> "MagicTable"
```

Applies a transformation to the data based on a natural language query.

- `natural_query`: A natural language description of the desired transformation.

#### join_with_query

```python
async def join_with_query(self, natural_query: str) -> "MagicTable"
```

Joins the current data with the result of a natural language query.

- `natural_query`: A natural language description of the join operation.

#### to_pandas

```python
def to_pandas(self) -> pd.DataFrame
```

Converts the MagicTable to a pandas DataFrame.

#### clear_all_data

```python
async def clear_all_data(self)
```

Clears all data from the Neo4j database, including APIEndpoint nodes.

### Context Manager

MagicTable can be used as an async context manager:

```python
async with MagicTable() as mt:
    # Use mt here
```

This ensures proper initialization and cleanup of resources.

## Utility Functions

### flatten_nested_structure

```python
def flatten_nested_structure(nested_structure)
```

Flattens a nested data structure into a list of dictionaries.

### call_ai_model

```python
async def call_ai_model(input_data: List[Dict[str, Any]], prompt: str, model: str = None) -> Dict[str, Any]
```

Calls an AI model with the given input data and prompt.

- `input_data`: The input data for the AI model.
- `prompt`: The prompt for the AI model.
- `model`: The specific AI model to use (optional).

## Environment Variables

MagicTables uses the following environment variables:

- `NEO4J_URI`: The URI of your Neo4j database.
- `NEO4J_USER`: Your Neo4j username.
- `NEO4J_PASSWORD`: Your Neo4j password.
- `OPENAI_API_KEY`: Your OpenAI API key.
- `JINA_API_KEY`: Your Jina AI API key.
- `OPENAI_BASE_URL`: Custom base URL for OpenAI API (optional).
- `OPENAI_MODEL`: Specific OpenAI model to use (optional).
- `LLM_PROVIDER`: The LLM provider to use (optional).
- `OPENROUTER_API_KEY`: Your OpenRouter API key (optional).
- `OLLAMA_API_KEY`: Your Ollama API key (optional).

Refer to the README.md file for more details on configuration and usage.
