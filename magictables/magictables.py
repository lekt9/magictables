import logging
from dateparser import parse
import asyncio
import pandas as pd
import polars as pl
from typing import Optional, Dict, Any, List, Tuple, Union
import aiohttp
import json
from .graph_interface import GraphInterface
from .utils import call_ai_model, generate_embeddings, flatten_nested_structure
from .tablegraph import TableGraph

class MagicTable(pl.DataFrame):
    _graph: GraphInterface = None  # Class variable to hold the graph instance

    @classmethod
    def set_graph(cls, graph: GraphInterface):
        cls._graph = graph

    @classmethod
    def get_default_graph(cls):
        if cls._graph is None:
            # Create a default in-memory graph
            cls._graph = TableGraph(backend="memory")
        return cls._graph

    def __init__(self, *args, **kwargs):
        self.api_urls = kwargs.pop('api_urls', [])  # Extract api_urls before passing to super()
        super().__init__(*args, **kwargs)
        self.name = kwargs.get('name', f'df_{id(self)}')
        # Ensure a graph is set
        self.get_default_graph()

    def _format_url_template(self, template: str, row: Dict[str, Any]) -> str:
        return template.format(**{k: row.get(k, f'{{{k}}}') for k in row.keys()})
    @classmethod
    async def from_polars(cls, df: pl.DataFrame, label: str) -> "MagicTable":
        magic_df = cls(df, name=label)
        api_url = f"local://{label}"
        description = f"Local DataFrame: {label}"
        embedding = await generate_embeddings([description])
        cls.get_default_graph().add_dataframe(label, df, api_url)
        return magic_df

    @classmethod
    async def from_api(cls, api_url: str, params: Optional[Dict[str, Any]] = None) -> "MagicTable":
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url, params=params) as response:
                data = await response.json()

        data = flatten_nested_structure(data)
        df = cls(pl.DataFrame(data))  # Remove the name parameter here
        df.name = api_url  # Set the name attribute after creation
        cls.get_default_graph().add_dataframe(api_url, df, "API")
        return df

    async def chain(self, other: Union["MagicTable", str]) -> "MagicTable":
        if isinstance(other, str):
            other_api_url_template = other
        elif isinstance(other, MagicTable) and hasattr(other, 'api_urls'):
            other_api_url_template = other.api_urls[-1]
        else:
            return await self._graph_based_chain(other)

        print(f"URL template: {other_api_url_template}")
        print(f"Columns in DataFrame: {self.columns}")
        
        # Prepare API URLs for each row
        api_urls = []
        for row in self.iter_rows(named=True):
            try:
                url = self._format_url_template(other_api_url_template, row)
                api_urls.append(url)
            except KeyError as e:
                print(f"KeyError for row: {row}")
                print(f"Missing key: {e}")
                raise

        # Process all API calls in a single batch
        all_results = await self._process_api_batch(api_urls)

        # Create a list to store the expanded original data
        expanded_original_data = []

        # Create a list to store the new data
        new_data = []
        # Iterate through the original data and the results
        for original_row, result in zip(self.iter_rows(named=True), all_results):
            if isinstance(result, list):
                # If the result is a list (multiple rows returned)
                for item in result:
                    expanded_original_data.append(original_row)
                    new_data.append(item)
            else:
                # If the result is a single item
                expanded_original_data.append(original_row)
                new_data.append(result)

        # Create DataFrames from the expanded data with explicit schema
        schema = {col: pl.Utf8 for col in self.columns}  # Assume all columns are strings
        expanded_original_df = pl.DataFrame(
            [{k: json.dumps(v) if isinstance(v, (list, dict)) else str(v) for k, v in row.items()}
            for row in expanded_original_data],
            schema=schema
        )
        
        # Infer schema for new_data with a larger sample size
        new_df = pl.DataFrame(
            [{k: json.dumps(v) if isinstance(v, (list, dict)) else v for k, v in row.items()}
            for row in new_data],
            infer_schema_length=None
        )

        # Find common columns
        common_columns = set(expanded_original_df.columns) & set(new_df.columns)

        # Exclude common columns from new_df
        new_df = new_df.drop(common_columns)

        # Combine results with expanded original data
        combined_results = pl.concat([expanded_original_df, new_df], how="horizontal")

        # Drop duplicate rows
        combined_results = combined_results.unique(keep="first")

        # Create a new MagicTable with combined results
        result_df = MagicTable(combined_results, api_urls=self.api_urls + [other_api_url_template])
        return result_df

    async def _graph_based_chain(self, target_df: "MagicTable", query: str) -> "MagicTable":
        source_df, target_df = await self._graph.apply_chaining(self.name, target_df.name, query)
        return MagicTable(source_df, name=f"{self.name}_chained")

    async def _process_api_batch(self, api_urls: List[str]) -> List[Dict[str, Any]]:
        async def fetch_url(url: str):
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        return None

        tasks = [fetch_url(url) for url in api_urls]
        results = await asyncio.gather(*tasks)
        return [flatten_nested_structure(result) if result else {} for result in results]

    def get_transformation_suggestions(self, column: str) -> List[str]:
        transformations = self._graph.get_transformations(self.name)
        if column in transformations:
            return [info["query"] for info in transformations[column].values()]
        return []

    def get_chaining_suggestions(self, target_df: "MagicTable") -> List[str]:
        return self._graph.get_chaining_suggestions(self.name, target_df.name)

    async def find_similar_dataframes(self, top_k: int = 5) -> List[Tuple[str, float]]:
        return await self._graph.find_similar_dataframes(self.name, top_k)

    async def execute_cypher(self, query: str, params: Dict[str, Any] = {}) -> "MagicTable":
        result = await self._graph.execute_cypher(query, params)
        return MagicTable(result)

    @classmethod
    async def gen(cls, query: str) -> "MagicTable":
        prompt = f"""
        Generate Python code to create a pandas DataFrame using iterators or generators based on the following natural language query:

        "{query}"

        The code should use pandas to efficiently create the DataFrame.
        Only import and use pandas (as pd). Do not use any other libraries.

        If the query mentions or implies the need for pagination or fetching data from an API,
        generate query parameter pairs based on the URL template, focusing on pagination requirements.

        Examples:

        1. Query: "Fetch top 100 movies from TMDB API with title, release date, and rating. Use this URL template: https://api.themoviedb.org/3/movie/top_rated?api_key={{api_key}}&page={{page}}"
        Expected output:

        import pandas as pd

        def generate_query_params():
            for page in range(1, 6):  # Assuming 20 movies per page, 5 pages for 100 movies
                yield {{
                    'api_key': 'dummy_api_key',
                    'page': page
                }}

        df = pd.DataFrame(generate_query_params())

        2. Query: "Fetch weather data for New York for the first week of January 2024, using a weather API with pagination. Use this URL template: https://api.example.com/weather?city={{city}}&date={{date}}&page={{page}}"
        Expected output:

        import pandas as pd

        def generate_query_params():
            city = 'New York'
            date_range = pd.date_range(start='2024-01-01', end='2024-01-07')
            for date in date_range:
                yield {{
                    'city': city,
                    'date': date.strftime('%Y-%m-%d'),
                    'page': 1  # Assuming one page per day
                }}

        df = pd.DataFrame(generate_query_params())

        Your response should be the Python code directly, without any JSON formatting.
        """
        code = await call_ai_model([], prompt, return_json=False)

        if not code:
            raise ValueError("Failed to generate DataFrame code")

        # Execute the generated code
        local_vars = {"pd": pd}
        exec(code, globals(), local_vars)

        if "df" in local_vars and isinstance(local_vars["df"], pd.DataFrame):
            # Convert pandas DataFrame to polars DataFrame
            logging.debug(local_vars["df"])
            return cls(pl.from_pandas(local_vars["df"]))
        else:
            raise ValueError("Generated code did not produce a valid DataFrame")
    def _parse_json_fields(self) -> "MagicTable":
        for column in self.columns:
            if self[column].dtype == pl.Utf8:
                try:
                    parsed = pl.Series(
                        name=column,
                        values=[json.loads(x) if x else None for x in self[column]],
                    )
                    if isinstance(parsed[0], (list, dict)):
                        self = self.with_columns([parsed])
                except:
                    pass
        return MagicTable(self)

    def _standardize_data_types(self, data: Dict[str, Any]) -> Dict[str, Any]:
        standardized_data = {}
        for key, value in data.items():
            if isinstance(value, str):
                # Attempt to parse as date
                try:
                    parsed_date = parse(value)
                    if parsed_date:
                        standardized_data[key] = parsed_date.isoformat()
                    else:
                        standardized_data[key] = value.strip()
                except:
                    standardized_data[key] = value.strip()
            elif isinstance(value, (int, float)):
                standardized_data[key] = float(value)
            elif isinstance(value, list):
                # Convert list to a string representation
                standardized_data[key] = json.dumps(value)
            elif isinstance(value, dict):
                # Convert dict to a string representation
                standardized_data[key] = json.dumps(value)
            else:
                standardized_data[key] = str(value)
        return standardized_data
    
    async def transform(self, query: str) -> "MagicTable":
        # First, get transformation suggestions for all columns
        all_suggestions = []
        for column in self.columns:
            suggestions = self.get_transformation_suggestions(column)
            all_suggestions.extend(suggestions)

        # Get all transformations
        transformations = self._graph.get_transformations(self.name)

        prompt = f"""
        Given the following DataFrame structure:
        Columns: {self.columns}
        Data Types: {self.dtypes}

        And the following transformation query:
        "{query}"

        Consider these existing transformation suggestions:
        {all_suggestions}

        And these available transformations:
        {transformations}

        Generate Python code to transform this DataFrame using pandas.
        The code should:
        1. Use only pandas (as pd) operations.
        2. Be efficient and use vectorized operations where possible.
        3. Handle potential data type issues.
        4. Return a new pandas DataFrame as the result.
        5. Not include any print statements or comments.
        6. Name the input DataFrame 'df' and the output DataFrame 'result'.
        7. Utilize the existing transformations where applicable.

        Here are some examples of well-structured transformations:

        Example 1 (Date manipulation and filtering):
        result = df.copy()
        result['date'] = pd.to_datetime(result['date'], errors='coerce')
        result['year'] = result['date'].dt.year
        result['month'] = result['date'].dt.month
        result = result[result['year'] >= 2020]

        Example 2 (String operations and categorical encoding):
        result = df.copy()
        result['name'] = result['name'].str.lower().str.strip()
        result['category'] = pd.Categorical(result['category'])
        result['category_code'] = result['category'].cat.codes

        Example 3 (Numeric operations and aggregation):
        result = df.copy()
        result['price'] = pd.to_numeric(result['price'], errors='coerce')
        result['discounted_price'] = result['price'] * 0.9
        result = result.groupby('category').agg({
            'price': 'mean',
            'discounted_price': 'mean',
            'quantity': 'sum'
        }).reset_index()

        Your response should be the Python code directly, without any JSON formatting.
        """
        
        code = await call_ai_model([], prompt, return_json=False)
        
        if not code:
            raise ValueError("Failed to generate transformation code")

        # Convert MagicTable to pandas DataFrame
        df = self.to_pandas()

        # Execute the generated code
        local_vars = {"pd": pd, "df": df}
        exec(code, globals(), local_vars)

        if "result" in local_vars and isinstance(local_vars["result"], pd.DataFrame):
            # Convert the result back to a MagicTable
            result = MagicTable(pl.from_pandas(local_vars["result"]))
            
            # Update the graph with the new transformation
            self._graph.add_transformation(self.name, query, code)
            
            return result
        else:
            raise ValueError("Generated code did not produce a valid DataFrame")