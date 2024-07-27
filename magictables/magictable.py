import json
import random

import asyncio
import logging
from typing import Any, Dict, List, Optional, Union

import aiohttp
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import re
from magictables.prompts import IDENTIFY_KEY_COLUMNS_PROMPT, TRANSFORM_PROMPT
from magictables.utils import call_ai_model, flatten_nested_structure
from .sources import (
    BaseSource,
    GenerativeSource,
    PDFSource,
    RawSource,
    APISource,
    WebSource,
)
import polars as pl
from .magictablechain import MagicTableChain
from .tablegraph import TableGraph

from functools import wraps


def wrap_polars_method(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        result = method(self, *args, **kwargs)
        if isinstance(result, pl.DataFrame):
            return MagicTable._from_existing_data(result, self.sources)
        return result

    return wrapper


class MagicTable(pl.DataFrame):
    _graph: TableGraph = None
    name: str = None
    sources: List[BaseSource] = []

    @classmethod
    def set_graph(cls, graph: TableGraph):
        cls._graph = graph

    @classmethod
    def get_default_graph(cls):
        if cls._graph is None:
            cls._graph = TableGraph(backend="memory")
            cls._graph._unpickle_state()  # Load the previous state if it exists
        return cls._graph

    @classmethod
    async def from_source(cls, source: BaseSource) -> "MagicTable":
        graph = cls.get_default_graph()
        source_id = source.get_id()
        existing_data = graph.query_or_fetch(source_id)

        if existing_data:
            df = pl.DataFrame(existing_data)
            return cls._from_existing_data(df, [source])

        data = await source.fetch_data()
        df = pl.DataFrame(data)
        return cls._from_existing_data(df, [source])

    @classmethod
    async def from_api(
        cls, api_url: str, params: Optional[Dict[str, Any]] = None
    ) -> "MagicTable":
        source = APISource(api_url, params)
        return await cls.from_source(source)

    @classmethod
    async def from_web(cls, url: str) -> "MagicTable":
        source = WebSource(url)
        return await cls.from_source(source)

    @classmethod
    async def from_pdf(cls, pdf_url: str) -> "MagicTable":
        source = PDFSource(pdf_url)
        return await cls.from_source(source)

    @classmethod
    async def from_gen(cls, query: str) -> "MagicTable":
        source = GenerativeSource(query)
        return await cls.from_source(source)

    @classmethod
    async def from_raw(cls, data: List[Dict[str, Any]]) -> "MagicTable":
        source = RawSource(data)
        return await cls.from_source(source)

    @classmethod
    def _from_existing_data(
        cls, df: pl.DataFrame, sources: List[BaseSource]
    ) -> "MagicTable":
        magic_table = cls(df)
        magic_table.sources = sources
        magic_table.name = "_".join([source.get_id() for source in sources])

        cls.get_default_graph().add_table(
            magic_table.name,
            df,
            metadata={"sources": [source.get_id() for source in sources]},
            source_info=[
                {
                    "type": source.get_type(),
                    "identifier": source.get_identifier(),
                    "params": source.get_params(),
                }
                for source in sources
            ],
        )
        return magic_table

    @staticmethod
    def _format_url_template(template: str, row: Dict[str, Any]) -> str:
        # Parse the URL
        parsed_url = urlparse(template)

        # Parse the query string
        query_params = parse_qs(parsed_url.query)

        # Replace placeholders in the query parameters
        for key, value in query_params.items():
            if len(value) == 1 and value[0].startswith("{") and value[0].endswith("}"):
                param_name = value[0][1:-1]  # Remove curly braces
                if param_name in row:
                    query_params[key] = [str(row[param_name])]

        # Replace placeholders in the path
        path = re.sub(
            r"\{([^{}]+)\}",
            lambda m: str(row.get(m.group(1), m.group(0))),
            parsed_url.path,
        )

        # Reconstruct the URL
        new_query = urlencode(query_params, doseq=True)
        return urlunparse(parsed_url._replace(path=path, query=new_query))

    def summary(self):
        return f"DataFrame: {len(self.to_pandas())} rows x {len(self.to_pandas().columns)} columns. Columns: {', '.join(self.to_pandas().columns)}. Types: {dict(self.to_pandas().dtypes)}. First row: {dict(zip(self.columns,self.row(0)))}"

    async def transform(self, query: str, model="gpt-4o-mini") -> "MagicTable":
        graph = self.get_default_graph()
        cached_transformation = graph.transformations.get(f"{self.name}_{query}")

        if cached_transformation:
            code = cached_transformation
        else:
            prompt = TRANSFORM_PROMPT.format(
                data=self.summary(),
                query=query,
            )
            code = await call_ai_model(
                self.head(30).to_dicts(),
                prompt,
                return_json=False,
                model=model,
            )

            if not code:
                raise ValueError("Failed to generate transformation code")

            graph.transformations[f"{self.name}_{query}"] = code
            graph._pickle_state()  # Save the state after adding a transformation

        print("Generated transformation code:")
        print(code)

        # Execute the generated code
        local_vars = {"df": self.to_pandas()}
        exec(code, globals(), local_vars)
        result_df = local_vars["df"]

        self.name = f"{self.name}_query:{query}_model:{model}"

        # Create a new GenerativeSource for the transformed data
        generative_source = GenerativeSource(self.name)
        new_sources = self.sources + [generative_source]

        result = self._from_existing_data(pl.from_pandas(result_df), new_sources)

        # Create a new MagicTableChain for this transformation
        chain = MagicTableChain(
            source_table=self.name,
            api_result_table=f"{self.name}_transform_result",
            merged_result_table=result.name,
            chain_type="transform",
            source_key="",
            target_key="",
            metadata={"query": query},
        )
        graph.add_chain(chain)

        return result

    async def chain(
        self,
        other: Union["MagicTable", str],
        source_key: Optional[str] = None,
        target_key: Optional[str] = None,
        query: str = "",
    ) -> "MagicTable":
        graph = self.get_default_graph()
        if isinstance(other, str):
            other_api_url_template = other
            new_sources = self.sources + [APISource(other)]
        elif isinstance(other, MagicTable) and other.sources:
            other_api_url_template = other.sources[-1].get_identifier()
            new_sources = self.sources + other.sources
        else:
            raise ValueError(
                "Invalid input for chaining. Expected MagicTable with sources or API URL template string."
            )

        new_id = "_".join([source.get_id() for source in new_sources])

        # Check if we have a cached result
        cached_result = graph.get_cached_chain_result(new_id)
        if cached_result is not None:
            cached_data = graph.get_table(new_id)
            if cached_data:
                return self._from_existing_data(
                    pl.DataFrame(cached_data["df"]), new_sources
                )
        placeholders = []
        for part in other_api_url_template.split("{"):
            if "}" in part:
                placeholder = part.split("}")[0]
                placeholders.append(placeholder)

        api_urls = []
        key_columns = await self._identify_key_columns(other_api_url_template, query)

        if source_key and target_key:
            if source_key in self.columns and target_key in placeholders:
                placeholder_to_column = {target_key: source_key}
            else:
                # If no common column, use query-based key_columns
                placeholder_to_column = dict(zip(placeholders, key_columns))
        else:
            # If source_key and target_key are not provided, use query-based key_columns
            placeholder_to_column = dict(zip(placeholders, key_columns))

        for row in self.iter_rows(named=True):
            url_params = {}
            for placeholder in placeholders:
                if placeholder in placeholder_to_column:
                    column = placeholder_to_column[placeholder]
                    if column in row:
                        url_params[placeholder] = row[column]
                    else:
                        url_params[placeholder] = f"{{{placeholder}}}"
                else:
                    url_params[placeholder] = f"{{{placeholder}}}"

            logging.debug(f"URL template: {other_api_url_template}")
            logging.debug(f"URL params: {url_params}")
            api_urls.append(
                self._format_url_template(other_api_url_template, url_params)
            )
        # Process all API calls in a single batch
        all_results = await self._process_api_batch_single_query(api_urls)

        logging.debug(len(all_results))

        # Create DataFrame from the original data
        expanded_original_df = pl.DataFrame(self.iter_rows(named=True))

        # Create a column with the API results
        expanded_original_df = expanded_original_df.with_columns(
            [pl.Series("api_results", all_results)]
        )

        # Explode the api_results column
        exploded_df = expanded_original_df.explode("api_results")

        # Convert the exploded api_results to struct and then to columns
        api_result_columns = set()
        for result in all_results:
            if isinstance(result, list) and result:
                api_result_columns.update(result[0].keys())
            elif isinstance(result, dict):
                api_result_columns.update(result.keys())

        exploded_df = exploded_df.with_columns(
            [
                pl.col("api_results").struct.rename_fields(
                    [f"api_{col}" for col in api_result_columns]
                )
            ]
        )
        exploded_df = exploded_df.unnest("api_results")

        # Create a new MagicTable with the exploded results
        result_df = self._from_existing_data(exploded_df, new_sources)

        # Add chain between source and result tables
        chain = MagicTableChain(
            self.name,
            result_df.name,
            result_df.name,
            "api_chain",
            ",".join(key_columns),
            "",
            {"api_url_template": other_api_url_template},
        )
        graph.add_chain(chain)

        return result_df

    async def _process_url(self, url: str) -> Dict[str, Any]:
        max_retries = 5
        base_delay = 1

        for attempt in range(max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        if response.status == 429:
                            raise aiohttp.ClientResponseError(
                                response.request_info,
                                response.history,
                                status=429,
                            )
                        response.raise_for_status()
                        data = await response.json()
                        data = flatten_nested_structure(data)
                        logging.debug(f"Fetched data for URL {url}: {data}")
                    return data
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logging.error(f"Error fetching data for URL {url}: {e}")
                if attempt == max_retries - 1:
                    logging.error(f"Last attempt failed for URL {url}")
                    return {}
                else:
                    delay = (2**attempt * base_delay) + (random.randint(0, 1000) / 1000)
                    logging.warning(
                        f"Attempt {attempt + 1} failed for URL {url}. Retrying in {delay:.2f} seconds..."
                    )
                    await asyncio.sleep(delay)
        return {}

    async def _process_api_batch_single_query(
        self, api_urls: List[str]
    ) -> List[Dict[str, Any]]:
        graph = self.get_default_graph()

        # Check for stored results
        stored_data = {}
        for url in api_urls:
            table_data = graph.get_table(url)
            if table_data:
                stored_data[url] = table_data["df"].to_dicts()

        # If all URLs have stored data, return immediately
        if len(stored_data) == len(api_urls):
            return [stored_data[url] for url in api_urls]

        # Identify URLs to fetch
        urls_to_fetch = [url for url in api_urls if url not in stored_data]
        logging.debug(f"URLs to fetch: {urls_to_fetch}")

        if urls_to_fetch:
            # Fetch new results
            new_results = await asyncio.gather(
                *[self._process_url(url) for url in urls_to_fetch]
            )

            # Store new results
            for url, data in zip(urls_to_fetch, new_results):
                if data:
                    if not isinstance(data, list):
                        data = [data]
                    graph.add_table(
                        url,
                        pl.DataFrame(data),
                        metadata={"source": "API"},
                        source_info=[{"type": "API", "identifier": url, "params": {}}],
                    )
                    stored_data[url] = data

        # Prepare final results in the order of input api_urls
        all_results = [stored_data.get(url, []) for url in api_urls]

        return all_results

    async def _identify_key_columns(
        self, api_url_template: str, query: str = ""
    ) -> List[str]:
        """
        Identify the most suitable key columns for the given API URL template.

        :param api_url_template: The API URL template to analyze.
        :param query: Additional query information to provide context.
        :return: A list of names of the identified key columns.
        """
        # Extract placeholders from the API URL template
        placeholders = [p.strip("{}") for p in api_url_template.split("{") if "}" in p]

        # Get column information
        column_info = {
            col: {"dtype": str(dtype), "sample": self[col].head(5).to_list()}
            for col, dtype in zip(self.columns, self.dtypes)
        }

        # Prepare the prompt
        prompt = IDENTIFY_KEY_COLUMNS_PROMPT.format(
            api_url_template=api_url_template,
            placeholders=placeholders,
            column_info=json.dumps(column_info, indent=2),
        )
        if query:
            prompt += f"\nQuery to achieve: {query}"

        # Call the AI model to identify key columns
        response = await call_ai_model([], prompt, return_json=True)

        # Extract the column mapping from the response
        column_mapping = response.get("column_mapping", [])

        # Create a dictionary of placeholder to column mappings, excluding null values
        key_columns_dict = {
            item["placeholder"]: item["column"]
            for item in column_mapping
            if item["column"] is not None
        }

        # Ensure all placeholders are accounted for
        for placeholder in placeholders:
            if placeholder not in key_columns_dict:
                key_columns_dict[placeholder] = placeholder

        # Get the final list of key columns, ensuring they exist in the DataFrame
        key_columns = [col for col in key_columns_dict.values() if col in self.columns]

        if not key_columns:
            raise ValueError(
                f"Unable to identify suitable key columns for the given API URL template: {api_url_template}"
            )

        return key_columns


# Add this function after the class definition
def wrap_magictable_methods():
    for method_name in dir(pl.DataFrame):
        method = getattr(pl.DataFrame, method_name)
        if callable(method) and not method_name.startswith("_"):
            setattr(MagicTable, method_name, wrap_polars_method(method))


# Call the function to wrap the methods
wrap_magictable_methods()
