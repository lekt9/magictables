import json
import random

import asyncio
import logging
from typing import Any, Dict, List, Union

import aiohttp
import pandas as pd

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
    async def from_source(
        cls, source: Union[RawSource, APISource, WebSource, PDFSource]
    ) -> "MagicTable":
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
        return template.format(**{k: row.get(k, f"{{{k}}}") for k in row.keys()})

    def summary(self):
        return f"DataFrame: {len(self.to_pandas())} rows x {len(self.to_pandas().columns)} columns. Columns: {', '.join(self.to_pandas().columns)}. Types: {dict(self.to_pandas().dtypes)}. First row: {dict(zip(self.columns,self.row(0)))}"

    async def transform(self, query: str) -> "MagicTable":
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
                [], prompt, return_json=False  # , model="openai/gpt-4o"
            )

            if not code:
                raise ValueError("Failed to generate transformation code")

            graph.transformations[f"{self.name}_{query}"] = code
            graph._pickle_state()  # Save the state after adding a transformation

        df = self.to_pandas()
        logging.debug(df)
        local_vars = {"pd": pd, "df": df}
        logging.debug(code)

        try:
            exec(code, {"pd": pd, "df": df}, local_vars)
        except Exception as e:
            logging.error(f"Error executing transformation code: {str(e)}")
            raise ValueError(f"Error executing transformation code: {str(e)}")

        if "result" in local_vars and isinstance(local_vars["result"], pd.DataFrame):
            result_df = pl.from_pandas(local_vars["result"])

            # Create a new GenerativeSource for the transformed data
            generative_source = GenerativeSource(query, self.name)
            new_sources = self.sources + [generative_source]

            result = self._from_existing_data(result_df, new_sources)

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
        else:
            raise ValueError("Generated code did not produce a valid DataFrame")

    async def chain(self, other: Union["MagicTable", str]) -> "MagicTable":
        graph = self.get_default_graph()
        if isinstance(other, str):
            other_api_url_template = other
            new_sources = self.sources + [APISource(other)]
        elif isinstance(other, MagicTable) and other.sources:
            other_api_url_template = other.sources[-1].get_identifier()
            new_sources = self.sources + other.sources
        else:
            raise ValueError(
                "Invalid input for chaining. Expected MagicTable with API URL or API URL template string."
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

        # Identify key columns for API URL template
        key_columns = await self._identify_key_columns(other_api_url_template)

        # Prepare API URLs for each row
        api_urls = [
            self._format_url_template(other_api_url_template, row)
            for row in self.select(key_columns).iter_rows(named=True)
        ]

        # Process all API calls in a single batch
        all_results = await self._process_api_batch_single_query(api_urls)

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

        # Create DataFrames from the expanded data
        expanded_original_df = pl.DataFrame(expanded_original_data)
        new_df = pl.DataFrame(new_data)

        # Combine results with expanded original data
        left_columns = set(expanded_original_df.columns)
        right_columns = set(new_df.columns)

        # Find the common columns
        common_columns = left_columns.intersection(right_columns)

        # Remove common columns from the right DataFrame
        new_df_unique = new_df.drop(common_columns)

        # Concatenate the DataFrames horizontally
        combined_results = pl.concat(
            [expanded_original_df, new_df_unique], how="horizontal"
        )

        # Create a new MagicTable with combined results
        result_df = self._from_existing_data(combined_results, new_sources)

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

    async def _identify_key_columns(self, api_url_template: str) -> List[str]:
        """
        Identify the most suitable key columns for the given API URL template.

        :param api_url_template: The API URL template to analyze.
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

        # Call the AI model to identify key columns
        response = await call_ai_model([], prompt, return_json=True)
        key_columns = response.get("column_names", [])

        if not key_columns or any(col not in self.columns for col in key_columns):
            raise ValueError(
                f"Unable to identify suitable key columns for the given API URL template: {api_url_template}"
            )

        return key_columns
