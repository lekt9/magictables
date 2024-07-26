import random

import asyncio
import logging
from typing import Any, Dict, List, Union

import aiohttp

from magictables.utils import flatten_nested_structure
from .sources import BaseSource, PDFSource, RawSource, APISource, WebSource
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

    async def chain(
        self,
        other: Union["MagicTable", str],
        source_key: str,
        target_key: str,
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

        # Prepare API URLs for each row
        api_urls = [
            self._format_url_template(other_api_url_template, row)
            for row in self.iter_rows(named=True)
        ]

        # Process all API calls in a single batch
        all_results = await self._process_api_batch_single_query(api_urls)

        # Create lists to store the expanded original data and new data
        expanded_original_data = []
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

        # Create a new MagicTable for the API results
        api_result_df = self._from_existing_data(
            new_df,
            new_sources,
        )

        # Combine results with expanded original data
        combined_results = pl.concat([expanded_original_df, new_df], how="horizontal")

        # Create a new MagicTable with combined results
        result_df = self._from_existing_data(
            combined_results,
            new_sources,
        )

        # Add chain between source, API result, and merged result tables
        chain = MagicTableChain(
            self.name,
            api_result_df.name,
            result_df.name,
            "api_chain",
            source_key,
            target_key,
            {"api_url_template": other_api_url_template},
        )
        graph.add_chain(chain)

        return result_df

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

    async def _process_url(self, url: str) -> Dict[str, Any]:
        max_retries = 2
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
