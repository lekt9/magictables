import asyncio
import logging
from typing import Optional, Dict, Any, List, Union
import pandas as pd
import polars as pl
import aiohttp
from datetime import timedelta
from .utils import (
    call_ai_model,
    generate_embeddings,
    flatten_nested_structure,
)
from .tablegraph import TableGraph, Node
from .prompts import TRANSFORM_PROMPT, GENERATE_DATAFRAME_PROMPT


class MagicTable(pl.DataFrame):
    _graph: TableGraph = None
    _embedding: List[float] = None

    @classmethod
    def set_graph(cls, graph: TableGraph):
        cls._graph = graph

    @classmethod
    def get_default_graph(cls):
        if cls._graph is None:
            cls._graph = TableGraph(backend="memory")
            cls._graph._unpickle_state()  # Load the previous state if it exists
        return cls._graph

    def summary(self):
        return f"DataFrame: {len(self.to_pandas())} rows x {len(self.to_pandas().columns)} columns. Columns: {', '.join(self.to_pandas().columns)}. Types: {dict(self.to_pandas().dtypes)}. First row: {dict(zip(self.columns,self.row(0)))}"

    @classmethod
    def set_cache_expiry(cls, expiry: timedelta):
        cls.get_default_graph().cache_expiry = expiry

    def __init__(self, *args, **kwargs):
        self.api_urls = kwargs.pop("api_urls", [])
        super().__init__(*args, **kwargs)
        self.get_default_graph()

    @property
    def name(self):
        return str(
            hash("_".join(self.columns) if len(self.columns) > 0 else f"df_{id(self)}")
        )

    def _format_url_template(self, template: str, row: Dict[str, Any]) -> str:
        return template.format(**{k: row.get(k, f"{{{k}}}") for k in row.keys()})

    @property
    async def embedding(self) -> List[float]:
        if self._embedding is None:
            # Generate a sample of the data for embedding
            sample_size = min(100, len(self))  # Adjust sample size as needed
            sample_data = self.sample(n=sample_size, seed=42)

            # Convert sample data to string representation
            sample_str = sample_data.to_pandas().to_string(index=False, max_rows=20)

            # Generate embedding from the sample data
            self._embedding = (await generate_embeddings([sample_str]))[0]
        return self._embedding

    def __getattribute__(self, name):
        # Override to handle async property
        attr = super().__getattribute__(name)
        if name == "embedding" and callable(attr):
            import asyncio

            return asyncio.get_event_loop().run_until_complete(attr())
        return attr

    @classmethod
    async def from_polars(cls, df: pl.DataFrame, label: str) -> "MagicTable":
        magic_df = cls(df)

        cls.get_default_graph().add_table(
            table_name=label,
            df=df.to_pandas(),
            metadata={"source": "local"},
            source="local",
        )
        return magic_df

    @classmethod
    async def from_api(
        cls, api_url: str, params: Optional[Dict[str, Any]] = None
    ) -> "MagicTable":
        graph = cls.get_default_graph()
        existing_data = graph.query_or_fetch(api_url, params)

        if existing_data:
            df = pd.DataFrame([node.row_data for node in existing_data])
            return cls(pl.from_pandas(df))

        async with aiohttp.ClientSession() as session:
            async with session.get(api_url, params=params) as response:
                data = await response.json()

        data = flatten_nested_structure(data)
        df = cls(pl.DataFrame(data))
        graph.add_table(
            table_name=api_url,
            df=pd.DataFrame(data),
            metadata={"source": "API"},
            source="API",
        )
        return df

    async def transform(self, query: str) -> "MagicTable":
        graph = self.get_default_graph()
        cached_transformation = graph.get_transformation(self.name, query)

        if cached_transformation:
            code = cached_transformation["code"]
        else:
            prompt = TRANSFORM_PROMPT.format(
                data=self.summary(),
                query=query,
            )
            code = await call_ai_model(
                [],
                prompt,
                return_json=False,
            )

            if not code:
                raise ValueError("Failed to generate transformation code")

            graph.add_transformation(self.name, query, code)
            graph._pickle_state()  # Save the state after adding a transformation

        df = self.to_pandas()
        local_vars = {"pd": pd, "df": df}
        exec(code, globals(), local_vars)

        if "result" in local_vars and isinstance(local_vars["result"], pd.DataFrame):
            result = MagicTable(pl.from_pandas(local_vars["result"]))
            return result
        else:
            raise ValueError("Generated code did not produce a valid DataFrame")

    async def chain(self, other: Union["MagicTable", str]) -> "MagicTable":
        if isinstance(other, str):
            other_api_url_template = other
        elif isinstance(other, MagicTable) and other.api_urls:
            other_api_url_template = other.api_urls[-1]
        else:
            raise ValueError(
                "Invalid input for chaining. Expected MagicTable with API URL or API URL template string."
            )

        # Prepare API URLs for each row
        api_urls = [
            self._format_url_template(other_api_url_template, row)
            for row in self.iter_rows(named=True)
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
        combined_results = pl.concat([expanded_original_df, new_df], how="horizontal")

        # Create a new MagicTable with combined results
        result_df = MagicTable(combined_results)
        result_df.api_urls = self.api_urls + [other_api_url_template]

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
            nodes = graph.query_or_fetch(url)
            if nodes:
                stored_data[url] = [node.row_data for node in nodes]

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
                    nodes = [
                        Node(
                            node_id=f"{url}_{i}",
                            table_name=url,
                            row_data=item,
                            metadata={"source": "API"},
                            source="API",
                        )
                        for i, item in enumerate(data)
                    ]
                    graph.add_nodes_batch(nodes)
                    stored_data[url] = data

        # Prepare final results in the order of input api_urls
        all_results = [stored_data.get(url, []) for url in api_urls]

        return all_results

    async def execute_cypher(
        self, query: str, params: Dict[str, Any] = {}
    ) -> "MagicTable":
        result = await self._graph.execute_cypher(query, params)
        return MagicTable(pl.DataFrame(result))

    @classmethod
    async def gen(cls, query: str) -> "MagicTable":
        graph = cls.get_default_graph()
        existing_data = graph.query_or_fetch(query)

        if existing_data:
            df = pd.DataFrame([node.row_data for node in existing_data])
            return cls(pl.from_pandas(df))

        prompt = GENERATE_DATAFRAME_PROMPT.format(query=query)
        code = await call_ai_model([], prompt, return_json=False)

        if not code:
            raise ValueError("Failed to generate DataFrame creation code")

        local_vars = {"pd": pd}
        exec(code, globals(), local_vars)

        if "result" in local_vars and isinstance(local_vars["result"], pd.DataFrame):
            result_df = cls(pl.from_pandas(local_vars["result"]))
            graph.add_table(
                table_name=query,
                df=local_vars["result"],
                metadata={"source": "generated"},
                source="generated",
            )
            return result_df
        else:
            raise ValueError("Generated code did not produce a valid DataFrame")
