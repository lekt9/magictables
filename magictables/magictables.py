import asyncio
from typing import Optional, Dict, Any, List, Union
import pandas as pd
import polars as pl
import aiohttp
import json
from datetime import timedelta
from .utils import (
    call_ai_model,
    fetch_url,
    generate_embeddings,
    flatten_nested_structure,
)
from .tablegraph import TableGraph
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
            label,
            df.to_pandas(),
            {
                "source": "local",
            },
            "local",
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
            api_url,
            pd.DataFrame(data),
            {
                "source": "API",
            },
            "API",
        )
        return df

    async def chain(
        self, other: Union["MagicTable", str], query: str = ""
    ) -> "MagicTable":
        if isinstance(other, str):
            other_api_url_template = other
        elif isinstance(other, MagicTable) and hasattr(other, "api_urls"):
            other_api_url_template = other.api_urls[-1]
        else:
            raise ValueError("Unsupported chaining operation")

        api_urls = [
            self._format_url_template(other_api_url_template, row)
            for row in self.iter_rows(named=True)
        ]
        all_results = await self._process_api_batch(api_urls)

        expanded_original_data = []
        new_data = []
        for original_row, result in zip(self.iter_rows(named=True), all_results):
            if isinstance(result, list):
                for item in result:
                    expanded_original_data.append(original_row)
                    new_data.append(item)
            else:
                expanded_original_data.append(original_row)
                new_data.append(result)

        schema = {col: pl.Utf8 for col in self.columns}
        expanded_original_df = pl.DataFrame(
            [
                {
                    k: json.dumps(v) if isinstance(v, (list, dict)) else str(v)
                    for k, v in row.items()
                }
                for row in expanded_original_data
            ],
            schema=schema,
        )

        new_df = pl.DataFrame(
            [
                {
                    k: json.dumps(v) if isinstance(v, (list, dict)) else v
                    for k, v in row.items()
                }
                for row in new_data
            ],
            infer_schema_length=None,
        )

        common_columns = set(expanded_original_df.columns) & set(new_df.columns)
        new_df = new_df.drop(common_columns)
        combined_results = pl.concat([expanded_original_df, new_df], how="horizontal")
        combined_results = combined_results.unique(keep="first")

        result_df = MagicTable(
            combined_results, api_urls=self.api_urls + [other_api_url_template]
        )
        return result_df

    async def _process_api_batch(self, api_urls: List[str]) -> List[Dict[str, Any]]:
        graph = self.get_default_graph()
        stored_data = {}
        urls_to_fetch = []

        nodes_with_urls = graph.get_nodes_batch_with_cache_check(api_urls)

        for url, node in nodes_with_urls:
            if node is not None:
                stored_data[url] = node.row_data  # This is correct now
            else:
                urls_to_fetch.append(url)

        if urls_to_fetch:
            new_results = await asyncio.gather(
                *[fetch_url(url) for url in urls_to_fetch]
            )

            url_data_pairs = []
            for url, data in zip(urls_to_fetch, new_results):
                if data:
                    url_data_pairs.append((url, pd.DataFrame([data])))
                    stored_data[url] = data

            # Use add_tables_batch to add all new data at once
            if url_data_pairs:
                graph.add_tables_batch(url_data_pairs)

        return [stored_data.get(url, {}) for url in api_urls]

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
                query, local_vars["result"], {"source": "generated"}, "generated"
            )
            return result_df
        else:
            raise ValueError("Generated code did not produce a valid DataFrame")
