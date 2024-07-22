# source.py
import polars as pl
import requests
from typing import Dict, Any, List, Optional

from magictables.utils import flatten_nested_structure
from .magictables import INTERNAL_COLUMNS, MagicTable
from dataclasses import dataclass, field


@dataclass
class ExecutionResult:
    data: List[Dict[str, Any]]
    contexts: List[str]
    responses: List[Optional[requests.Response]] = field(default_factory=list)


class Source:
    def __init__(self, name: str, source_type: str):
        self.name = name
        self.source_type = source_type
        self.magic_table = MagicTable.get_instance()

    @classmethod
    def api(cls, name: str):
        return cls(name, "api")

    @classmethod
    def web(cls, name: str):
        return cls(name, "web")

    @classmethod
    def database(cls, name: str):
        return cls(name, "database")

    @classmethod
    def file(cls, name: str):
        return cls(name, "file")

    @classmethod
    def search(cls, name: str):
        return cls(name, "search")

    def add_route(self, route_name: str, url: str, query: str):
        self.magic_table.add_route(self.name, route_name, url, query)

    def execute_batch(
        self, input_data: pl.DataFrame, query: str, route_name: str, urls: pl.Series
    ) -> pl.DataFrame:
        def process_batch(batch: pl.DataFrame) -> pl.DataFrame:
            def make_api_request(url: str) -> Dict[str, Any]:
                result = self._execute_api(url, query)
                print("result", result)
                return result.data[0] if result.data else {}

            results = [make_api_request(url) for url in batch["url"]]
            return pl.DataFrame(results)

        # Convert to LazyFrame and add URL column
        lazy_df = input_data.lazy().with_columns([pl.Series(name="url", values=urls)])

        # Apply map_batches
        result_lazy = lazy_df.map_batches(process_batch)

        # Collect the result
        result_df = result_lazy.collect()

        # Process the results
        for url in urls:
            context = url
            response = (
                None  # We don't have individual responses in this parallel approach
            )
            self._store_execution_details(context, response)

        # Cache and store results
        try:
            identifier = self.magic_table.predict_identifier(result_df)[0]
            self.magic_table.cache_result(
                self.name, str(identifier), result_df.to_dict(as_series=False)
            )
        except Exception as e:
            print(f"Error caching result: {e}")

        try:
            self.magic_table.store_result(self.name, result_df)
        except Exception as e:
            print(f"Error storing result in database: {e}")

        return result_df

    def execute(
        self,
        input_data: Optional[pl.DataFrame],
        query: str,
        route_name: Optional[str] = None,
        url: Optional[str] = None,
    ) -> pl.DataFrame:
        if url is None:
            raise ValueError("URL must be provided for API execution")

        if input_data is None:
            input_data = pl.DataFrame()

        cached_result = self.magic_table.find_similar_cached_result(
            self.name, input_data
        )

        if cached_result is not None:
            return pl.DataFrame(cached_result)

        if self.source_type == "api":
            try:
                result = self._execute_api(url, query)
                result_df = pl.DataFrame(result.data)
                result_df = result_df.select(
                    [col for col in result_df.columns if col not in INTERNAL_COLUMNS]
                )
            except Exception as e:
                print(f"Error in _execute_api: {e}")
                return pl.DataFrame()
        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")

        # Store execution details
        for context, response in zip(result.contexts, result.responses):
            try:
                self._store_execution_details(context, response)
            except Exception as e:
                print(f"Error storing execution details: {e}")

        # Cache and store results
        try:
            identifier = self.magic_table.predict_identifier(result_df)[0]
            self.magic_table.cache_result(
                self.name, str(identifier), result_df.to_dict(as_series=False)
            )
        except Exception as e:
            print(f"Error caching result: {e}")

        try:
            self.magic_table.store_result(self.name, result_df)
        except Exception as e:
            print(f"Error storing result in database: {e}")

        return result_df

    def _execute_api(self, url: str, query: str) -> ExecutionResult:
        try:
            response = requests.get(
                url, headers={"Accept": "application/json"}, timeout=10
            )

            response.raise_for_status()
            data = response.json()

            data = flatten_nested_structure(data)

            return ExecutionResult(
                data=[data] if isinstance(data, dict) else data,
                contexts=[url],
                responses=[response],
            )
        except Exception as e:
            print(f"Error executing API call: {e}")
            return ExecutionResult(data=[], contexts=[url], responses=[])

    def _store_execution_details(self, populated_route: str, response: Any):
        execution_details = {
            "populated_route": populated_route,
            "response": self._serialize_response(response),
        }
        self.magic_table.store_execution_details(self.name, execution_details)

    def _serialize_response(self, response: Any) -> Dict[str, Any]:
        if isinstance(response, requests.Response):
            return {
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "content": response.text,
            }
        elif isinstance(response, dict):
            return response
        elif isinstance(response, list):
            return {"items": response}
        else:
            return {"content": str(response)}
