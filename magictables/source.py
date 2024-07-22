# source.py
import polars as pl
import requests
from typing import Dict, Any, List, Optional
from .utils import flatten_nested_structure
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
        results = []
        for url in urls:
            result = self.execute(input_data, query, route_name, url)
            results.append(result)
        return pl.concat(results)

    def execute(
        self,
        input_data: Optional[pl.DataFrame],
        query: str,
        route_name: Optional[str] = None,
        url: Optional[str] = None,
    ) -> pl.DataFrame:
        if url is None:
            raise ValueError("URL must be provided for API execution")

        # If input_data is None, create an empty DataFrame
        if input_data is None:
            input_data = pl.DataFrame()

        # Try to find a similar cached result
        cached_result = self.magic_table.find_similar_cached_result(
            self.name, input_data
        )

        if cached_result is not None:
            return pl.DataFrame(cached_result)

        if self.source_type == "api":
            try:
                result = self._execute_api(url, query)
            except Exception as e:
                print(f"Error in _execute_api: {e}")
                return pl.DataFrame()
        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")

        try:
            # Flatten the nested structure and rename duplicate columns
            flattened_data = [flatten_nested_structure(item) for item in result.data]

            # Rename duplicate columns
            all_keys = set()
            for item_list in flattened_data:
                for item in item_list:
                    renamed_item = {}
                    for key, value in item.items():
                        if key in all_keys:
                            count = 1
                            new_key = f"{key}_{count}"
                            while new_key in all_keys:
                                count += 1
                                new_key = f"{key}_{count}"
                            renamed_item[new_key] = value
                        else:
                            renamed_item[key] = value
                        all_keys.add(new_key if key in all_keys else key)
                    item.update(renamed_item)

            # Flatten the list of lists into a single list of dictionaries
            flattened_data = [item for sublist in flattened_data for item in sublist]
            result_df = pl.DataFrame(flattened_data)

            # Handle ID columns
            id_columns = [
                col for col in result_df.columns if col.lower().endswith("id")
            ]
            if len(id_columns) > 1:
                result_df = result_df.with_columns(
                    [
                        pl.concat_str(
                            [result_df[col] for col in id_columns], separator="_"
                        ).alias("merged_id")
                    ]
                )
                result_df = result_df.drop(id_columns)

            result_df = result_df.select(
                [col for col in result_df.columns if col not in INTERNAL_COLUMNS]
            )
            print(result_df, " res")

        except Exception as e:
            print(f"Error converting result to DataFrame: {e}")
            print(f"Result data: {result.data}")
            return pl.DataFrame()

        # Store the contexts and responses
        for context, response in zip(result.contexts, result.responses):
            try:
                self._store_execution_details(context, response)
            except Exception as e:
                print(f"Error storing execution details: {e}")

        # Cache the result
        try:
            identifier = self.magic_table.predict_identifier(result_df)[0]
            self.magic_table.cache_result(
                self.name, str(identifier), result_df.to_dict(as_series=False)
            )
        except Exception as e:
            print(f"Error caching result: {e}")

        # Store the result in the database
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

            # Check if the data is a list of items or a single item
            if isinstance(data, list):
                flattened_data = [flatten_nested_structure(item) for item in data]
            else:
                flattened_data = [flatten_nested_structure(data)]

            # Convert the flattened data to a DataFrame
            df = pl.DataFrame(flattened_data)

            # Handle duplicate column names
            renamed_columns = {}
            for col in df.columns:
                count = 1
                new_col = col
                while new_col in renamed_columns.values():
                    new_col = f"{col}_{count}"
                    count += 1
                renamed_columns[col] = new_col

            df = df.rename(renamed_columns)

            # Expand struct columns
            for col in df.columns:
                if df[col].dtype == pl.Struct:
                    expanded = df[col].struct.unnest()
                    df = df.drop(col).hstack(expanded)

            return ExecutionResult(
                data=df.to_dicts(), contexts=[url], responses=[response]
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
