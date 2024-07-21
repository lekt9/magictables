from typing import Dict
import pandas as pd
import polars as pl
import requests
from urllib.parse import quote_plus
from pdf_reader import get_elements_from_pdf
from .utils import call_ai_model, generate_call_id, flatten_nested_structure
from .magictables import MagicTable


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

    def execute(self, **kwargs) -> pl.DataFrame:
        call_id = generate_call_id(self.name, kwargs)
        cached_result = self.magic_table.get_cached_result(self.name, call_id)

        if cached_result is not None:
            return pl.DataFrame(cached_result)

        if self.source_type == "api":
            result = self._execute_api(**kwargs)
        elif self.source_type == "web":
            result = self._execute_web(**kwargs)
        elif self.source_type == "file":
            result = self._execute_file(**kwargs)
        elif self.source_type == "search":
            result = self._execute_search(**kwargs)
        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")

        self.magic_table.cache_result(
            self.name, call_id, result.to_dict(as_series=False)
        )
        return result

    def _execute_api(self, **kwargs) -> pl.DataFrame:
        route_info = self.magic_table.get_route(
            self.name, kwargs.get("route_name", "default")
        )
        url = route_info["url"].format(**kwargs)
        headers = {"Accept": "application/json"}
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            flattened_data = flatten_nested_structure(data)
            return pl.DataFrame(flattened_data)
        else:
            raise ValueError(
                f"Failed to fetch data from {url}. Status code: {response.status_code}"
            )

    def _execute_web(self, **kwargs) -> pl.DataFrame:
        url = kwargs.get("url")
        if url is None:
            raise ValueError("URL is required for web source type")

        jina_api_url = f"https://r.jina.ai/{url}"
        headers = {"Accept": "application/json"}
        response = requests.get(jina_api_url, headers=headers)

        if response.status_code == 200:
            data = response.json()["data"]
            return pl.DataFrame(
                {
                    "title": [data["title"]],
                    "url": [data["url"]],
                    "content": [data["content"]],
                }
            )
        else:
            raise ValueError(
                f"Failed to fetch content from {url}. Status code: {response.status_code}"
            )

    def _execute_file(self, **kwargs) -> pl.DataFrame:
        file_path = kwargs.get("file_path")
        if file_path is None:
            raise ValueError("file_path is required for file source type")

        file_type = file_path.split(".")[-1].lower()

        if file_type == "csv":
            return pl.read_csv(file_path)
        elif file_type == "json":
            return pl.read_json(file_path)
        elif file_type == "parquet":
            return pl.read_parquet(file_path)
        elif file_type in ["xls", "xlsx"]:
            return pl.from_pandas(pd.read_excel(file_path))
        elif file_type == "pdf":
            pdf_text = get_elements_from_pdf(file_path=file_path)
            return pl.DataFrame(
                {"page": range(1, len(pdf_text) + 1), "content": pdf_text}
            )
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

    def _execute_search(self, **kwargs) -> pl.DataFrame:
        query = kwargs.get("query")
        if query is None:
            raise ValueError("query is required for search source type")

        encoded_query = quote_plus(query)
        jina_search_url = f"https://s.jina.ai/{encoded_query}"
        headers = {"Accept": "application/json"}

        response = requests.get(jina_search_url, headers=headers)

        if response.status_code == 200:
            data = response.json()["data"]
            return pl.DataFrame(
                {
                    "title": [data["title"]],
                    "url": [data["url"]],
                    "content": [data["content"]],
                }
            )
        else:
            raise ValueError(
                f"Failed to fetch search results for '{query}'. Status code: {response.status_code}"
            )

    def _generate_selectors(self, html_subset: str) -> Dict[str, str]:
        input_data = {"html": html_subset}
        prompt = "Analyze the HTML structure and generate CSS selectors for key data elements. Focus on the main content area. Return a JSON object with element names as keys and CSS selectors as values."
        return call_ai_model(input_data, prompt)

    def batch_process(self, batch_size: int, **kwargs) -> pl.DataFrame:
        results = []
        for i in range(0, len(kwargs["data"]), batch_size):
            batch = kwargs["data"][i : i + batch_size]
            batch_kwargs = {**kwargs, "data": batch}
            result = self.execute(**batch_kwargs)
            results.append(result)
        return pl.concat(results)
