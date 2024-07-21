# source.py
import pandas as pd
from .utils import call_ai_model, generate_call_id
from .magictables import MagicTable
import polars as pl
import requests
from bs4 import BeautifulSoup
from pdf_reader import get_elements_from_pdf


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

    def add_route(self, route_name: str, url: str, query: str):
        self.magic_table.add_route(self.name, route_name, url, query)

    def execute(self, **kwargs):
        call_id = generate_call_id(self.name, kwargs)
        cached_result = self.magic_table.get_cached_result(self.name, call_id)

        if cached_result is not None:
            return cached_result

        if self.source_type == "api":
            result = self._execute_api(**kwargs)
        elif self.source_type == "web":
            result = self._execute_web(**kwargs)
        elif self.source_type == "file":
            result = self._execute_file(**kwargs)
        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")

        self.magic_table.cache_result(self.name, call_id, result)
        return result

    def _execute_api(self, **kwargs):
        route_info = self.magic_table.get_route(
            self.name, kwargs.get("route_name", "default")
        )
        url = route_info["url"].format(**kwargs)
        response = requests.get(url)
        data = response.json()
        return pl.DataFrame([data])

    def _execute_web(self, **kwargs):
        url = kwargs.get("url")
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        # Use AI to generate selectors
        selectors = call_ai_model(
            {"html": str(soup)}, "Generate CSS selectors for key data elements"
        )

        data = {}
        for key, selector in selectors.items():
            elements = soup.select(selector)
            data[key] = [element.text.strip() for element in elements]

        return pl.DataFrame(data)

    def _execute_file(self, **kwargs):
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

            # Convert the extracted text to a DataFrame
            # You might want to adjust this based on your specific needs
            return pl.DataFrame(
                {"page": range(1, len(pdf_text) + 1), "content": pdf_text}
            )
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
