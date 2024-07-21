from typing import Dict, Any, List, Union
from .utils import call_ai_model, generate_call_id
from .shadow_db import ShadowDB
import polars as pl
import requests
import os
from pdf_reader import get_elements_from_pdf


class Source:
    def __init__(self, name: str, source_type: str, query: str = None):
        self.name = name
        self.source_type = source_type
        self.query = query
        self.shadow_db = ShadowDB.get_instance()

    @classmethod
    def api(cls, name: str):
        return cls(name, "api")

    @classmethod
    def pdf(cls, name: str, query: str = None):
        return cls(name, "pdf", query)

    @classmethod
    def sql(cls, name: str, query: str = None):
        return cls(name, "sql", query)

    @classmethod
    def csv(cls, name: str, query: str = None):
        return cls(name, "csv", query)

    def add_route(self, route_name: str, url: str, query: str):
        self.shadow_db.add_route(self.name, route_name, url, query)

    def route(self, route_name: str) -> "Source":
        route_info = self.shadow_db.get_route(self.name, route_name)
        return Source.api(f"{self.name}_{route_name}", **route_info)

    def _derive_mapping(self, **kwargs):
        mapping = self.shadow_db.get_mapping(self.name)
        if mapping is None:
            input_data = {
                "source_type": self.source_type,
                "name": self.name,
                "query": self.query,
                "kwargs": kwargs,
            }
            prompt = f"Derive an appropriate mapping for a {self.source_type} source named '{self.name}' with the given parameters and query: {self.query}"
            mapping = self._call_ai_model(input_data, prompt)
            self.shadow_db.save_mapping(self.name, mapping)
        return mapping

    def execute(self, **kwargs):
        call_id = generate_call_id(self.name, kwargs)
        cached_result = self.shadow_db.get_cached_result(self.name, call_id)

        if cached_result is not None:
            return pl.DataFrame(cached_result)

        mapping = self._derive_mapping(**kwargs)

        if self.source_type == "api":
            result = self._execute_api(mapping, **kwargs)
        elif self.source_type == "pdf":
            result = self._execute_pdf(mapping, **kwargs)
        elif self.source_type == "sql":
            result = self._execute_sql(mapping, **kwargs)
        elif self.source_type == "csv":
            result = self._execute_csv(mapping, **kwargs)
        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")

        result_dict = result.to_dict(as_series=False)
        self.shadow_db.cache_result(self.name, call_id, result_dict)
        return result

    def _execute_api(self, mapping: Dict[str, Any], **kwargs) -> pl.DataFrame:
        url = mapping["url"].format(**kwargs)
        response = requests.get(url)
        data = response.json()
        return pl.DataFrame(self._extract_data(data, mapping["data_path"]))

    def _execute_pdf(self, mapping: Dict[str, Any], **kwargs) -> pl.DataFrame:
        pdf_path = kwargs.get("pdf_path") or mapping.get("file_path")
        if os.path.isdir(pdf_path):
            results = []
            for file in os.listdir(pdf_path):
                if file.endswith(".pdf"):
                    file_path = os.path.join(pdf_path, file)
                    results.extend(self._process_single_pdf(file_path))
        else:
            results = self._process_single_pdf(pdf_path)

        return self._convert_elements_to_dataframe(results)

    def _process_single_pdf(self, pdf_path: str) -> List[Dict[str, Any]]:
        elements = get_elements_from_pdf(pdf_path)
        return elements

    def _convert_elements_to_dataframe(
        self, elements: List[Dict[str, Any]]
    ) -> pl.DataFrame:
        data = []
        for element in elements:
            if element["type"] == "table":
                for row in element["content"]:
                    data.append(
                        {
                            "type": "table",
                            "page": element["page"],
                            "content": " | ".join(str(cell) for cell in row),
                        }
                    )
            elif element["type"] == "paragraph":
                data.append(
                    {
                        "type": "paragraph",
                        "page": element["page"],
                        "content": element["content"],
                    }
                )

        return pl.DataFrame(data)

    def _execute_sql(self, mapping: Dict[str, Any], **kwargs) -> pl.DataFrame:
        query = mapping["query"].format(**kwargs)
        result = self.shadow_db.execute_sql(query)
        return pl.DataFrame(result)

    def _execute_csv(self, mapping: Dict[str, Any], **kwargs) -> pl.DataFrame:
        csv_path = kwargs.get("csv_path") or mapping.get("file_path")
        return pl.read_csv(csv_path)

    def _extract_data(
        self, data: Union[Dict, List], data_path: List[str]
    ) -> Union[Dict, List]:
        for key in data_path:
            if isinstance(data, dict):
                data = data.get(key, {})
            elif isinstance(data, list) and key.isdigit():
                index = int(key)
                data = data[index] if 0 <= index < len(data) else {}
            else:
                return {}
        return data

    def _call_ai_model(self, input_data: Dict[str, Any], prompt: str) -> Dict[str, Any]:
        base_prompt = f"""
        As an AI assistant for MagicTables, your task is to derive an appropriate mapping for a {self.source_type} source named '{self.name}' with the following details:

        - Source type: {self.source_type}
        - Query: {self.query}
        - Additional parameters: {input_data['kwargs']}

        Your goal is to generate a mapping that allows MagicTables to:
        1. Efficiently retrieve data from the source
        2. Parse and structure the data appropriately
        3. Enable automatic caching in the shadowdatabase

        Provide a JSON response with the necessary fields for this source type. Include:
        - URL or file path (if applicable)
        - Data extraction path or query
        - Any additional parameters needed for data retrieval and parsing

        Ensure your response is optimized for automatic data caching and retrieval.
        """

        if self.source_type == "api":
            base_prompt += "\nFor API sources, include the URL template and how to navigate the JSON response."
        elif self.source_type == "pdf":
            base_prompt += "\nFor PDF sources, specify file path and any specific content locators if needed."
        elif self.source_type == "sql":
            base_prompt += "\nFor SQL sources, provide a query template that can be easily parameterized."
        elif self.source_type == "csv":
            base_prompt += "\nFor CSV sources, include the file path and any specific column selections."

        # Call the AI model
        ai_response = call_ai_model(input_data, base_prompt)

        # Validate and process the mapping based on source type
        processed_mapping = self._process_mapping(ai_response)

        return processed_mapping

    def _process_mapping(self, mapping: Dict[str, Any]) -> Dict[str, Any]:
        required_fields = ["file_path"]
        if self.source_type == "api":
            required_fields = ["url", "data_path"]
        elif self.source_type == "pdf":
            required_fields = ["file_path"]
        elif self.source_type == "sql":
            required_fields = ["query"]
        elif self.source_type == "csv":
            required_fields = ["file_path", "columns"]

        for field in required_fields:
            if field not in mapping:
                raise ValueError(
                    f"Missing required field '{field}' in AI-generated mapping"
                )

        # Additional processing based on source type
        if self.source_type == "api":
            mapping["url"] = mapping["url"].format(**self.query)
        elif self.source_type == "pdf":
            if not isinstance(mapping["file_path"], str):
                raise ValueError("PDF file path should be a string")
        elif self.source_type == "sql":
            # Ensure the query is parameterized
            if "{" not in mapping["query"] or "}" not in mapping["query"]:
                raise ValueError("SQL query should be parameterized")

        return mapping
