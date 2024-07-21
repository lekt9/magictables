# source.py
import polars as pl
import pandas as pd
import requests
from urllib.parse import quote_plus
from typing import Dict, Any, List, Optional
from .utils import call_ai_model, generate_call_id, flatten_nested_structure
from .magictables import MagicTable
from pdf_reader import get_elements_from_pdf


class Source:
    def __init__(self, name: str, source_type: str):
        self.name = name
        self.source_type = source_type
        self.magic_table = MagicTable.get_instance()
        self.routes: Dict[str, Dict[str, str]] = {}

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
        self.routes[route_name] = {"url": url, "query": query}
        self.magic_table.add_route(self.name, route_name, url, query)

    def execute(
        self, input_data: pl.DataFrame, query: str, route_name: Optional[str] = None
    ) -> pl.DataFrame:
        call_id = generate_call_id(
            self.name,
            {
                "input_data": input_data.to_dict(as_series=False),
                "query": query,
                "route_name": route_name,
            },
        )
        cached_result = self.magic_table.get_cached_result(self.name, call_id)

        if cached_result is not None:
            return pl.DataFrame(cached_result)

        if self.source_type == "api":
            result = self._execute_api(input_data, query, route_name)
        elif self.source_type == "web":
            result = self._execute_web(input_data, query)
        elif self.source_type == "file":
            result = self._execute_file(input_data, query)
        elif self.source_type == "search":
            result = self._execute_search(input_data, query)
        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")

        self.magic_table.cache_result(
            self.name, call_id, result.to_dict(as_series=False)
        )
        return result

    def _execute_api(
        self, input_data: pl.DataFrame, query: str, route_name: Optional[str] = None
    ) -> pl.DataFrame:
        if route_name:
            route_info = self.routes.get(route_name)
            if not route_info:
                raise ValueError(
                    f"No route found for {self.name} with route_name: {route_name}"
                )
            url_template = route_info["url"]
        else:
            input_dict = {
                "input_data": input_data.to_dict(as_series=False),
                "query": query,
                "routes": list(self.routes.keys()),
            }
            prompt = f"""
            Process the input data based on the query: {query}. Determine the API calls to make for {self.name}.
            Return a JSON object with 'route_name' and 'url_template'.

            Available routes: {list(self.routes.keys())}

            Example output:
            {{
                "route_name": "user_info",
                "url_template": "https://api.github.com/users/{username}"
            }}

            Now, based on the actual input data and query provided, generate the appropriate API call parameters:
            """
            api_info = call_ai_model(input_dict, prompt)
            route_name = api_info["route_name"]
            url_template = api_info["url_template"]

        def fetch_api_data(row: Dict[str, Any]) -> Dict[str, Any]:
            try:
                url = url_template.format(**row)
                headers = {"Accept": "application/json"}
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                data = response.json()
                return flatten_nested_structure(data)[0]  # Assuming single row result
            except Exception as e:
                print(f"Error fetching data from URL: {str(e)}")
                return {}

        # Use map to apply fetch_api_data to each row
        first_result = fetch_api_data(input_data.row(0, named=True))

        # Create a schema based on the first result
        schema = {k: type(v) for k, v in first_result.items()}

        results = input_data.select(
            pl.struct(pl.all()).map_elements(fetch_api_data).alias("struct")
        )

        # Expand the struct column into multiple columns
        results = results.unnest("struct")

        # Cast columns to inferred types
        for col, dtype in schema.items():
            if col in results.columns:
                if dtype == str:
                    results = results.with_columns(pl.col(col).cast(pl.Utf8))
                elif dtype == int:
                    results = results.with_columns(pl.col(col).cast(pl.Int64))
                elif dtype == float:
                    results = results.with_columns(pl.col(col).cast(pl.Float64))
                elif dtype == bool:
                    results = results.with_columns(pl.col(col).cast(pl.Boolean))
                else:
                    # For other types, let polars infer the type
                    pass
        # Remove rows where all columns are null
        valid_results = results.drop_nulls()

        return valid_results if not valid_results.is_empty() else pl.DataFrame()

    def _execute_web(self, input_data: pl.DataFrame, query: str) -> pl.DataFrame:
        input_dict = {"input_data": input_data.to_dict(as_series=False), "query": query}
        prompt = f"Process the input data based on the query: {query}. Determine the web pages to scrape for {self.name}. Return a JSON object with the URLs to scrape."
        urls_to_scrape = call_ai_model(input_dict, prompt)

        results = []
        for url in urls_to_scrape:
            jina_api_url = f"https://r.jina.ai/{url}"
            headers = {"Accept": "application/json"}
            response = requests.get(jina_api_url, headers=headers)

            if response.status_code == 200:
                data = response.json()["data"]
                results.append(
                    pl.DataFrame(
                        {
                            "title": [data["title"]],
                            "url": [data["url"]],
                            "content": [data["content"]],
                        }
                    )
                )
            else:
                raise ValueError(
                    f"Failed to fetch content from {url}. Status code: {response.status_code}"
                )

        return pl.concat(results) if results else pl.DataFrame()

    def _execute_file(self, input_data: pl.DataFrame, query: str) -> pl.DataFrame:
        input_dict = {"input_data": input_data.to_dict(as_series=False), "query": query}
        prompt = f"Process the input data based on the query: {query}. Determine the files to read for {self.name}. Return a JSON object with the file paths and types."
        files_to_read = call_ai_model(input_dict, prompt)

        results = []
        for file_info in files_to_read:
            file_path = file_info["path"]
            file_type = file_info["type"]

            if file_type == "csv":
                results.append(pl.read_csv(file_path))
            elif file_type == "json":
                results.append(pl.read_json(file_path))
            elif file_type == "parquet":
                results.append(pl.read_parquet(file_path))
            elif file_type in ["xls", "xlsx"]:
                results.append(pl.from_pandas(pd.read_excel(file_path)))
            elif file_type == "pdf":
                pdf_text = get_elements_from_pdf(file_path=file_path)
                results.append(
                    pl.DataFrame(
                        {"page": range(1, len(pdf_text) + 1), "content": pdf_text}
                    )
                )
            else:
                raise ValueError(f"Unsupported file type: {file_type}")

        return pl.concat(results) if results else pl.DataFrame()

    def _execute_search(self, input_data: pl.DataFrame, query: str) -> pl.DataFrame:
        input_dict = {"input_data": input_data.to_dict(as_series=False), "query": query}
        prompt = f"Process the input data based on the query: {query}. Determine the search queries for {self.name}. Return a JSON object with the search queries."
        search_queries = call_ai_model(input_dict, prompt)

        results = []
        for search_query in search_queries:
            encoded_query = quote_plus(search_query)
            jina_search_url = f"https://s.jina.ai/{encoded_query}"
            headers = {"Accept": "application/json"}

            response = requests.get(jina_search_url, headers=headers)

            if response.status_code == 200:
                data = response.json()["data"]
                results.append(
                    pl.DataFrame(
                        {
                            "title": [data["title"]],
                            "url": [data["url"]],
                            "content": [data["content"]],
                        }
                    )
                )
            else:
                raise ValueError(
                    f"Failed to fetch search results for '{search_query}'. Status code: {response.status_code}"
                )

        return pl.concat(results) if results else pl.DataFrame()

    def batch_process(
        self,
        batch_size: int,
        input_data: pl.DataFrame,
        query: str,
        route_name: Optional[str] = None,
    ) -> pl.DataFrame:
        total_rows = input_data.shape[0]
        results = []

        for i in range(0, total_rows, batch_size):
            batch = input_data.slice(i, min(batch_size, total_rows - i))
            batch_result = self.execute(
                input_data=batch, query=query, route_name=route_name
            )
            results.append(batch_result)

        return pl.concat(results)

    def enrich_data(self, data: pl.DataFrame) -> pl.DataFrame:
        input_data = {"data": data.to_dict(as_series=False)}
        prompt = "Analyze the given data and suggest potential enrichments. This may include derived features, aggregations, or additional data sources that could be integrated. Return a JSON array with the enrichment suggestions."
        enrichment_suggestions = call_ai_model(input_data, prompt)

        for enrichment in enrichment_suggestions:
            enrichment_type = enrichment.get("type")
            if enrichment_type == "derived_feature":
                data = data.with_columns(
                    pl.expr(enrichment.get("expression", "")).alias(
                        enrichment.get("name", "")
                    )
                )
            elif enrichment_type == "aggregation":
                data = data.with_columns(
                    pl.col(enrichment.get("column", ""))
                    .agg(enrichment.get("function", "mean"))
                    .alias(enrichment.get("name", ""))
                )
            elif enrichment_type == "external_data":
                print("NOT IMPLEMENTED TO FETCH EXTERNAL DATA")
        return data

    def optimize_query(self, query: str, data: pl.DataFrame) -> str:
        input_data = {"query": query, "data_schema": data.schema}
        prompt = "Analyze the given query and data schema. Suggest optimizations for the query to improve performance and efficiency. Return an optimized version of the query."
        optimized_query = call_ai_model(input_data, prompt)
        return optimized_query["optimized_query"]

    def explain_data(self, data: pl.DataFrame) -> str:
        input_data = {"data": data.to_dict(as_series=False)}
        prompt = "Analyze the given data and provide a comprehensive explanation. Include insights about the data structure, key statistics, potential patterns or trends, and any notable characteristics. Return a detailed explanation in natural language."
        explanation = call_ai_model(input_data, prompt)
        return explanation["explanation"]

    # Add new methods to handle other AI model calls

    def _generate_selectors(self, html_subset: str) -> Dict[str, str]:
        input_data = {"html": html_subset}
        prompt = "Analyze the HTML structure and generate CSS selectors for key data elements. Focus on the main content area. Return a JSON object with element names as keys and CSS selectors as values."
        return call_ai_model(input_data, prompt)

    def _process_api_response(
        self, response_data: Dict[str, Any], query: str
    ) -> pl.DataFrame:
        input_data = {"response_data": response_data, "query": query}
        prompt = f"Process the API response data based on the query: {query}. Extract relevant information and structure it for analysis. Return a JSON object with the processed data."
        processed_data = call_ai_model(input_data, prompt)
        return pl.DataFrame(processed_data)

    def _extract_web_content(self, html_content: str, query: str) -> pl.DataFrame:
        input_data = {"html_content": html_content, "query": query}
        prompt = f"Extract relevant information from the HTML content based on the query: {query}. Structure the extracted data for analysis. Return a JSON object with the extracted and structured data."
        extracted_data = call_ai_model(input_data, prompt)
        return pl.DataFrame(extracted_data)

    def _process_file_content(
        self, file_content: Any, file_type: str, query: str
    ) -> pl.DataFrame:
        input_data = {
            "file_content": file_content,
            "file_type": file_type,
            "query": query,
        }
        prompt = f"Process the {file_type} file content based on the query: {query}. Extract relevant information and structure it for analysis. Return a JSON object with the processed data."
        processed_data = call_ai_model(input_data, prompt)
        return pl.DataFrame(processed_data)

    def _process_search_results(
        self, search_results: List[Dict[str, str]], query: str
    ) -> pl.DataFrame:
        input_data = {"search_results": search_results, "query": query}
        prompt = f"Process the search results based on the query: {query}. Extract relevant information and structure it for analysis. Return a JSON object with the processed data."
        processed_data = call_ai_model(input_data, prompt)
        return pl.DataFrame(processed_data)

    def validate_data(self, data: pl.DataFrame) -> pl.DataFrame:
        input_data = {"data": data.to_dict(as_series=False)}
        prompt = "Validate the given data. Check for inconsistencies, missing values, and potential errors. Return a JSON object with the validation results and any necessary corrections."
        validation_result = call_ai_model(input_data, prompt)

        # Apply corrections if any
        if "corrections" in validation_result:
            for column, corrections in validation_result["corrections"].items():
                data = data.with_columns(pl.col(column).replace(corrections))

        # Add validation flags if any
        if "flags" in validation_result:
            for column, flags in validation_result["flags"].items():
                data = data.with_columns(pl.lit(flags).alias(f"{column}_flag"))

        return data
