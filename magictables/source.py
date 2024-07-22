# source.py
import polars as pl
import requests
from typing import Dict, Any, List, Optional
from .utils import call_ai_model, flatten_nested_structure
from .magictables import MagicTable
from urllib.parse import urlparse, parse_qs, urlencode

from typing import Dict
from .magictables import MagicTable
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional


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

    def _process_api_response(
        self, response_data: Dict[str, Any], query: str
    ) -> pl.DataFrame:
        input_dict = {"response_data": response_data, "query": query}
        prompt = f"""
        Process the API response data based on the query: {query}.
        Extract relevant information and structure it into a tabular format.
        Return a JSON object representing the structured data, where keys are column names and values are lists of column values.

        Example output:
        {{
            "id": [1, 2, 3],
            "name": ["John", "Jane", "Bob"],
            "age": [25, 30, 35]
        }}

        Now, based on the actual API response and query provided, generate the structured data:
        """
        structured_data = call_ai_model(input_dict, prompt)
        return pl.DataFrame(structured_data)

    def _extract_relevant_data(
        self, data: Dict[str, Any], query: str
    ) -> Dict[str, List[Any]]:
        input_dict = {"data": data, "query": query}
        prompt = f"""
        Extract relevant information from the data based on the query: {query}.
        Return a JSON object where keys are column names and values are lists of column values.

        Example output:
        {{
            "id": [1, 2, 3],
            "name": ["John", "Jane", "Bob"],
            "age": [25, 30, 35]
        }}

        Now, based on the actual data and query provided, extract the relevant information:
        """
        relevant_data = call_ai_model(input_dict, prompt)
        return relevant_data

    def _store_execution_details(self, populated_route: str, response: Any):
        """
        Store the populated route with parameters and the response in the MagicTable database.
        """
        execution_details = {
            "populated_route": populated_route,
            "response": self._serialize_response(response),
        }

        # Store execution details in the MagicTable database
        self.magic_table.store_execution_details(self.name, execution_details)

    def _serialize_response(self, response: Any) -> Dict[str, Any]:
        """
        Serialize the response object to a dictionary.
        """
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

    def execute(
        self,
        input_data: Optional[pl.DataFrame],
        query: str,
        route_name: Optional[str] = None,
    ) -> pl.DataFrame:
        magic_table = MagicTable.get_instance()

        # If input_data is None, create an empty DataFrame
        if input_data is None:
            input_data = pl.DataFrame()

        # Try to find a similar cached result
        cached_result = magic_table.find_similar_cached_result(self.name, input_data)

        if cached_result is not None:
            return pl.DataFrame(cached_result)

        if self.source_type == "api":
            result = self._execute_api(input_data, query, route_name)
        # elif self.source_type == "web":
        #     result = self._execute_web(input_data, query)
        # elif self.source_type == "file":
        #     result = self._execute_file(input_data, query)
        # elif self.source_type == "search":
        #     result = self._execute_search(input_data, query)
        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")

        # Convert the result to a DataFrame
        result_df = pl.DataFrame(result.data)

        # Store the contexts and responses
        for context, response in zip(result.contexts, result.responses):
            self._store_execution_details(context, response)

        # Cache the result
        identifier = magic_table.predict_identifier(result_df)[0]
        magic_table.cache_result(
            self.name, str(identifier), result_df.to_dict(as_series=False)
        )

        # Store the result in the database
        magic_table.store_result(self.name, result_df)

        return result_df

    def _execute_api(
        self,
        input_data: pl.DataFrame,
        query: str,
        route_name: Optional[str] = None,
    ) -> ExecutionResult:
        if route_name is None:
            raise ValueError("route_name must be provided for API execution")

        route = self.magic_table.get_route(self.name, route_name)
        if not route:
            raise ValueError(f"No route found for {self.name} with name {route_name}")

        url_template = route["url"]

        flattened_results = []
        populated_urls = []
        responses = []

        # If input_data is empty, execute the API call once without parameters
        if input_data.is_empty():
            populated_url = url_template
            populated_urls.append(populated_url)

            response = requests.get(
                populated_url, headers={"Accept": "application/json"}, timeout=10
            )
            response.raise_for_status()
            data = response.json()
            flattened_data = flatten_nested_structure(data)
            flattened_results.extend(flattened_data)
            responses.append(response)
        else:
            # If input_data is not empty, iterate over its rows
            for row in input_data.to_dicts():
                populated_url = self._populate_url(url_template, row)
                populated_urls.append(populated_url)

                response = requests.get(
                    populated_url, headers={"Accept": "application/json"}, timeout=10
                )
                response.raise_for_status()
                data = response.json()
                flattened_data = flatten_nested_structure(data)
                flattened_results.extend(flattened_data)
                responses.append(response)

        if not flattened_results:
            raise ValueError("No data returned from API")

        return ExecutionResult(
            data=flattened_results, contexts=populated_urls, responses=responses
        )

    def _populate_url(self, url_template: str, input_data: Dict[str, Any]) -> str:
        # Parse the URL template
        parsed_url = urlparse(url_template)
        path = parsed_url.path
        query_params = parse_qs(parsed_url.query)

        # Populate path parameters
        for column, value in input_data.items():
            placeholder = f"{{{column}}}"
            if placeholder in path:
                path = path.replace(placeholder, str(value))

        # Populate query parameters
        for param, values in query_params.items():
            if (
                len(values) > 0
                and values[0].startswith("{")
                and values[0].endswith("}")
            ):
                column = values[0][1:-1]  # Remove curly braces
                if column in input_data:
                    query_params[param] = [str(input_data[column])]

        # Reconstruct the URL
        populated_url = parsed_url._replace(
            path=path, query=urlencode(query_params, doseq=True)
        ).geturl()

        return populated_url

    # def _execute_web(self, input_data: pl.DataFrame, query: str) -> ExecutionResult:
    #     urls_to_scrape = self._determine_urls_to_scrape(input_data, query)
    #     results = []
    #     contexts = []
    #     responses = []

    #     for url in urls_to_scrape:
    #         jina_api_url = f"https://r.jina.ai/{url}"
    #         try:
    #             response = requests.get(
    #                 jina_api_url, headers={"Accept": "application/json"}
    #             )
    #             response.raise_for_status()
    #             data = response.json()["data"]
    #             results.append(
    #                 {
    #                     "title": data["title"],
    #                     "url": data["url"],
    #                     "content": data["content"],
    #                 }
    #             )
    #             contexts.append(url)
    #             responses.append(response)
    #         except Exception as e:
    #             print(f"Failed to fetch content from {url}. Error: {str(e)}")
    #             responses.append(None)

    #     if not results:
    #         raise ValueError("No data scraped from web pages")

    #     return ExecutionResult(data=results, contexts=contexts, responses=responses)

    # def _execute_file(self, input_data: pl.DataFrame, query: str) -> ExecutionResult:
    #     files_to_read = self._determine_files_to_read(input_data, query)
    #     results = []
    #     contexts = []

    #     for file_info in files_to_read:
    #         file_path = file_info.get("path", "")
    #         file_type = file_info.get("type", "")

    #         if not file_path or not file_type:
    #             print(f"Invalid file info: {file_info}")
    #             continue

    #         try:
    #             df = self._read_file(file_path, file_type)
    #             results.extend(df.to_dicts())
    #             contexts.append(file_path)
    #         except Exception as e:
    #             print(f"Error reading file {file_path}: {str(e)}")

    #     if not results:
    #         raise ValueError("No data read from files")

    #     return ExecutionResult(data=results, contexts=contexts)

    # def _execute_search(self, input_data: pl.DataFrame, query: str) -> ExecutionResult:
    #     search_queries = self._generate_search_queries(input_data, query)
    #     results = []
    #     contexts = []
    #     responses = []

    #     for search_query in search_queries:
    #         encoded_query = quote_plus(search_query)
    #         url = f"https://api.duckduckgo.com/?q={encoded_query}&format=json"
    #         try:
    #             response = requests.get(url)
    #             response.raise_for_status()
    #             data = response.json()
    #             for result in data.get("RelatedTopics", []):
    #                 results.append(
    #                     {
    #                         "query": search_query,
    #                         "text": result.get("Text", ""),
    #                         "url": result.get("FirstURL", ""),
    #                     }
    #                 )
    #             contexts.append(url)
    #             responses.append(response)
    #         except Exception as e:
    #             print(
    #                 f"Failed to fetch search results for query: {search_query}. Error: {str(e)}"
    #             )
    #             responses.append(None)

    #     if not results:
    #         raise ValueError("No search results found")

    #     return ExecutionResult(data=results, contexts=contexts, responses=responses)
