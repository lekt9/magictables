# source.py
import polars as pl
import pandas as pd
import requests
from urllib.parse import quote_plus
from typing import Dict, Any, List, Optional, Tuple
from .utils import call_ai_model, flatten_nested_structure
from .magictables import MagicTable
from pdf_reader import get_elements_from_pdf
from urllib.parse import urlparse, parse_qs, urlencode

from typing import Dict
from .magictables import MagicTable


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

    def _populate_url(self, url_template: str, input_data: pl.DataFrame) -> str:
        # Parse the URL template
        parsed_url = urlparse(url_template)
        path = parsed_url.path
        query_params = parse_qs(parsed_url.query)

        # Populate path parameters
        for column in input_data.columns:
            placeholder = f"{{{column}}}"
            if placeholder in path:
                path = path.replace(placeholder, str(input_data[column][0]))

        # Populate query parameters
        for param, values in query_params.items():
            if (
                len(values) > 0
                and values[0].startswith("{")
                and values[0].endswith("}")
            ):
                column = values[0][1:-1]  # Remove curly braces
                if column in input_data.columns:
                    query_params[param] = [str(input_data[column][0])]

        # Reconstruct the URL
        populated_url = parsed_url._replace(
            path=path, query=urlencode(query_params, doseq=True)
        ).geturl()

        return populated_url

    def _execute_api(
        self, input_data: pl.DataFrame, query: str, route_name: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], str, Optional[requests.Response]]:
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
                "url_template": "https://api.github.com/users/{{username}}"
            }}

            Now, based on the actual input data and query provided, generate the appropriate API call parameters:
            """
            api_info = call_ai_model(input_dict, prompt)
            route_name = api_info["route_name"]
            url_template = api_info["url_template"]

            # Populate the URL with parameters
        populated_url = self._populate_url(url_template, input_data)

        def fetch_api_data(row: Dict[str, Any]) -> List[Dict[str, Any]]:
            try:
                url = populated_url.format(**row)
                print(f"Attempting to fetch data from URL: {url}")
                headers = {"Accept": "application/json"}
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                data = response.json()
                flattened_data = flatten_nested_structure(data)
                return flattened_data  # This is now a list of dictionaries
            except Exception as e:
                return []  # Return an empty list in case of an error

        results = input_data.select(
            pl.struct(pl.all())
            .map_elements(fetch_api_data, return_dtype=pl.List(pl.Struct))
            .alias("struct_list")
        )

        flattened_results = []
        for row in results.to_dicts():
            for struct_list in row.values():
                flattened_results.extend(struct_list)

        # We don't have a single response object for multiple API calls, so we'll return None
        return flattened_results, populated_url, None

    def _execute_web(
        self, input_data: pl.DataFrame, query: str
    ) -> Tuple[pl.DataFrame, str, Optional[requests.Response]]:
        input_dict = {"input_data": input_data.to_dict(as_series=False), "query": query}
        prompt = f"Process the input data based on the query: {query}. Determine the web pages to scrape for {self.name}. Return a JSON object with the URLs to scrape."
        urls_to_scrape = call_ai_model(input_dict, prompt)

        results = []
        last_url = ""
        last_response = None
        for url in urls_to_scrape:
            jina_api_url = f"https://r.jina.ai/{url}"
            headers = {"Accept": "application/json"}
            response = requests.get(jina_api_url, headers=headers)
            last_url = url
            last_response = response

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
                print(
                    f"Failed to fetch content from {url}. Status code: {response.status_code}"
                )

        return (
            pl.concat(results) if results else pl.DataFrame(),
            last_url,
            last_response,
        )

    def _execute_file(
        self, input_data: pl.DataFrame, query: str
    ) -> Tuple[pl.DataFrame, str, None]:
        input_dict = {"input_data": input_data.to_dict(as_series=False), "query": query}
        prompt = f"Process the input data based on the query: {query}. Determine the files to read for {self.name}. Return a JSON object with the file paths and types."
        files_to_read = call_ai_model(input_dict, prompt)

        results = []
        last_file_path = ""
        for file_info in files_to_read:
            file_path = file_info.get("path", "")
            file_type = file_info.get("type", "")
            last_file_path = file_path

            if not file_path or not file_type:
                print(f"Invalid file info: {file_info}")
                continue

            try:
                if file_type == "csv":
                    df = pl.read_csv(file_path)
                elif file_type == "json":
                    df = pl.read_json(file_path)
                elif file_type == "parquet":
                    df = pl.read_parquet(file_path)
                elif file_type == "excel":
                    df = pl.from_pandas(pd.read_excel(file_path))
                elif file_type == "pdf":
                    elements = get_elements_from_pdf(file_path)
                    df = pl.DataFrame(elements)
                else:
                    print(f"Unsupported file type: {file_type}")
                    continue

                results.append(df)
            except Exception as e:
                print(f"Error reading file {file_path}: {str(e)}")

        return pl.concat(results) if results else pl.DataFrame(), last_file_path, None

    def _execute_search(
        self, input_data: pl.DataFrame, query: str
    ) -> Tuple[pl.DataFrame, str, Optional[requests.Response]]:
        input_dict = {"input_data": input_data.to_dict(as_series=False), "query": query}
        prompt = f"Process the input data based on the query: {query}. Generate search queries for {self.name}. Return a JSON object with the search queries."
        search_queries = call_ai_model(input_dict, prompt)

        results = []
        last_url = ""
        last_response = None
        for search_query in search_queries:
            encoded_query = quote_plus(search_query)
            url = f"https://api.duckduckgo.com/?q={encoded_query}&format=json"
            response = requests.get(url)
            last_url = url
            last_response = response

            if response.status_code == 200:
                data = response.json()
                for result in data.get("RelatedTopics", []):
                    results.append(
                        pl.DataFrame(
                            {
                                "query": [search_query],
                                "text": [result.get("Text", "")],
                                "url": [result.get("FirstURL", "")],
                            }
                        )
                    )
            else:
                print(
                    f"Failed to fetch search results for query: {search_query}. Status code: {response.status_code}"
                )

        return (
            pl.concat(results) if results else pl.DataFrame(),
            last_url,
            last_response,
        )

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
        self, input_data: pl.DataFrame, query: str, route_name: Optional[str] = None
    ) -> pl.DataFrame:
        magic_table = MagicTable.get_instance()

        print("input_data", input_data)

        # Try to find a similar cached result
        cached_result = magic_table.find_similar_cached_result(self.name, input_data)

        if cached_result is not None:
            return pl.DataFrame(cached_result)

        # If no cached result found, proceed with execution
        if self.source_type == "api":
            result_list, populated_route, response = self._execute_api(
                input_data, query, route_name
            )
        elif self.source_type == "web":
            result_list, populated_route, response = self._execute_web(
                input_data, query
            )
        elif self.source_type == "file":
            result_list, populated_route, response = self._execute_file(
                input_data, query
            )
        elif self.source_type == "search":
            result_list, populated_route, response = self._execute_search(
                input_data, query
            )
        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")

        # Convert the result_list to a DataFrame
        result = pl.DataFrame(result_list)

        # Store the populated route and response
        self._store_execution_details(populated_route, response)

        # Cache the result
        identifier = magic_table.predict_identifier(result)[0]

        print("identifier", identifier)

        magic_table.cache_result(
            self.name, str(identifier), result.to_dict(as_series=False)
        )

        # Store the result in the database
        magic_table.store_result(self.name, result)

        print("result", result)

        return result
