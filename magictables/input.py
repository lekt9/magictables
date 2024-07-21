from urllib.parse import quote_plus
import polars as pl
import requests

from .magictables import MagicTable


class Input:
    def __init__(self):
        self.magic_table = MagicTable.get_instance()

    def from_api(self, api_name: str, route_name: str, **kwargs) -> pl.DataFrame:
        route = self.magic_table.get_route(api_name, route_name)
        if not route:
            raise ValueError(f"API route not found: {api_name}/{route_name}")

        url = route["url"].format(**kwargs)
        headers = {"Accept": "application/json"}

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            return pl.DataFrame([data])
        else:
            raise ValueError(
                f"Failed to fetch data from {url}. Status code: {response.status_code}"
            )

    def from_web(self, url: str) -> pl.DataFrame:
        jina_api_url = f"https://r.jina.ai/{url}"
        headers = {"Accept": "application/json"}

        response = requests.get(jina_api_url, headers=headers)

        if response.status_code == 200:
            data = response.json()["data"]

            df = pl.DataFrame(
                {
                    "title": [data["title"]],
                    "url": [data["url"]],
                    "content": [data["content"]],
                }
            )

            return df
        else:
            raise ValueError(
                f"Failed to fetch content from {url}. Status code: {response.status_code}"
            )

    def from_search(self, query: str) -> pl.DataFrame:
        encoded_query = quote_plus(query)
        jina_search_url = f"https://s.jina.ai/{encoded_query}"
        headers = {"Accept": "application/json"}

        response = requests.get(jina_search_url, headers=headers)

        if response.status_code == 200:
            data = response.json()["data"]

            df = pl.DataFrame(
                {
                    "title": [data["title"]],
                    "url": [data["url"]],
                    "content": [data["content"]],
                }
            )

            return df
        else:
            raise ValueError(
                f"Failed to fetch search results for '{query}'. Status code: {response.status_code}"
            )

    def from_csv(self, csv_path: str) -> pl.DataFrame:
        return pl.read_csv(csv_path)

    def batch_process(self, source_func, batch_size: int, **kwargs) -> pl.DataFrame:
        results = []
        for i in range(0, len(kwargs["data"]), batch_size):
            batch = kwargs["data"][i : i + batch_size]
            batch_kwargs = {**kwargs, "data": batch}
            result = source_func(**batch_kwargs)
            results.append(result)
        return pl.concat(results)
