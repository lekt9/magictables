from urllib.parse import quote_plus
import polars as pl
import requests

from .shadow_db import ShadowDB
from .utils import create_sqlalchemy_engine


class Input:
    def __init__(self):
        self.shadow_db = ShadowDB.get_instance()
        self.engine = create_sqlalchemy_engine(self.shadow_db.engine.url)

    def from_api(self, api_name: str, route_name: str, **kwargs) -> pl.DataFrame:
        source = self.shadow_db.get_route(api_name, route_name)
        if not source:
            raise ValueError(f"API route not found: {api_name}/{route_name}")

        result = source.execute(**kwargs)
        return result

    def from_web(self, url: str) -> pl.DataFrame:
        jina_api_url = f"https://r.jina.ai/{url}"
        headers = {"Accept": "application/json"}

        response = requests.get(jina_api_url, headers=headers)

        if response.status_code == 200:
            data = response.json()["data"]

            # Create a DataFrame from the API response
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

            # Create a DataFrame from the API response
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

    def from_pdf(self, pdf_path: str, query: str) -> pl.DataFrame:
        source = Source.pdf("pdf_source")
        result = source.execute(pdf_path=pdf_path, query=query)
        return result

    def from_sql(self, query: str) -> pl.DataFrame:
        return sql_to_polars(query, self.engine)

    def from_csv(self, csv_path: str) -> pl.DataFrame:
        return pl.read_csv(csv_path)

    def to_sql(self, df: pl.DataFrame, table_name: str, if_exists="replace"):
        polars_to_sql(df, table_name, self.engine, if_exists)

    def batch_process(self, source_func, batch_size: int, **kwargs) -> pl.DataFrame:
        results = []
        for i in range(0, len(kwargs["data"]), batch_size):
            batch = kwargs["data"][i : i + batch_size]
            batch_kwargs = {**kwargs, "data": batch}
            result = source_func(**batch_kwargs)
            results.append(result)
        return pl.concat(results)
