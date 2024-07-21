from .source import Source
from .shadow_db import ShadowDB
from .utils import call_ai_model
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed


class Chain:
    def __init__(self):
        self.steps = []
        self.shadow_db = ShadowDB.get_instance()
        self.analysis_query = None

    def add(self, source: Source):
        self.steps.append(source)

    def analyze(self, query: str):
        self.analysis_query = query

    def execute(self, **kwargs):
        with ThreadPoolExecutor() as executor:
            future_results = [
                executor.submit(step.execute, **kwargs) for step in self.steps
            ]
            results = [future.result() for future in as_completed(future_results)]

        # Convert results to LazyFrames if they aren't already
        lazy_results = [
            result.lazy() if isinstance(result, pl.DataFrame) else pl.LazyFrame(result)
            for result in results
        ]

        # Combine results using lazy evaluation
        combined_result = pl.concat(lazy_results)

        materialized_result = combined_result.collect()

        analyzed_result = self._analyze(materialized_result)
        return analyzed_result

    def _analyze(self, data: pl.DataFrame) -> pl.DataFrame:
        if self.analysis_query is None:
            return data

        # Convert to dict only if necessary for the AI model
        input_data = {
            "data": data.to_dict(as_series=False),
            "query": self.analysis_query,
        }
        prompt = "Analyze the given data based on the provided query. Return a JSON object representing the analyzed data."
        result = call_ai_model(input_data, prompt)

        # Convert the result back to a Polars DataFrame
        return pl.DataFrame(result)
