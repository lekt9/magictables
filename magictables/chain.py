# chain.py
import polars as pl
from typing import List, Dict, Any, Optional
from .source import Source
from .magictables import MagicTable
from .utils import call_ai_model


class Chain:
    def __init__(self):
        self.steps: List[Dict[str, Any]] = []
        self.magic_table: MagicTable = MagicTable.get_instance()
        self.analysis_query: Optional[str] = None
        self.result: Optional[pl.DataFrame] = None

    def add(self, source: Source, query: str, route_name: Optional[str] = None):
        self.steps.append({"source": source, "query": query, "route_name": route_name})
        return self

    def analyze(self, query: str):
        self.analysis_query = query
        return self

    def execute(self, input_data: pl.DataFrame) -> pl.DataFrame:
        self.result = input_data

        for step in self.steps:
            source = step["source"]
            query = step["query"]
            route_name = step["route_name"]

            try:
                current_result = source.execute(
                    input_data=self.result, query=query, route_name=route_name
                )
                if current_result is not None and not current_result.is_empty():
                    self.result = self._merge_dataframes(
                        self.result, current_result, query
                    )
                else:
                    print(f"Warning: Step '{query}' returned no data. Skipping merge.")
            except Exception as e:
                print(f"Error executing step: {e}")
                print(f"Current result columns: {self.result.columns}")
                continue

        if self.analysis_query:
            self.result = self._analyze(self.result)

        return self.result

    def _merge_dataframes(
        self, df1: pl.DataFrame, df2: pl.DataFrame, query: str
    ) -> pl.DataFrame:
        merge_instructions = self._get_merge_instructions(df1, df2, query)
        return self._apply_merge_instructions(df1, df2, merge_instructions)

    def _get_merge_instructions(
        self, df1: pl.DataFrame, df2: pl.DataFrame, query: str
    ) -> Dict[str, Any]:
        input_data = {
            "df1_columns": df1.columns,
            "df2_columns": df2.columns,
            "query": query,
        }
        prompt = """
        Analyze the columns of both dataframes and the given query. Provide instructions on how to merge these dataframes.
        Return a JSON object with:
        - 'merge_type': The type of join to perform (e.g., 'inner', 'outer', 'left', 'right')
        - 'on': A list of column names to join on. These should exist in both dataframes. If no common columns are found, suggest the best columns to use for joining.
        - 'how': Either 'horizontal' for a standard join, or 'vertical' for concatenation.
        
        df1 columns: {df1_columns}
        df2 columns: {df2_columns}
        Query: {query}
        """.format(
            **input_data
        )
        return call_ai_model(input_data, prompt)

    def _apply_merge_instructions(
        self, df1: pl.DataFrame, df2: pl.DataFrame, instructions: Dict[str, Any]
    ) -> pl.DataFrame:
        merge_type = instructions.get("merge_type", "outer")
        on = instructions.get("on", [])
        how = instructions.get("how", "horizontal")

        print("Merge instructions:", instructions)
        print("df1 columns:", df1.columns)
        print("df2 columns:", df2.columns)

        # Check if either dataframe is empty
        if df1.is_empty():
            return df2
        if df2.is_empty():
            return df1

        # Filter 'on' to only include columns that exist in both dataframes
        valid_on = [col for col in on if col in df1.columns and col in df2.columns]

        if not valid_on and how == "horizontal":
            print(
                "Warning: No common columns found for joining. Falling back to cross join."
            )
            return df1.join(df2, how="cross")

        if how == "horizontal":
            return df1.join(df2, on=valid_on, how=merge_type)
        elif how == "vertical":
            # Check if dataframes have the same schema
            if df1.columns == df2.columns:
                return pl.concat([df1, df2], how="vertical")
            else:
                # If schemas don't match, return df1 and log a warning
                print(
                    "Warning: Cannot concatenate vertically due to mismatched schemas. Returning first dataframe."
                )
                return df1
        else:
            return pl.concat([df1, df2], how="horizontal")

    def _analyze(self, data: pl.DataFrame) -> pl.DataFrame:
        input_data = {
            "data": data.to_dict(as_series=False),
            "query": self.analysis_query,
        }
        prompt = f"Analyze the given data based on the query: {self.analysis_query}. Provide insights, patterns, and recommendations. Return a JSON object with the analysis results."
        result = call_ai_model(input_data, prompt)

        return pl.DataFrame(result)
