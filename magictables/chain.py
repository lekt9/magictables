# chain.py
import json
from typing import List
from .source import Source
from .magictables import MagicTable
from .utils import call_ai_model, to_dataframe
import polars as pl


class Chain:
    def __init__(self):
        self.steps: List[Source] = []
        self.magic_table = MagicTable.get_instance()
        self.analysis_query = None

    def add(self, source: Source):
        self.steps.append(source)

    def analyze(self, query: str):
        self.analysis_query = query

    def execute(self, **kwargs) -> pl.DataFrame:
        results = []
        for step in self.steps:
            result = step.execute(**kwargs)
            results.append(result)

            # Identify relationships between steps
            if len(results) > 1:
                self._identify_relationships(results[-2], results[-1], step.name)

        combined_result = pl.concat([to_dataframe(r) for r in results if r is not None])

        if self.analysis_query:
            analyzed_result = self._analyze(combined_result)
            return analyzed_result

        return combined_result

    def _identify_relationships(
        self, df1: pl.DataFrame, df2: pl.DataFrame, source_name: str
    ):
        common_columns = set(df1.columns) & set(df2.columns)
        for column in common_columns:
            self.magic_table.add_relationship(
                source_name, df2.columns[0], f"common_{column}"
            )

        # Add more sophisticated relationship identification logic
        numeric_columns1 = df1.select(pl.col(pl.NUMERIC_DTYPES)).columns
        numeric_columns2 = df2.select(pl.col(pl.NUMERIC_DTYPES)).columns

        for col1 in numeric_columns1:
            for col2 in numeric_columns2:
                correlation = df1[col1].corr(df2[col2])
                if abs(correlation) > 0.8:  # High correlation threshold
                    self.magic_table.add_relationship(
                        source_name, df2.columns[0], f"correlated_{col1}_{col2}"
                    )

        # Use AI to identify potential relationships
        relationship_prompt = f"Identify potential relationships between {source_name} and {df2.columns[0]} based on their columns and data patterns."
        ai_relationships = call_ai_model(
            {"df1": df1.to_dict(as_series=False), "df2": df2.to_dict(as_series=False)},
            relationship_prompt,
        )

        for relationship in ai_relationships:
            self.magic_table.add_relationship(source_name, df2.columns[0], relationship)

    def _analyze(self, data: pl.DataFrame) -> pl.DataFrame:
        input_data = {
            "data": data.to_dict(as_series=False),
            "query": self.analysis_query,
        }
        prompt = "Analyze the given data based on the provided query. Return a JSON object representing the analyzed data."
        result = call_ai_model(input_data, prompt)

        return pl.DataFrame(result)

    def get_related(self, key: str, relationship: str) -> pl.DataFrame:
        query = f"""
        ?[related_data] := {self.steps[0].name}('{key}', data),
                           data['{relationship}'] = related_id,
                           {self.steps[-1].name}(related_id, related_data)
        """
        result = self.magic_table.db.run(query)
        return pl.DataFrame([json.loads(row["related_data"]) for row in result])

    def smart_join(self, prompt: str) -> pl.DataFrame:
        # Execute the chain to get all data
        data = self.execute()

        # Get relationships for all sources
        relationships = {
            source: self.magic_table.get_relationships(source) for source in self.steps
        }

        # Prepare input for AI model
        input_data = {
            "data": data.to_dict(as_series=False),
            "relationships": relationships,
            "prompt": prompt,
            "columns": data.columns,
        }

        # Call AI model to get joining instructions
        join_instructions = call_ai_model(
            input_data,
            "Analyze the data, relationships, and columns. Provide detailed instructions for joining the data based on the given prompt. Include join types, columns to join on, and any necessary data transformations.",
        )

        # Initialize the joined data with the first dataframe
        joined_data = data

        # Apply joining instructions
        for instruction in join_instructions:
            table_name = instruction["table"]
            join_type = instruction["join_type"]
            join_columns = instruction["join_columns"]
            transformations = instruction.get("transformations", [])

            # Apply transformations
            for transform in transformations:
                joined_data = joined_data.with_columns(eval(transform))

            # Perform the join
            if join_columns:
                joined_data = joined_data.join(
                    data.select(pl.col(table_name + "_*")),
                    on=join_columns,
                    how=join_type,
                )
            else:
                joined_data = joined_data.join(
                    data.select(pl.col(table_name + "_*")), how="cross"
                )

        # Apply any final transformations or filters
        final_transformations = join_instructions[-1].get("final_transformations", [])
        for transform in final_transformations:
            joined_data = joined_data.with_columns(eval(transform))

        return joined_data
