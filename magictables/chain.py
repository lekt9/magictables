import json
from .source import Source
from .magictables import MagicTable
from .utils import call_ai_model
import polars as pl
from typing import List, Dict, Any, Optional, Tuple


class Chain:
    def __init__(self):
        self.steps: List[Dict[str, Any]] = []
        self.magic_table: MagicTable = MagicTable.get_instance()
        self.analysis_query: Optional[str] = None
        self.result: Optional[pl.DataFrame] = None

    def add(self, source: Source, query: Optional[str] = None):
        self.steps.append({"source": source, "query": query})
        return self

    def analyze(self, query: str):
        self.analysis_query = query
        return self

    def execute(self, **kwargs) -> pl.DataFrame:
        for step in self.steps:
            source = step["source"]
            query = step["query"]
            current_result = source.execute(**kwargs)

            if self.result is None:
                self.result = current_result
            else:
                self.result = self._merge_dataframes(
                    self.result, current_result, source.name, query
                )

        if self.analysis_query:
            self.result = self._analyze(self.result)

        return self.result

    def _merge_dataframes(
        self,
        df1: pl.DataFrame,
        df2: pl.DataFrame,
        source_name: str,
        query: Optional[str],
    ) -> pl.DataFrame:
        relationships = self._identify_relationships(df1, df2, source_name)

        if query:
            relationships = self._augment_relationships(relationships, query, df1, df2)

        if relationships:
            best_relationship = max(
                relationships, key=lambda x: x[2]
            )  # Get the relationship with the highest similarity
            col1, col2, similarity = best_relationship

            print(
                f"Merging on columns: {col1} and {col2} with similarity {similarity:.2f}"
            )
            return df1.join(df2, left_on=col1, right_on=col2, how="outer")
        else:
            print(
                "No strong relationships found. Aligning and concatenating dataframes."
            )

            # Create a mapping of columns based on the identified relationships
            column_mapping = {}
            for col1, col2, _ in relationships:
                column_mapping[col2] = col1

            # Rename columns in df2 based on the mapping
            df2_aligned = df2.rename(column_mapping)

            # Get all unique column names
            all_columns = list(set(df1.columns) | set(df2_aligned.columns))

            # Function to get the dtype of a column, defaulting to String if not present
            def get_dtype(df, col):
                return df.schema[col] if col in df.schema else pl.String

            # Align both dataframes
            df1_aligned = df1.select(
                [
                    (
                        pl.col(c)
                        if c in df1.columns
                        else pl.lit(None).cast(get_dtype(df2_aligned, c)).alias(c)
                    )
                    for c in all_columns
                ]
            )
            df2_aligned = df2_aligned.select(
                [
                    (
                        pl.col(c)
                        if c in df2_aligned.columns
                        else pl.lit(None).cast(get_dtype(df1, c)).alias(c)
                    )
                    for c in all_columns
                ]
            )

            # Concatenate the aligned dataframes
            return pl.concat([df1_aligned, df2_aligned], how="vertical")

    def _identify_relationships(
        self, df1: pl.DataFrame, df2: pl.DataFrame, source_name: str
    ) -> List[Tuple[str, str, float]]:
        def get_column_statistics(df: pl.DataFrame, column: str) -> dict:
            stats = df.select(
                [
                    pl.col(column).n_unique().alias("unique_count"),
                    pl.col(column).count().alias("total_count"),
                    pl.col(column).null_count().alias("null_count"),
                    pl.col(column).cast(pl.Utf8).alias("dtype"),
                ]
            ).to_dict(as_series=False)
            return {k: v[0] for k, v in stats.items()}

        def get_sample_values(
            df: pl.DataFrame, column: str, max_samples: int = 10000
        ) -> List:
            stats = get_column_statistics(df, column)
            unique_count = stats["unique_count"]
            total_count = stats["total_count"]

            if unique_count <= max_samples:
                return df.select(pl.col(column)).unique().to_series().to_list()
            else:
                sample_size = min(max_samples, total_count)
                return (
                    df.select(pl.col(column))
                    .sample(n=sample_size, shuffle=True)
                    .to_series()
                    .to_list()
                )

        def calculate_similarity(values1: List, values2: List) -> float:
            set1, set2 = set(values1), set(values2)
            intersection = len(set1.intersection(set2))
            union = len(set1.union(set2))
            return intersection / union if union > 0 else 0

        potential_relationships = []
        for col1 in df1.columns:
            stats1 = get_column_statistics(df1, col1)
            values1 = get_sample_values(df1, col1)
            for col2 in df2.columns:
                stats2 = get_column_statistics(df2, col2)
                values2 = get_sample_values(df2, col2)

                similarity = calculate_similarity(values1, values2)

                if similarity > 0.5:  # You can adjust this threshold
                    potential_relationships.append((col1, col2, similarity))

        # Sort relationships by similarity in descending order
        potential_relationships.sort(key=lambda x: x[2], reverse=True)

        # Add relationships to MagicTable
        for col1, col2, similarity in potential_relationships:
            relationship = f"potential_merge_key_{col1}_{col2}"
            self.magic_table.add_relationship(source_name, df2.columns[0], relationship)
            print(
                f"Potential merge key found: {col1} (from previous step) and {col2} (from {source_name}) with similarity {similarity:.2f}"
            )

        return potential_relationships if potential_relationships else []

    def _augment_relationships(
        self,
        relationships: List[Tuple[str, str, float]],
        query: str,
        df1: pl.DataFrame,
        df2: pl.DataFrame,
    ) -> List[Tuple[str, str, float]]:
        input_data = {
            "relationships": relationships,
            "query": query,
            "df1_columns": df1.columns,
            "df2_columns": df2.columns,
        }
        prompt = """
        Given the list of potential relationships between two dataframes and a user-provided query, 
        modify the relationships to better match the query. You can add new relationships, remove existing ones, 
        or adjust the similarity scores. Return a JSON object with the updated list of relationships in the format:
        [{"col1": "column_name_1", "col2": "column_name_2", "similarity": float_value}, ...]
        """
        result = call_ai_model(input_data, prompt)

        # Convert the AI model result to the expected format
        updated_relationships = [
            (r["col1"], r["col2"], r["similarity"]) for r in result
        ]

        # Cache the updated relationships
        cache_key = f"{df1.columns[0]}_{df2.columns[0]}_{query}"
        self.magic_table.cache_relationships(cache_key, updated_relationships)

        return updated_relationships

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
        ?[related_data] := {self.steps[0]["source"].name}('{key}', data),
                           data['{relationship}'] = related_id,
                           {self.steps[-1]["source"].name}(related_id, related_data)
        """
        result = self.magic_table.db.run(query)
        return pl.DataFrame([json.loads(row["related_data"]) for row in result])
