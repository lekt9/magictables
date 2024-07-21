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

    def execute(self, input_data: pl.DataFrame) -> pl.DataFrame:
        current_data = input_data

        print("self steps", self.steps)

        for step in self.steps:
            source = step["source"]
            query = step["query"]
            route_name = step["route_name"]

            try:
                step_result = source.execute(
                    input_data=current_data, query=query, route_name=route_name
                )

                if isinstance(step_result, pl.DataFrame) and not step_result.is_empty():
                    current_data = self._merge_dataframes(
                        current_data, step_result, query
                    )
                else:
                    print(
                        f"Warning: Step '{query}' returned no data or invalid result. Using previous data."
                    )
            except Exception as e:
                print(f"Error executing step: {e}")
                continue

        if self.analysis_query:
            current_data = self._analyze(current_data)

        self.result = current_data
        return self.result

    def _identify_relevant_columns(
        self, data: pl.DataFrame, next_step: Dict[str, Any]
    ) -> List[str]:
        input_data = {
            "current_columns": data.columns,
            "next_step_query": next_step["query"],
            "next_step_route": next_step["route_name"],
        }
        prompt = f"""
        Analyze the current DataFrame columns and the next step in the chain.
        Identify which columns are likely to be relevant or required for the next step.
        Consider the query and route name of the next step when making this decision.
        Return a JSON array of column names that should be passed to the next step.

        Current columns: {input_data['current_columns']}
        Next step query: {input_data['next_step_query']}
        Next step route: {input_data['next_step_route']}
        """
        result = call_ai_model(input_data, prompt)
        return result["relevant_columns"]

    def analyze(self, query: str):
        self.analysis_query = query
        return self

    def _merge_dataframes(
        self, df1: pl.DataFrame, df2: pl.DataFrame, query: str
    ) -> pl.DataFrame:
        try:
            merge_instructions = self._get_merge_instructions(df1, df2, query)

            if not merge_instructions.get("on"):
                print("No common columns found. Attempting AI-assisted join...")
                ai_join_instructions = self._get_ai_join_instructions(df1, df2, query)
                if not ai_join_instructions["join_instructions"]:
                    print(
                        "No join instructions provided. Concatenating dataframes horizontally."
                    )
                    return pl.concat([df1, df2], how="horizontal")
                return self._apply_ai_join_instructions(df1, df2, ai_join_instructions)

            return self._apply_merge_instructions(df1, df2, merge_instructions)
        except Exception as e:
            print(f"Error in _merge_dataframes: {e}")
            print("Falling back to horizontal concatenation.")
            return pl.concat([df1, df2], how="horizontal")

    def _get_ai_join_instructions(
        self, df1: pl.DataFrame, df2: pl.DataFrame, query: str
    ) -> Dict[str, Any]:
        input_data = {
            "df1_columns": df1.columns,
            "df2_columns": df2.columns,
            "df1_sample": df1.head().to_dict(as_series=False),
            "df2_sample": df2.head().to_dict(as_series=False),
            "query": query,
        }
        prompt = """
            Analyze the columns and sample data of both dataframes. Suggest how to join these dataframes even if they don't have common column names.
            Consider the query context when determining the best join strategy.

            Return a JSON object with the following structure:
            {
                "join_type": ["inner", "outer", "left", "right"],
                "join_instructions": [
                    {
                        "df1_col": "column_name_from_df1",
                        "df2_col": "column_name_from_df2",
                        "transformation": "optional_transformation_instruction"
                    }
                ],
                "additional_operations": [
                    {
                        "dataframe": ["df1", "df2"],
                        "operation": "polars_operation_to_perform"
                    }
                ]
            }

            Dataframe 1 columns: {df1_columns}
            Dataframe 2 columns: {df2_columns}
            Dataframe 1 sample: {df1_sample}
            Dataframe 2 sample: {df2_sample}
            Query context: {query}
            """.format(
            **input_data
        )

        try:
            print("Calling AI model for join instructions...")
            response = call_ai_model(input_data, prompt)
            print("AI model response:", response)

            if not isinstance(response, dict):
                print(
                    f"Error: AI model response is not a dictionary. Type: {type(response)}"
                )
                raise ValueError("Invalid AI model response format")

            join_type = response.get("join_type", "left")
            join_instructions = response.get("join_instructions", [])
            additional_operations = response.get("additional_operations", [])

            # Validate join instructions
            for instr in join_instructions:
                if "df1_col" not in instr or "df2_col" not in instr:
                    raise ValueError("Invalid join instruction format")

            # Validate additional operations
            for op in additional_operations:
                if "dataframe" not in op or "operation" not in op:
                    raise ValueError("Invalid additional operation format")
                if op["dataframe"] not in ["df1", "df2"]:
                    raise ValueError(
                        "Invalid dataframe specified in additional operation"
                    )

            return {
                "join_type": join_type,
                "join_instructions": join_instructions,
                "additional_operations": additional_operations,
            }
        except Exception as e:
            print(f"Error in _get_ai_join_instructions: {e}")
            print("AI model response:", response)
            # Return a default join strategy
            return {
                "join_type": "left",
                "join_instructions": [],
                "additional_operations": [],
            }

    def _apply_ai_join_instructions(
        self, df1: pl.DataFrame, df2: pl.DataFrame, instructions: Dict[str, Any]
    ) -> pl.DataFrame:
        join_type = instructions.get("join_type", ["left"])[0]
        join_instructions = instructions.get("join_instructions", [])
        additional_operations = instructions.get("additional_operations", [])

        print("Additional operations:", additional_operations)

        # Apply additional operations
        for operation in additional_operations:
            df_name = operation["dataframe"]
            op_str = operation["operation"]
            try:
                if df_name == "df1":
                    df1 = eval(f"df1.{op_str}")
                elif df_name == "df2":
                    df2 = eval(f"df2.{op_str}")
            except Exception as e:
                print(f"Error applying operation '{op_str}' to {df_name}: {e}")

        # Prepare join conditions
        join_conditions = []
        for instr in join_instructions:
            df1_col = instr["df1_col"]
            df2_col = instr["df2_col"]
            transformation = instr.get("transformation")

            if transformation:
                try:
                    df1_expr = eval(f"pl.col('{df1_col}').{transformation}")
                    df2_expr = eval(f"pl.col('{df2_col}').{transformation}")
                    join_conditions.append(df1_expr == df2_expr)
                except Exception as e:
                    print(f"Error applying transformation '{transformation}': {e}")
                    join_conditions.append(pl.col(df1_col) == pl.col(df2_col))
            else:
                join_conditions.append(pl.col(df1_col) == pl.col(df2_col))

        # Perform the join
        if join_conditions:
            joined_df = df1.join(df2, how=join_type, on=join_conditions)
        else:
            print("Warning: No valid join conditions. Falling back to cross join.")
            joined_df = df1.join(df2, how="cross")

        return joined_df

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
            - 'merge_type': The type of join to perform (e.g., ["inner", "outer", "left", "right"])
            - 'on': A list of column names to join on. These should exist in both dataframes. If no common columns are found, suggest the best columns to use for joining.
            - 'how': Either ["horizontal"] for a standard join, or ["vertical"] for concatenation.
            
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
        merge_type = instructions.get("merge_type", ["outer"])[0]
        how = instructions.get("how", ["horizontal"])[0]
        on = instructions.get("on", [])

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
            print("No common columns found. Concatenating dataframes horizontally.")
            return pl.concat([df1, df2], how="horizontal")

        if how == "horizontal":
            if valid_on:
                return df1.join(df2, on=valid_on, how=merge_type)
            else:
                return pl.concat([df1, df2], how="horizontal")
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
