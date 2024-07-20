from .decorators import mtable
import polars as pl
from typing import List, Callable, Any, Dict
import logging
from .utils import call_ai_model, generate_schema


class ChainedMTable:
    def __init__(self):
        self.functions: List[Callable] = []
        self.schemas: List[Dict[str, str]] = []

    def add(self, func: Callable) -> "ChainedMTable":
        self.functions.append(func)
        return self

    def execute(self, initial_input: Any) -> pl.DataFrame:
        result = initial_input
        for i, func in enumerate(self.functions):
            logging.debug(f"Executing function: {func.__name__}")

            # Generate schema for the current result
            current_schema = generate_schema(result)
            self.schemas.append(current_schema)

            if i > 0:
                # Generate mapping between previous and current schema
                mapping = self._generate_schema_mapping(
                    self.schemas[i - 1], current_schema
                )

                # Apply mapping to the result
                result = self._apply_mapping(result, mapping)

            # Execute the function with the mapped result
            result = func(result)

            # Ensure the result is a DataFrame
            if not isinstance(result, pl.DataFrame):
                result = pl.DataFrame([result] if isinstance(result, dict) else result)

        return result

    def _generate_schema_mapping(
        self, prev_schema: Dict[str, str], current_schema: Dict[str, str]
    ) -> Dict[str, str]:
        prompt = f"""
        Given the previous schema:
        {prev_schema}
        
        And the current schema:
        {current_schema}
        
        Generate a mapping between these schemas. The mapping should be a dictionary where:
        - Keys are column names from the current schema
        - Values are expressions using column names from the previous schema
        
        Return the mapping as a Python dictionary.
        """

        mapping_result = call_ai_model([{"role": "user", "content": prompt}])
        mapping = eval(mapping_result[0]["content"])
        return mapping

    def _apply_mapping(self, df: pl.DataFrame, mapping: Dict[str, str]) -> pl.DataFrame:
        for target_col, source_expr in mapping.items():
            try:
                df = df.with_columns(pl.expr(source_expr).alias(target_col))
            except Exception as e:
                logging.warning(
                    f"Failed to apply mapping for column {target_col}: {str(e)}"
                )
        return df


def create_chain(*functions: Callable) -> ChainedMTable:
    chain = ChainedMTable()

    for func in functions:
        if not hasattr(func, "function_name"):
            func = mtable()(func)
        chain.add(func)

    return chain
