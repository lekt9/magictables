import logging
from typing import List, Callable, Any, Union, Dict
from magictables import mtable
import polars as pl
from .utils import call_ai_model, generate_call_id
from .database import magic_db
from .mapping import store_mapping


class ChainedMTable:
    def __init__(self):
        self.functions: List[Callable] = []

    def add(self, func: Callable) -> "ChainedMTable":
        self.functions.append(func)
        return self

    def execute(self, initial_input: Any) -> pl.DataFrame:
        result = initial_input
        for i, func in enumerate(self.functions):
            call_id = generate_call_id(func, result)
            table_name = f"magic_{func.__name__}"

            logging.debug(f"Executing function: {func.__name__}")
            logging.debug(
                f"Input data shape: {result.shape if isinstance(result, pl.DataFrame) else 'Not a DataFrame'}"
            )
            logging.debug(
                f"Input data columns: {result.columns if isinstance(result, pl.DataFrame) else 'Not a DataFrame'}"
            )

            # Check cache first
            cached_result = magic_db.get_cached_result(table_name, call_id)
            if cached_result is not None and not cached_result.is_empty():
                print(f"Cache hit for {func.__name__}")
                raw_df = cached_result
            else:
                print(f"Cache miss for {func.__name__}")
                # Execute the current function
                if i == 0 and not func.__annotations__.get("return"):
                    # If it's the first function and it doesn't expect any input, call it without arguments
                    raw_df = func()
                else:
                    raw_df = func(result)
                # Cache the raw results
                magic_db.cache_results(table_name, raw_df, call_id)

            if i > 0:
                prev_output_schema = getattr(
                    self.functions[i - 1], "output_schema", None
                )
                current_input_schema = getattr(func, "input_schema", None)

                if prev_output_schema and current_input_schema:
                    # Generate a unique key for this schema pair
                    schema_key = self._generate_schema_key(
                        prev_output_schema, current_input_schema
                    )

                    if schema_key not in self.schema_mappings:
                        # Generate and store the mapping only if it doesn't exist
                        mapping_prompt = f"Generate a mapping between the previous output schema: {prev_output_schema} and the current input schema: {current_input_schema}"
                        mapping_result = call_ai_model(
                            [{"role": "user", "content": mapping_prompt}]
                        )
                        mapping = mapping_result[0]
                        self.schema_mappings[schema_key] = mapping

                        # Store the mapping in the database
                        store_mapping(schema_key, mapping)
                    else:
                        mapping = self.schema_mappings[schema_key]

                    # Apply the mapping
                    result = self._apply_mapping(raw_df, mapping)
                else:
                    result = raw_df

            else:
                result = raw_df

            # Apply the function's transformation
            if hasattr(func, "query"):
                result = transform_data_with_query(
                    result,
                    func.query,
                    func.__name__,
                    getattr(func, "output_schema", {}),
                )
            logging.debug(
                f"Output data shape: {result.shape if isinstance(result, pl.DataFrame) else 'Not a DataFrame'}"
            )
            logging.debug(
                f"Output data columns: {result.columns if isinstance(result, pl.DataFrame) else 'Not a DataFrame'}"
            )

        return result

    def _apply_mapping(self, df: pl.DataFrame, mapping: Dict[str, str]) -> pl.DataFrame:
        for target_col, source_expr in mapping.items():
            df = df.with_columns(pl.expr(source_expr).alias(target_col))
        return df

    def _generate_schema_key(self, prev_schema: Dict, current_schema: Dict) -> str:
        # Generate a unique key based on the two schemas
        combined_schema = json.dumps(
            {"prev": prev_schema, "current": current_schema}, sort_keys=True
        )
        return hashlib.md5(combined_schema.encode()).hexdigest()


# ... (rest of the code remains the same)


def create_chain(*functions: Union[Callable, Dict[str, Any]]) -> ChainedMTable:
    """
    Create a ChainedMTable from a list of functions or function configurations.

    Args:
        *functions: A list of functions decorated with @mtable or dictionaries
                    containing function configurations.

    Returns:
        ChainedMTable: A configured ChainedMTable instance.

    Example:
        chain = create_chain(
            get_user_data,
            {"func": calculate_bmi, "query": "Calculate BMI and categorize it"},
            analyze_bmi_by_age_group
        )
    """
    chain = ChainedMTable()

    for func in functions:
        if isinstance(func, dict):
            if "func" not in func:
                raise ValueError("Function configuration must contain 'func' key")

            base_func = func["func"]
            query = func.get("query")
            input_schema = func.get("input_schema")
            output_schema = func.get("output_schema")

            decorated_func = mtable(
                query=query, input_schema=input_schema, output_schema=output_schema
            )(base_func)
            chain.add(decorated_func)
        else:
            chain.add(func)

    return chain
