import polars as pl
from typing import List, Callable, Any, Dict, Optional
import logging

from .mapping import get_stored_mapping
from .utils import apply_mapping, generate_ai_mapping, call_ai_model


class ChainedMTable:
    def __init__(self, *functions):
        self.functions = functions

    def execute(self, input_data: Dict[str, Any]) -> pl.DataFrame:
        result = None
        for func in self.functions:
            if result is None:
                result = func(input_data)
            else:
                func_params = getattr(func, "func_params", {})
                table_name = f"magic_{func.__name__}"
                stored_mapping = get_stored_mapping(table_name)

                if stored_mapping:
                    # Apply the stored mapping
                    result = apply_mapping(result, stored_mapping)

                extracted_params = self._extract_params(result, func, func_params)
                result = func(**extracted_params)
        return result

    def _extract_params(
        self, df: pl.DataFrame, func: Callable, func_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        params = {}
        logging.debug(f"DataFrame columns: {df.columns}")
        logging.debug(f"Function parameters: {func_params}")

        required_params = [param for param in func_params if param != "return"]

        # Try to extract parameters using existing methods
        for param_name in required_params:
            if param_name in df.columns:
                params[param_name] = df[param_name].to_list()
            elif param_name.lower() in [col.lower() for col in df.columns]:
                matching_col = next(
                    col for col in df.columns if col.lower() == param_name.lower()
                )
                params[param_name] = df[matching_col].to_list()

        # If not all required parameters are found, use AI to generate mapping
        if len(params) < len(required_params):
            missing_params = [param for param in required_params if param not in params]
            logging.info(
                f"Missing parameters: {missing_params}. Generating AI mapping."
            )

            max_attempts = 3
            for attempt in range(max_attempts):
                ai_mapping = generate_ai_mapping(df, missing_params)

                success = True
                for param, expr in ai_mapping.items():
                    try:
                        params[param] = df.select(pl.eval(expr)).to_series().to_list()
                    except Exception as e:
                        logging.error(
                            f"Error applying AI-generated mapping for {param}: {str(e)}"
                        )
                        success = False
                        break

                if success:
                    break
                else:
                    logging.warning(
                        f"AI mapping attempt {attempt + 1} failed. Retrying..."
                    )

            if not success:
                logging.error(
                    "All AI mapping attempts failed. Falling back to column guessing."
                )

        # If still missing parameters, use AI to guess the most appropriate column
        if len(params) < len(required_params):
            missing_params = [param for param in required_params if param not in params]
            for param in missing_params:
                ai_guess = self._guess_column_for_param(df, param, func.__name__)
                if ai_guess:
                    params[param] = df[ai_guess].to_list()

        # Ensure single-value parameters are not passed as lists
        for param, value in params.items():
            if isinstance(value, list) and len(value) == 1:
                params[param] = value[0]

        logging.debug(f"Extracted parameters for {func.__name__}: {params}")
        return params

    def _merge_dataframes(self, df1: pl.DataFrame, df2: pl.DataFrame) -> pl.DataFrame:
        # Identify common columns for merging
        common_columns = set(df1.columns).intersection(set(df2.columns))

        if not common_columns:
            logging.warning(
                "No common columns found. Attempting to identify join columns using AI."
            )
            join_columns = self._identify_join_columns_with_ai(df1, df2)
            if join_columns:
                return self._perform_join(df1, df2, join_columns)
            else:
                logging.warning(
                    "Unable to identify join columns. Performing a cross join."
                )
                return df1.join(df2, how="cross")

        # Use the first common column as the join key
        join_key = list(common_columns)[0]
        logging.info(f"Merging DataFrames on column: {join_key}")

        return self._perform_join(df1, df2, [join_key])

    # def execute(self, initial_input: Any) -> pl.DataFrame:
    #     result = ensure_dataframe(initial_input)
    #     for i, func in enumerate(self.functions):
    #         logging.debug(f"Executing function: {func.__name__}")
    #         try:
    #             if i > 0:
    #                 # Extract parameters for the next function
    #                 params = self._extract_params(result, func)
    #                 missing_params = [
    #                     param
    #                     for param in func.__annotations__
    #                     if param not in params and param != "return"
    #                 ]
    #                 if missing_params:
    #                     raise ValueError(
    #                         f"Missing required parameters for {func.__name__}: {', '.join(missing_params)}"
    #                     )
    #                 new_df = func(**params)
    #             else:
    #                 new_df = func(initial_input)

    #             # Ensure the result is a DataFrame
    #             new_df = ensure_dataframe(new_df)

    #             if new_df.is_empty():
    #                 logging.warning(
    #                     f"Function {func.__name__} returned an empty DataFrame. Stopping chain execution."
    #                 )
    #                 break

    #             if i > 0:
    #                 # Merge the new DataFrame with the previous result
    #                 result = self._merge_dataframes(result, new_df)
    #             else:
    #                 result = new_df
    #         except Exception as e:
    #             logging.error(f"Error executing function {func.__name__}: {str(e)}")
    #             break  # Stop execution if there's an error

    #     return result

    def _perform_join(
        self, df1: pl.DataFrame, df2: pl.DataFrame, join_columns: List[str]
    ) -> pl.DataFrame:
        # Perform the merge
        merged_df = df1.join(df2, on=join_columns, how="outer")

        # Handle duplicate columns (except the join columns)
        for col in set(df1.columns).intersection(set(df2.columns)):
            if col not in join_columns:
                merged_df = merged_df.drop(f"{col}_right")
                merged_df = merged_df.rename({f"{col}_left": col})

        return merged_df

    def _identify_join_columns_with_ai(
        self, df1: pl.DataFrame, df2: pl.DataFrame
    ) -> Optional[List[str]]:
        # Function to get a sample of the DataFrame as a list of dicts
        def get_sample(df: pl.DataFrame, n: int = 5) -> List[Dict]:
            return df.sample(n=min(n, len(df))).to_dicts()

        sample1 = get_sample(df1)
        sample2 = get_sample(df2)

        query = f"""
        Given two DataFrames with the following columns and sample data:

        DataFrame 1:
        Columns: {df1.columns}
        Sample data: {sample1}

        DataFrame 2:
        Columns: {df2.columns}
        Sample data: {sample2}

        Identify the most appropriate columns to join these DataFrames on.
        Consider the following:
        1. Semantic similarities between column names
        2. Data types and formats of the columns
        3. Unique identifiers or potential foreign key relationships
        4. Common prefixes or suffixes in column names
        5. The actual content of the columns in the sample data

        Return a list of column pairs (one from each DataFrame) that could be used for joining.
        If no suitable join columns can be identified, return an empty list.

        Format your response as a Python list of tuples, e.g.:
        [("column1_df1", "column1_df2"), ("column2_df1", "column2_df2")]

        Provide a brief explanation for your choice of join columns.
        """

        df_info = {
            "df1_columns": df1.columns,
            "df2_columns": df2.columns,
            "df1_sample": sample1,
            "df2_sample": sample2,
        }
        ai_response = call_ai_model([df_info], prompt=query)

        if (
            ai_response
            and isinstance(ai_response[0], dict)
            and "join_columns" in ai_response[0]
        ):
            join_columns = ai_response[0]["join_columns"]
            explanation = ai_response[0].get("explanation", "No explanation provided")
            logging.info(f"AI-identified join columns: {join_columns}")
            logging.info(f"Explanation: {explanation}")
            return join_columns
        else:
            logging.warning("AI could not identify suitable join columns.")
            return None

    def _guess_column_for_param(
        self, df: pl.DataFrame, param_name: str, func_name: str
    ) -> Optional[str]:
        prompt = f"""
        Given a DataFrame with the following columns:
        {df.columns}

        And a function named '{func_name}' that requires a parameter '{param_name}',
        which column from the DataFrame would be the most appropriate to use for this parameter?

        Consider the following:
        1. Semantic similarities between the parameter name and column names
        2. Data types of the columns
        3. Common naming conventions for IDs or foreign keys

        Return the name of the most appropriate column, or None if no suitable column can be identified.
        Put it in a JSON object for {{
            "column": <column name>
        }}
        """

        ai_response = call_ai_model(
            {"columns": df.columns, "param_name": param_name, "func_name": func_name},
            prompt,
        )

        if ai_response and isinstance(ai_response, dict) and "column" in ai_response:
            guessed_column = ai_response["column"]
            if guessed_column in df.columns:
                logging.info(
                    f"AI guessed column '{guessed_column}' for parameter '{param_name}'"
                )
                return guessed_column
            else:
                logging.warning(
                    f"AI guessed column '{guessed_column}' is not in the DataFrame"
                )

        logging.warning(
            f"AI could not guess a suitable column for parameter '{param_name}'"
        )
        return None


def create_chain(*functions: Callable) -> ChainedMTable:
    return ChainedMTable(*functions)
    # chain = ChainedMTable()

    # for i, func in enumerate(functions):
    #     if not hasattr(func, "function_name"):
    #         func = mtable()(func)

    #     # If there's a next function, pass its parameters to the current function
    #     if i < len(functions) - 1:
    #         next_func = functions[i + 1]
    #         next_func_params = {
    #             param: param_type
    #             for param, param_type in next_func.__annotations__.items()
    #             if param != "return"
    #         }
    #         func = add_next_function_params(func)
    #         func.next_function_params = next_func_params

    #     chain.add(func)

    # return chain


def add_next_function_params(func: Callable) -> Callable:
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    wrapper.next_function_params = {}
    return wrapper
