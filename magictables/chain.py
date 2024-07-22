import polars as pl
from typing import List, Dict, Any, Optional
from .source import Source
from .magictables import INTERNAL_COLUMNS, MagicTable
from .utils import call_ai_model, flatten_nested_structure
import re
from urllib.parse import urlencode, urlparse, parse_qs


class Chain:
    def __init__(self):
        self.steps: List[Dict[str, Any]] = []
        self.magic_table: MagicTable = MagicTable.get_instance()
        self.analysis_query: Optional[str] = None
        self.result: Optional[pl.DataFrame] = None

    def add(self, source: Source, query: str, route_name: Optional[str] = None):
        self.steps.append({"source": source, "query": query, "route_name": route_name})
        return self

    def _populate_url(
        self, url_template: str, mapping: Dict[str, Dict[str, Any]], row: Dict[str, Any]
    ) -> str:
        parsed_url = urlparse(url_template)
        path = parsed_url.path
        query_params = parse_qs(parsed_url.query)

        print("Debug: path =", path)

        # Check if there are any placeholders in the path
        placeholders = re.findall(r"\{([^}]+)\}", path)
        print("Debug: placeholders found =", placeholders)

        if not placeholders:
            print("No placeholders found in the path. Using alternative method.")
            # Alternative method using re.search()
            placeholder = re.search(r"\{([^}]+)\}", path)
            while placeholder:
                placeholder_text = placeholder.group(1)
                print("Debug: processing placeholder:", placeholder_text)
                print("mapping", mapping)
                print("mapping", type(mapping))

                if placeholder_text in mapping:
                    print("pl", placeholder_text)
                    map_info = mapping[placeholder_text]
                    print("place", map_info)
                    source_col = map_info["source_column"]
                    value = row[map_info["source_column"]]

                    print("Debug: value =", value)

                    if map_info["transformation"]:
                        value = eval(
                            f"pl.Series([value]).{map_info['transformation']}"
                        )[0]

                    path = path.replace(f"{{{placeholder_text}}}", str(value))

                # Look for next placeholder
                placeholder = re.search(r"\{([^}]+)\}", path)
        else:
            # Original method using re.findall()
            for placeholder in placeholders:
                print("Debug: processing placeholder:", placeholder)
                print("mapping", mapping)
                print("mapping", type(mapping))

                if placeholder in mapping:
                    map_info = mapping[placeholder]
                    source_col = map_info["source_column"]
                    value = row[map_info["source_column"]]

                    print("Debug: value =", value)

                    if map_info["transformation"]:
                        value = eval(
                            f"pl.Series([value]).{map_info['transformation']}"
                        )[0]

                    path = path.replace(f"{{{placeholder}}}", str(value))

        # Rest of the function remains the same...

        # Populate query parameters
        for param, values in query_params.items():
            if param in mapping:
                map_info = mapping[param]
                source_col = map_info["source_column"]
                if source_col in row:
                    value = row[map_info["new_column"]]
                    print("Debug: query param value =", value)

                    if map_info["transformation"]:
                        value = eval(
                            f"pl.Series([value]).{map_info['transformation']}"
                        )[0]
                    query_params[param] = [str(value)]

        # Reconstruct the URL
        populated_url = parsed_url._replace(
            path=path, query=urlencode(query_params, doseq=True)
        ).geturl()

        print("Debug: final populated_url =", populated_url)
        return populated_url

    def execute(self, input_data: Optional[pl.DataFrame] = None) -> pl.DataFrame:
        current_data = input_data if input_data is not None else pl.DataFrame()
        for step in self.steps:
            source = step["source"]
            query = step["query"]
            route_name = step["route_name"]

            try:
                # Get the route details
                route = source.magic_table.get_route(source.name, route_name)
                if not route:
                    raise ValueError(
                        f"No route found for {source.name} with name {route_name}"
                    )

                # Extract parameters from the URL template
                url_template = route["url"]
                print("url_template", url_template)
                params = self._extract_params_from_url_template(url_template, route)
                print("params", params)
                # Check if there are any parameters to process
                if not params["path_params"] and not params["query_params"]:
                    # If no parameters, execute the source directly
                    step_result = source.execute(
                        input_data=current_data,
                        query=query,
                        route_name=route_name,
                        url=url_template,
                    )
                else:
                    # Generate column mapping options
                    mapping_options = self._generate_column_mappings(
                        current_data, params, query, route
                    )

                    print("Mapping options:", mapping_options)

                    for mapping in mapping_options:
                        failure_count = 0
                        while failure_count < 3:
                            transformed_data = current_data.clone()
                            try:
                                for param, map_info in mapping.items():
                                    # Check if the param already has a value
                                    if param in route and route[param]:
                                        continue  # Skip mapping if param already has a value

                                    source_col = map_info["source_column"]
                                    new_col = map_info["new_column"]
                                    transformation = map_info["transformation"]

                                    if source_col in transformed_data.columns:
                                        if transformation:
                                            # Check if the transformation is a list join operation
                                            if "list.join" in transformation:
                                                # First, cast the list elements to strings, then join
                                                transformed_data = (
                                                    transformed_data.with_columns(
                                                        pl.col(source_col)
                                                        .cast(pl.List(pl.Utf8))
                                                        .list.join(", ")
                                                        .alias(new_col)
                                                    )
                                                )
                                            else:
                                                # For other transformations, use the original logic
                                                transformed_data = (
                                                    transformed_data.with_columns(
                                                        eval(transformation).alias(
                                                            new_col
                                                        )
                                                    )
                                                )
                                        else:
                                            transformed_data = (
                                                transformed_data.with_columns(
                                                    pl.col(source_col).alias(new_col)
                                                )
                                            )
                                step_results = []

                                # Iterate over each row in the transformed data
                                for row in transformed_data.iter_rows(named=True):
                                    # Populate the URL with the transformed data for this row
                                    populated_url = self._populate_url(
                                        url_template, mapping, row
                                    )

                                    print("pop url", populated_url)
                                    # Execute the step with the transformed data and populated URL
                                    row_result = source.execute(
                                        input_data=pl.DataFrame([row]),
                                        query=query,
                                        route_name=route_name,
                                        url=populated_url,
                                    )
                                    step_results.append(row_result)

                                # Combine all row results
                                step_result = pl.concat(step_results)

                                # If successful, break both loops
                                print(f"Successful execution with mapping: {mapping}")
                                break

                            except Exception as e:
                                print(f"Execution failed with mapping {mapping}: {e}")
                                failure_count += 1
                                if failure_count == 3:
                                    print(
                                        f"Moving to next mapping option after 3 failures"
                                    )
                                continue

                        if failure_count < 3:
                            # If we've successfully processed the data, break the outer loop
                            break
                    else:
                        # If all mapping options fail, raise an exception
                        raise ValueError("All mapping options failed for this step")

                # Process the step_result (whether it came from direct execution or parameter mapping)
                if not step_result.is_empty():
                    step_result = step_result.select(
                        [
                            col
                            for col in step_result.columns
                            if col not in INTERNAL_COLUMNS
                        ]
                    )

                    flattened_result = flatten_nested_structure(
                        step_result.to_dict(as_series=False)
                    )

                    # Convert the flattened result back to a DataFrame
                    temp_df = pl.DataFrame(flattened_result)

                    # Expand the arrays into multiple rows
                    expanded_df = temp_df.explode(temp_df.columns)

                    # Update current_data with the expanded DataFrame
                    current_data = expanded_df
                else:
                    print("No valid results returned for this step")

            except Exception as e:
                print(f"Error executing step: {e}")
                # Instead of continuing, we'll return the current_data
                # This allows partial results to be returned if a step fails
                return current_data

        if self.analysis_query:
            current_data = self._analyze(current_data)

        self.result = current_data
        return self.result

    def _extract_params_from_url_template(
        self, url_template: str, route: Dict[str, Any]
    ) -> Dict[str, List[str]]:
        path_params = [
            param
            for param in re.findall(r"\{([^}]+)\}", url_template)
            if param not in route or not route[param]
        ]
        parsed_url = urlparse(url_template)
        query_params = parse_qs(parsed_url.query)
        print(query_params)

        # Filter out query params that are already populated in the URL template
        unpopulated_query_params = [
            param
            for param, values in query_params.items()
            if not values or not values[0] or values[0] == "{" + param + "}"
        ]

        return {"path_params": path_params, "query_params": unpopulated_query_params}

    def _generate_column_mappings(
        self,
        df: pl.DataFrame,
        params: Dict[str, List[str]],
        query: str,
        route: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        all_params = params["path_params"] + params["query_params"]

        column_types = {col: str(df[col].dtype) for col in df.columns}

        input_data = {
            "columns": df.columns,
            "column_types": column_types,
            "required_params": all_params,
            "query": query,
        }
        prompt = f"""
        Given the current DataFrame columns, their types, and the parameters needed for the URL template,
        suggest potential ways to map the columns to the required parameters.
        Only suggest mappings for columns that actually exist in the DataFrame.
        Order them by the most likely mapping to the least likely mapping. This usually means that id style mappings are preferred.
        Consider the query context and column types when determining the best mappings.
        For each mapping, provide a Polars expression to transform the data if necessary.
        
        Current columns and their types: {input_data['column_types']}
        Required parameters: {input_data['required_params']}
        Query context: {input_data['query']}

        Return a JSON object where keys are the required parameter names and values are objects containing:
        - 'source_column': The name of the source column (must exist in the DataFrame)
        - 'new_column': The name of the new column to create (should match the required parameter)
        - 'transformation': A Polars expression as a string to transform the data (or null if no transformation is needed)

        Only include mappings for parameters that can be derived from existing columns.
        """

        result = call_ai_model(input_data, prompt)

        # Convert the result to the expected format
        mapping_options = [{param: mapping} for param, mapping in result.items()]

        return mapping_options

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
        prompt = f"""
        Analyze the columns and sample data from two DataFrames (df1 and df2).
        Suggest the best way to join these DataFrames based on the given query context.
        Consider semantic relationships between columns, not just exact name matches.
        If a direct join is not possible, suggest creating a new column or transforming existing ones to enable a join.

        DataFrame 1 columns: {input_data['df1_columns']}
        DataFrame 1 sample: {input_data['df1_sample']}

        DataFrame 2 columns: {input_data['df2_columns']}
        DataFrame 2 sample: {input_data['df2_sample']}

        Query context: {input_data['query']}

        Return a JSON object with the following structure:
        {{
            "join_type": "inner" or "left" or "right" or "outer",
            "join_instructions": [
                {{
                    "df1_column": "column_name_from_df1",
                    "df2_column": "column_name_from_df2",
                    "transformation": "Polars expression as a string to transform the data (or null if no transformation is needed)"
                }},
                ...
            ]
        }}
        """
        return call_ai_model(input_data, prompt)

    def _apply_ai_join_instructions(
        self, df1: pl.DataFrame, df2: pl.DataFrame, instructions: Dict[str, Any]
    ) -> pl.DataFrame:
        join_type = instructions["join_type"]
        join_instructions = instructions["join_instructions"]

        # Apply transformations and prepare join columns
        df1_join_cols = []
        df2_join_cols = []

        for instr in join_instructions:
            df1_col = instr["df1_column"]
            df2_col = instr["df2_column"]
            transformation = instr.get("transformation")

            if transformation:
                df1 = df1.with_columns(eval(transformation.replace("df1", "pl")))
                df2 = df2.with_columns(eval(transformation.replace("df2", "pl")))

            df1_join_cols.append(df1_col)
            df2_join_cols.append(df2_col)

        # Perform the join
        if join_type == "inner":
            return df1.join(
                df2, left_on=df1_join_cols, right_on=df2_join_cols, how="inner"
            )
        elif join_type == "left":
            return df1.join(
                df2, left_on=df1_join_cols, right_on=df2_join_cols, how="left"
            )
        elif join_type == "right":
            return df1.join(
                df2, left_on=df1_join_cols, right_on=df2_join_cols, how="right"
            )
        elif join_type == "outer":
            return df1.join(
                df2, left_on=df1_join_cols, right_on=df2_join_cols, how="outer"
            )
        else:
            raise ValueError(f"Unsupported join type: {join_type}")

    def _get_merge_instructions(
        self, df1: pl.DataFrame, df2: pl.DataFrame, query: str
    ) -> Dict[str, Any]:
        common_columns = set(df1.columns) & set(df2.columns)
        if not common_columns:
            return {}

        input_data = {
            "common_columns": list(common_columns),
            "df1_columns": df1.columns,
            "df2_columns": df2.columns,
            "query": query,
        }
        prompt = f"""
        Given the common columns between two DataFrames and the query context,
        suggest the best way to merge these DataFrames.
        Consider the semantics of the columns and the query when making your decision.

        Common columns: {input_data['common_columns']}
        DataFrame 1 columns: {input_data['df1_columns']}
        DataFrame 2 columns: {input_data['df2_columns']}
        Query context: {input_data['query']}

        Return a JSON object with the following structure:
        {{
            "on": ["column1", "column2", ...],  # List of columns to join on
            "how": "inner" or "left" or "right" or "outer"  # Join method
        }}
        """
        return call_ai_model(input_data, prompt)

    def _apply_merge_instructions(
        self, df1: pl.DataFrame, df2: pl.DataFrame, instructions: Dict[str, Any]
    ) -> pl.DataFrame:
        on = instructions["on"]
        how = instructions["how"]
        return df1.join(df2, on=on, how=how)

    def _analyze(self, data: pl.DataFrame) -> pl.DataFrame:
        input_data = {
            "columns": data.columns,
            "sample_data": data.head().to_dict(as_series=False),
            "query": self.analysis_query,
        }
        prompt = f"""
        Analyze the given DataFrame and perform the requested analysis based on the query.
        Return a JSON object describing the operations to be performed on the DataFrame.
        The operations should be a list of dictionaries, each containing:
        - 'operation': The name of the Polars operation to perform
        - 'params': A dictionary of parameters for the operation

        Columns: {input_data['columns']}
        Sample data: {input_data['sample_data']}
        Query: {input_data['query']}

        Example return value:
        {{
            "operations": [
                {{"operation": "filter", "params": {{"column": "age", "predicate": "> 30"}}}},
                {{"operation": "groupby", "params": {{"by": ["category"], "agg": {{"sales": "sum"}}}}}},
                {{"operation": "sort", "params": {{"by": "sales", "descending": true}}}}
            ]
        }}
        """
        result = call_ai_model(input_data, prompt)

        for op in result["operations"]:
            operation = op["operation"]
            params = op["params"]

            if operation == "filter":
                data = data.filter(
                    eval(f"pl.col('{params['column']}') {params['predicate']}")
                )
            elif operation == "groupby":
                agg_dict = {col: getattr(pl, agg) for col, agg in params["agg"].items()}
                data = data.groupby(params["by"]).agg(agg_dict)
            elif operation == "sort":
                data = data.sort(
                    params["by"], descending=params.get("descending", False)
                )
            # Add more operations as needed

        return data

    def get_result(self) -> Optional[pl.DataFrame]:
        return self.result
