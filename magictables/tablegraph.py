from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from magictables.magictables import MagicTable
import logging
import os
import pickle
import pandas as pd
import networkx as nx
import polars as pl
from sklearn.metrics.pairwise import cosine_similarity
from py2neo import Graph as Neo4jGraph, Node, Relationship
from typing import Dict, Any, List, Optional, Tuple
from dotenv import load_dotenv

from magictables.utils import call_ai_model, generate_embeddings

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TableGraph:
    def __init__(
        self,
        backend=None,
        neo4j_uri=None,
        neo4j_user=None,
        neo4j_password=None,
        pickle_file="tablegraph.pkl",
    ):
        load_dotenv()  # Load environment variables from .env file if present

        if backend is None:
            # Check for Neo4j environment variables
            neo4j_uri = os.getenv("NEO4J_URI")
            neo4j_user = os.getenv("NEO4J_USER")
            neo4j_password = os.getenv("NEO4J_PASSWORD")

            if neo4j_uri and neo4j_user and neo4j_password:
                self.backend = "neo4j"
            else:
                self.backend = "memory"
        else:
            self.backend = backend

        self.pickle_file = pickle_file

        if self.backend == "memory":
            self.graph = nx.MultiGraph()
        elif self.backend == "neo4j":
            if not (neo4j_uri and neo4j_user and neo4j_password):
                raise ValueError(
                    "Neo4j credentials not found in environment variables."
                )
            try:
                self.graph = Neo4jGraph(neo4j_uri, auth=(neo4j_user, neo4j_password))
            except Exception as e:
                print(f"Failed to connect to Neo4j: {str(e)}")
                print("Falling back to memory backend.")
                self.backend = "memory"
                self.graph = nx.MultiGraph()
        else:
            raise ValueError("Unsupported backend. Use 'memory' or 'neo4j'.")

        self.dataframes = {}
        self.embeddings = {}

    async def add_dataframe(self, name: str, df: "MagicTable", source_name: str):
        self.dataframes[name] = df

        df_text = " ".join(df.columns) + " " + " ".join(df.dtypes.astype(str))
        self.embeddings[name] = (await generate_embeddings([df_text]))[0]

        transformations = await self._generate_potential_transformations(df)

        self.add_node(
            name,
            type="dataframe",
            columns=list(df.columns),
            source_name=source_name,
            transformations=transformations,
        )

    async def apply_chaining(
        self, source_df_name: str, target_df_name: str, query: str
    ) -> Tuple["MagicTable", "MagicTable"]:
        source_df = self.dataframes[source_df_name]
        target_df = self.dataframes[target_df_name]

        chaining = self.get_chaining_suggestions(source_df_name, target_df_name)
        if not chaining:
            chaining = await self._generate_chaining(source_df, target_df)

        if query not in chaining:
            raise ValueError(f"Chaining query '{query}' not found")

        chain_info = chaining[query]
        exec(chain_info["code"])

        self.dataframes[source_df_name] = source_df
        self.dataframes[target_df_name] = target_df

        return source_df, target_df

    def get_transformations(self, df_name: str) -> Dict[str, Any]:
        node = self.get_node(df_name)
        if node is None:
            return {}
        return node.get("transformations", {})

    async def find_similar_dataframes(
        self, df_name: str, top_k: int = 5
    ) -> List[Tuple[str, float]]:
        target_embedding = self.embeddings[df_name]
        similarities = []

        for name, embedding in self.embeddings.items():
            if name != df_name:
                similarity = cosine_similarity([target_embedding], [embedding])[0][0]
                similarities.append((name, similarity))

        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:top_k]

    async def execute_cypher(
        self, query: str, params: Dict[str, Any] = {}
    ) -> "MagicTable":
        if self.backend == "neo4j":
            try:
                from magictables.magictables import MagicTable

                result = self.graph.run(query, **params)
                return MagicTable(result.data())
            except Exception as e:
                logger.error(f"Error executing Cypher query: {str(e)}")
                raise
        else:
            logger.warning("Cypher queries are only supported with Neo4j backend")
            raise ValueError("Cypher queries are only supported with Neo4j backend")

    def add_node(self, node_name: str, **attrs):
        if self.backend == "memory":
            self.graph.add_node(node_name, **attrs)
        elif self.backend == "neo4j":
            node = Node("Dataframe", name=node_name, **attrs)
            self.graph.create(node)
        self._pickle_state()

    def get_node(self, node_name: str):
        if self.backend == "memory":
            return self.graph.nodes.get(node_name)
        elif self.backend == "neo4j":
            return self.graph.nodes.match("Dataframe", name=node_name).first()

    def _column_similarity(self, col1: pl.Series, col2: pl.Series) -> float:
        if col1.dtype == col2.dtype:
            unique_ratio = min(col1.n_unique(), col2.n_unique()) / max(
                col1.n_unique(), col2.n_unique()
            )
            return 0.5 + 0.5 * unique_ratio
        return 0

    async def _generate_potential_transformations(
        self, df: "MagicTable"
    ) -> Dict[str, Any]:
        existing_transformations = self.get_transformations(df.name)

        if existing_transformations:
            # If transformations exist, you might want to check if they're still valid
            # For example, you could compare the current columns with the stored ones
            stored_columns = self.get_node(df.name).get("columns", [])
            if set(df.columns) == set(stored_columns):
                return existing_transformations

        pandas_df = df.to_pandas()
        input_data = {
            "columns": list(pandas_df.columns),
            "dtypes": [str(dtype) for dtype in pandas_df.dtypes],
            "sample_data": pandas_df.head(5).to_dict(orient="records"),
        }
        prompt = f"""Generate potential transformations for each column in the pandas DataFrame. Include 'to_uppercase' and 'to_lowercase' for string columns, and suggest other relevant transformations based on the data types and sample data.

Current DataFrame structure:
Columns: {input_data['columns']}
Data Types: {input_data['dtypes']}

Sample Data:
{pd.DataFrame(input_data['sample_data']).to_string(index=False)}

Please provide Python code to generate transformations for this DataFrame, adhering to the following guidelines:
1. Only use pandas (as pd) and no other libraries.
2. Ensure all operations are efficient and use pandas vectorized operations where possible.
3. Handle potential data type issues, especially for date/time columns or numeric calculations.
4. The code should return a dictionary where keys are column names and values are lists of transformation dictionaries.
5. Each transformation dictionary should have 'name' and 'code' keys.
6. Do not include any print statements or comments in the code.
7. The input DataFrame is named 'df'.

Here are some examples of the kind of transformations we're looking for:

transformations = {{
    'title': [
        {{'name': 'to_uppercase', 'code': "df['title'] = df['title'].str.upper()"}},
        {{'name': 'to_lowercase', 'code': "df['title'] = df['title'].str.lower()"}},
        {{'name': 'capitalize_first', 'code': "df['title'] = df['title'].str.capitalize()"}}
    ],
    'release_date': [
        {{'name': 'to_datetime', 'code': "df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')"}},
        {{'name': 'extract_year', 'code': "df['release_year'] = df['release_date'].dt.year"}}
    ],
    'vote_average': [
        {{'name': 'round_to_nearest_half', 'code': "df['vote_average'] = (df['vote_average'] * 2).round() / 2"}},
        {{'name': 'to_percentage', 'code': "df['vote_percentage'] = df['vote_average'] * 10"}}
    ],
    'popularity': [
        {{'name': 'log_transform', 'code': "df['log_popularity'] = np.log1p(df['popularity'])"}},
        {{'name': 'normalize', 'code': "df['normalized_popularity'] = (df['popularity'] - df['popularity'].min()) / (df['popularity'].max() - df['popularity'].min())"}}
    ]
}}

Your response should be the Python code directly, without any JSON formatting.
"""

        code = await call_ai_model([], prompt, return_json=False)

        if not code:
            raise ValueError("Failed to generate transformation code")

        # Execute the generated code
        local_vars = {"pd": pd, "df": pandas_df}
        exec(code, globals(), local_vars)

        if "transformations" in local_vars and isinstance(
            local_vars["transformations"], dict
        ):
            transformations = local_vars["transformations"]
            self.update_node(df.name, transformations=transformations)
            return transformations
        else:
            raise ValueError("Generated code did not produce valid transformations")

    async def _generate_chaining(
        self, df1: "MagicTable", df2: "MagicTable"
    ) -> Dict[str, Any]:
        # First, check if chaining suggestions already exist for these DataFrames
        existing_suggestions = self.get_chaining_suggestions(df1.name, df2.name)

        if existing_suggestions:
            # Check if the suggestions are still valid
            # For example, you could compare the current columns with the stored ones
            stored_columns_df1 = self.get_node(df1.name).get("columns", [])
            stored_columns_df2 = self.get_node(df2.name).get("columns", [])
            if set(df1.columns) == set(stored_columns_df1) and set(df2.columns) == set(
                stored_columns_df2
            ):
                return existing_suggestions

        # If no suggestions exist or they're outdated, generate new ones
        pandas_df1 = df1.to_pandas()
        pandas_df2 = df2.to_pandas()
        input_data = {
            "df1": {
                "name": df1.name,
                "columns": list(pandas_df1.columns),
                "dtypes": [str(dtype) for dtype in pandas_df1.dtypes],
                "sample_data": pandas_df1.head(5).to_dict(orient="records"),
            },
            "df2": {
                "name": df2.name,
                "columns": list(pandas_df2.columns),
                "dtypes": [str(dtype) for dtype in pandas_df2.dtypes],
                "sample_data": pandas_df2.head(5).to_dict(orient="records"),
            },
        }
        prompt = f"""Generate a single chaining suggestion between the two pandas DataFrames. Consider column similarities, data types, and potential relationships. Provide a similarity score and chaining code for the suggestion.

        DataFrame 1:
        Name: {input_data['df1']['name']}
        Columns: {input_data['df1']['columns']}
        Data Types: {input_data['df1']['dtypes']}

        DataFrame 2:
        Name: {input_data['df2']['name']}
        Columns: {input_data['df2']['columns']}
        Data Types: {input_data['df2']['dtypes']}

        Sample Data (DataFrame 1):
        {pd.DataFrame(input_data['df1']['sample_data']).to_string(index=False)}

        Sample Data (DataFrame 2):
        {pd.DataFrame(input_data['df2']['sample_data']).to_string(index=False)}

        Please provide Python code to generate a single chaining suggestion between these DataFrames, adhering to the following guidelines:
        1. Only use pandas (as pd) and no other libraries.
        2. Ensure all operations are efficient and use pandas vectorized operations where possible.
        3. Handle potential data type issues, especially for date/time columns or numeric calculations.
        4. The code should return a tuple containing the chaining description (string), similarity score (float), and the chaining code (string).
        5. Do not include any print statements or comments in the code.
        6. The input DataFrames are named 'df1' and 'df2'.

        Example of expected output:
        def generate_chaining_suggestion(df1, df2):
            # Check if both DataFrames have a 'title' column
            if 'title' in df1.columns and 'title' in df2.columns:
                # Calculate similarity score based on matching titles
                common_titles = set(df1['title']) & set(df2['title'])
                similarity_score = len(common_titles) / min(len(df1), len(df2))
                
                # Generate chaining code
                chaining_code = "merged_df = pd.merge(df1, df2, on='title', how='inner')"
                
                return ("Merge DataFrames on 'title' column", similarity_score, chaining_code)
            
            # If no common column found, return a default suggestion
            return ("No direct chaining possible", 0.0, "# No direct chaining possible")

        chaining_suggestion = generate_chaining_suggestion(df1, df2)

        Your response should be the Python code directly, without any JSON formatting.
        """

        code = await call_ai_model([], prompt, return_json=False)

        if not code:
            raise ValueError("Failed to generate chaining code")

        # Execute the generated code
        local_vars = {"pd": pd, "df1": pandas_df1, "df2": pandas_df2}
        exec(code, globals(), local_vars)

        if "chaining_suggestion" in local_vars and isinstance(
            local_vars["chaining_suggestion"], tuple
        ):
            suggestion = {
                local_vars["chaining_suggestion"][0]: {
                    "similarity_score": local_vars["chaining_suggestion"][1],
                    "code": local_vars["chaining_suggestion"][2],
                }
            }

            # Store the new chaining suggestion in the graph
            self.update_chaining_suggestions(df1.name, df2.name, suggestion)

            return suggestion
        else:
            raise ValueError(
                "Generated code did not produce a valid chaining suggestion"
            )

    def get_chaining_suggestions(self, df1_name: str, df2_name: str) -> Dict[str, Any]:
        if self.backend == "memory":
            return self.graph.get_edge_data(
                df1_name, df2_name, key="chaining_suggestions", default={}
            )
        elif self.backend == "neo4j":
            query = """
            MATCH (df1:Dataframe {name: $df1_name})-[r:CHAINING_SUGGESTIONS]->(df2:Dataframe {name: $df2_name})
            RETURN r.suggestions as suggestions
            """
            result = self.graph.run(query, df1_name=df1_name, df2_name=df2_name).data()
            return result[0]["suggestions"] if result else {}

    def update_chaining_suggestions(
        self, df1_name: str, df2_name: str, suggestions: Dict[str, Any]
    ):
        if self.backend == "memory":
            self.graph.add_edge(
                df1_name, df2_name, key="chaining_suggestions", suggestions=suggestions
            )
        elif self.backend == "neo4j":
            query = """
            MERGE (df1:Dataframe {name: $df1_name})
            MERGE (df2:Dataframe {name: $df2_name})
            MERGE (df1)-[r:CHAINING_SUGGESTIONS]->(df2)
            SET r.suggestions = $suggestions
            """
            self.graph.run(
                query, df1_name=df1_name, df2_name=df2_name, suggestions=suggestions
            )
        self._pickle_state()

    def update_node(self, node_name: str, **attrs):
        if self.backend == "memory":
            if node_name not in self.graph.nodes:
                self.graph.add_node(node_name)
            self.graph.nodes[node_name].update(attrs)
        elif self.backend == "neo4j":
            query = """
            MERGE (df:Dataframe {name: $name})
            SET df += $attrs
            """
            self.graph.run(query, name=node_name, attrs=attrs)
        self._pickle_state()

    def _pickle_state(self):
        if self.backend == "memory":
            state = {
                "graph": self.graph,
                "dataframes": self.dataframes,
                "embeddings": self.embeddings,
            }
            with open(self.pickle_file, "wb") as f:
                pickle.dump(state, f)

    async def get_chained_result(
        self, source_df_name: str, api_url_template: str
    ) -> Optional["MagicTable"]:
        from magictables.magictables import MagicTable

        if self.backend == "memory":
            key = (source_df_name, api_url_template)
            if key in self.graph.nodes:
                node_data = self.graph.nodes[key]
                if "chained_result" in node_data:
                    return MagicTable(node_data["chained_result"])
        elif self.backend == "neo4j":
            query = """
            MATCH (source:DataFrame {name: $source_name})
            -[:CHAINED_TO {api_url_template: $api_url_template}]->
            (result:ChainedResult)
            RETURN result.data AS chained_result
            """
            result = self.graph.run(
                query, source_name=source_df_name, api_url_template=api_url_template
            ).data()
            if result:
                chained_result = result[0]["chained_result"]
                return MagicTable(pickle.loads(chained_result))
        return None

    async def store_chained_result(
        self, source_df_name: str, api_url_template: str, result_df: "MagicTable"
    ):
        if self.backend == "memory":
            key = (source_df_name, api_url_template)
            self.graph.add_node(key, chained_result=result_df.to_dict(as_series=False))
            self.graph.add_edge(source_df_name, key, api_url_template=api_url_template)
        elif self.backend == "neo4j":
            source_node = Node("DataFrame", name=source_df_name)
            result_node = Node(
                "ChainedResult", data=pickle.dumps(result_df.to_dict(as_series=False))
            )
            relation = Relationship(
                source_node,
                "CHAINED_TO",
                result_node,
                api_url_template=api_url_template,
            )

            tx = self.graph.begin()
            tx.merge(source_node, "DataFrame", "name")
            tx.create(result_node)
            tx.create(relation)
            tx.commit()

        self._pickle_state()

    async def add_transformation(self, df_name: str, query: str, code: str):
        transformations = self.get_transformations(df_name)

        # Add the new transformation
        if df_name not in transformations:
            transformations[df_name] = {}

        transformations[df_name][query] = {"query": query, "code": code}

        # Update the node with the new transformations
        self.update_node(df_name, transformations=transformations)

        # Pickle the updated state
        self._pickle_state()
