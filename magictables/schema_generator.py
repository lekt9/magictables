import dataset
from typing import List, Dict, Any, Optional, Type
from pandera.typing import DataFrame as PanderaDataFrame
import os
import importlib
import sys

MAGIC_DB = "sqlite:///magic.db"
SCHEMA_DIR = "schemas"


def get_table_schema(table_name: str) -> List[Dict[str, Any]]:
    db = dataset.connect(MAGIC_DB)
    table = db[table_name]

    schema = []
    for column in table.columns:
        column_info = {
            "name": column,
            "type": str(table.table.columns[column].type),
            "nullable": table.table.columns[column].nullable,
            "default": table.table.columns[column].default,
        }
        schema.append(column_info)

    return schema


def generate_pandera_schema(table_name: str) -> str:
    schema = get_table_schema(table_name)

    model_fields = []
    imports = set(
        [
            "import pandera as pa",
            "from pandera.typing import DataFrame",
            "from pandera.engines import polars_engine",
        ]
    )

    for column in schema:
        field_name = column["name"]
        field_type = column["type"]

        if "INT" in field_type.upper():
            pa_type = "pa.Int64"
        elif "FLOAT" in field_type.upper() or "REAL" in field_type.upper():
            pa_type = "pa.Float64"
        elif "BOOL" in field_type.upper():
            pa_type = "pa.Boolean"
        elif "DATETIME" in field_type.upper():
            pa_type = "pa.DateTime"
        else:
            pa_type = "pa.String"

        if not column["nullable"]:
            model_fields.append(f"    {field_name}: {pa_type} = pa.Field()")
        else:
            model_fields.append(
                f"    {field_name}: Optional[{pa_type}] = pa.Field(nullable=True)"
            )
            imports.add("from typing import Optional")

    imports_str = "\n".join(sorted(imports))
    fields_str = "\n".join(model_fields)

    model = f"""{imports_str}

class {table_name.capitalize()}Model(pa.DataFrameModel):
    class Config:
        coerce = True
        engine = polars_engine

{fields_str}
"""
    return model


def generate_schemas_for_all_tables() -> Dict[str, str]:
    db = dataset.connect(MAGIC_DB)
    schemas = {}

    for table_name in db.tables:
        schemas[table_name] = generate_pandera_schema(table_name)

    return schemas


def load_schema_class(table_name: str) -> Optional[Type[PanderaDataFrame]]:
    module_name = f"{table_name}_schema"
    try:
        # Try to import the existing schema
        module = importlib.import_module(module_name)
        return getattr(module, f"{table_name.capitalize()}Model")
    except ImportError:
        # If the schema doesn't exist, generate it
        schema_code = generate_pandera_schema(table_name)

        # Ensure the schema directory exists
        os.makedirs(SCHEMA_DIR, exist_ok=True)

        # Write the schema to a file
        file_path = os.path.join(SCHEMA_DIR, f"{module_name}.py")
        with open(file_path, "w") as f:
            f.write(schema_code)

        # Add the schemas directory to sys.path if it's not already there
        if SCHEMA_DIR not in sys.path:
            sys.path.insert(0, SCHEMA_DIR)

        # Import the newly created module
        module = importlib.import_module(module_name)
        return getattr(module, f"{table_name.capitalize()}Model")


# Generate and save the schemas to a file
schemas = generate_schemas_for_all_tables()

with open("schemas.py", "w") as f:
    for schema_code in schemas.values():
        f.write(schema_code)
        f.write("\n\n")
