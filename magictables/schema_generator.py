import dataset
from typing import List, Dict, Any, Optional
import polars as pl
from pandera.engines import polars_engine as pa
from pandera.typing import DataFrame as PanderaDataFrame

MAGIC_DB = "sqlite:///magic.db"


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
            pa_type = "pa.Datetime"
        else:
            pa_type = "pa.String"

        if not column["nullable"]:
            model_fields.append(
                f"    {field_name}: {pa_type} = pa.Field(nullable=False)"
            )
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


# Generate and save the schemas to a file
schemas = generate_schemas_for_all_tables()

with open("schemas.py", "w") as f:
    for schema_code in schemas.values():
        f.write(schema_code)
        f.write("\n\n")
