import dataset
from typing import List, Dict, Any

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


def generate_pydantic_model(table_name: str) -> str:
    schema = get_table_schema(table_name)

    model_fields = []
    imports = set(["from pydantic import BaseModel"])

    for column in schema:
        field_name = column["name"]
        field_type = column["type"]

        if "INT" in field_type.upper():
            py_type = "int"
        elif "FLOAT" in field_type.upper() or "REAL" in field_type.upper():
            py_type = "float"
        elif "BOOL" in field_type.upper():
            py_type = "bool"
        elif "DATETIME" in field_type.upper():
            py_type = "datetime.datetime"
            imports.add("import datetime")
        else:
            py_type = "str"

        if not column["nullable"]:
            model_fields.append(f"    {field_name}: {py_type}")
        else:
            model_fields.append(f"    {field_name}: Optional[{py_type}] = None")
            imports.add("from typing import Optional")

    imports_str = "\n".join(sorted(imports))
    fields_str = "\n".join(model_fields)

    model = f"""{imports_str}

class {table_name.capitalize()}Model(BaseModel):
{fields_str}
"""
    return model


def generate_models_for_all_tables() -> Dict[str, str]:
    db = dataset.connect(MAGIC_DB)
    models = {}

    for table_name in db.tables:
        models[table_name] = generate_pydantic_model(table_name)

    return models
