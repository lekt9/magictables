# magictables/schema_generator.py

import os
import sqlite3
from typing import Dict, Any


def get_table_schema(cursor: sqlite3.Cursor, table_name: str) -> Dict[str, str]:
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    return {
        col[1]: col[2] for col in columns if col[1] != "id" and col[1] != "timestamp"
    }


def generate_type_definition(table_name: str, schema: Dict[str, str]) -> str:
    class_name = f"{table_name.capitalize()}Result"
    fields = []
    for column, dtype in schema.items():
        if dtype == "TEXT":
            field_type = "str"
        elif dtype == "INTEGER":
            field_type = "int"
        elif dtype == "REAL":
            field_type = "float"
        elif dtype == "BLOB":
            field_type = "Any"
        else:
            field_type = "Any"
        fields.append(f"    {column}: {field_type}")

    class_definition = f"class {class_name}(TypedDict):\n"
    class_definition += "\n".join(fields)

    return class_definition


def update_generated_types(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()

    type_definitions = ["from typing import Any, Dict, List, TypedDict, Optional\n\n"]

    for (table_name,) in tables:
        if table_name.startswith("magic_") or table_name.startswith("ai_"):
            schema = get_table_schema(cursor, table_name)
            type_definition = generate_type_definition(
                table_name.replace("magic_", "").replace("ai_", ""), schema
            )
            type_definitions.append(type_definition + "\n\n")

    # Get the directory of the script being run
    current_dir = os.getcwd()

    # Create the magictables_types directory if it doesn't exist
    types_dir = os.path.join(current_dir, "magictables_types")
    os.makedirs(types_dir, exist_ok=True)

    # Write the generated types to a file in the magictables_types directory
    with open(os.path.join(types_dir, "generated_types.py"), "w") as f:
        f.write("\n".join(type_definitions))


def get_type_hint(func_name: str):
    try:
        import magictables_types.generated_types as generated_types

        return getattr(generated_types, f"{func_name.capitalize()}Result")
    except (ImportError, AttributeError):
        return None
