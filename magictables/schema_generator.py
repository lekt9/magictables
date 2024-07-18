from typing import Dict, Union, List, Set, Optional, Tuple, Any
from datetime import datetime
import os
import sqlite3


def get_table_schema(
    cursor: sqlite3.Cursor, table_name: str
) -> Dict[str, Union[str, Dict, Tuple[str, bool]]]:
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    print(f"Table info for {table_name}:", columns)  # Debug print
    schema = {
        col[1]: (col[2], col[3] == 0)
        for col in columns
        if col[1] != "id" and col[1] != "timestamp"
    }
    # ...
    # Check for nested tables
    cursor.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '{table_name}_%'"
    )
    nested_tables = cursor.fetchall()
    for (nested_table_name,) in nested_tables:
        nested_schema = get_table_schema(cursor, nested_table_name)
        schema[nested_table_name[len(table_name) + 1 :]] = nested_schema

    return schema


def update_generated_types(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()

    type_definitions = [
        "from typing import Any, Dict, List, TypedDict, Optional, Union\n"
        "from datetime import date, datetime\n\n"
    ]
    generated_classes = set()

    for (table_name,) in tables:
        if table_name.startswith("magic_") or table_name.startswith("ai_"):
            schema = get_table_schema(cursor, table_name)
            type_definition = generate_type_definition(
                table_name.replace("magic_", "").replace("ai_", ""),
                schema,
                generated_classes,
            )
            type_definitions.append(type_definition + "\n\n")

    # Sort type definitions to ensure nested classes are defined first
    sorted_type_definitions = sorted(
        type_definitions[1:], key=lambda x: len(x.split("\n")), reverse=True
    )
    type_definitions = [type_definitions[0]] + sorted_type_definitions

    # Get the directory of the script being run
    current_dir = os.getcwd()

    # Create the magictables_types directory if it doesn't exist
    types_dir = os.path.join(current_dir, "magictables_types")
    os.makedirs(types_dir, exist_ok=True)

    # Write the generated types to a file in the magictables_types directory
    with open(os.path.join(types_dir, "generated_types.py"), "w") as f:
        f.write("\n".join(type_definitions))

    # Update the references
    with open(os.path.join(types_dir, "generated_types.py"), "r") as f:
        content = f.read()

    content = content.replace("List['", "List[")
    content = content.replace("']", "]")

    with open(os.path.join(types_dir, "generated_types.py"), "w") as f:
        f.write(content)


def generate_type_definition(
    table_name: str,
    schema: Dict[str, Union[str, Dict, Tuple[str, bool]]],
    generated_classes: Set[str] = set(),
) -> str:
    class_name = "".join(word.capitalize() for word in table_name.split("_")) + "Result"
    if class_name in generated_classes:
        return ""

    generated_classes.add(class_name)
    fields = []
    nested_definitions = []

    for column, dtype_info in schema.items():
        print("info", dtype_info)
        if isinstance(dtype_info, dict):
            nested_class_name = (
                "".join(
                    word.capitalize() for word in f"{table_name}_{column}".split("_")
                )
                + "Result"
            )
            nested_type_definition = generate_type_definition(
                f"{table_name}_{column}", dtype_info, generated_classes
            )
            nested_definitions.append(nested_type_definition)
            fields.append(f"    {column}: List[{nested_class_name}]")
        else:
            dtype, is_nullable = dtype_info
            if dtype == "TEXT":
                field_type = "str"
            elif dtype == "INTEGER":
                field_type = "int"
            elif dtype == "REAL":
                field_type = "float"
            elif dtype == "BLOB":
                field_type = "bytes"
            elif dtype == "BOOLEAN":
                field_type = "bool"
            elif dtype == "DATE" or dtype == "DATETIME":
                field_type = "datetime"
            else:
                print("type", dtype, is_nullable)
                field_type = "Any"  # Fallback for unknown types

            if is_nullable:
                fields.append(f"    {column}: Optional[{field_type}]")
            else:
                fields.append(f"    {column}: {field_type}")

    class_definition = f"class {class_name}(TypedDict, total=False):\n"
    class_definition += "\n".join(fields)

    return "\n\n".join(nested_definitions + [class_definition])


def get_type_hint(func_name: str):
    try:
        import magictables_types.generated_types as generated_types

        # Convert func_name to PascalCase
        class_name = (
            "".join(word.capitalize() for word in func_name.split("_")) + "Result"
        )
        return getattr(generated_types, class_name)
    except (ImportError, AttributeError):
        return None
