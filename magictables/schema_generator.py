from typing import Dict, Union, List, Set, Optional, Tuple, Any
from datetime import datetime
import os
import importlib.util
import sqlite3

from magictables.database import get_connection
from magictables.utils import generate_ai_descriptions


def get_table_schema(
    cursor: sqlite3.Cursor, table_name: str
) -> Dict[
    str, Union[str, Dict[str, Union[str, Dict, Tuple[str, bool]]], Tuple[str, bool]]
]:
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    print(f"Table info for {table_name}:", columns)  # Debug print
    schema = {
        col[1]: (col[2], col[3] == 0)
        for col in columns
        if col[1] and col[1] != "id" and col[1] != "timestamp"
    }
    # Check for nested tables
    cursor.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '{table_name}_%'"
    )
    nested_tables = cursor.fetchall()
    for (nested_table_name,) in nested_tables:
        nested_schema = get_table_schema(cursor, nested_table_name)
        schema[nested_table_name[len(table_name) + 1 :]] = nested_schema

    return schema


def get_type_hint(func_name: str) -> Optional[Any]:
    current_dir = os.getcwd()
    types_file = os.path.join(current_dir, "magictables_types", "generated_types.py")

    if not os.path.exists(types_file):
        print(f"Warning: {types_file} not found. Types may need to be generated.")
        return None

    spec = importlib.util.spec_from_file_location("generated_types", types_file)
    if spec is None or spec.loader is None:
        print(f"Error: Could not load spec for {types_file}")
        return None

    generated_types = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generated_types)

    class_name = "".join(word.capitalize() for word in func_name.split("_")) + "Result"

    type_hint = getattr(generated_types, class_name, None)
    if type_hint:
        # Add AI descriptions to the type hint
        table_name = f"magic_{func_name}"
        with get_connection() as (conn, cursor):
            schema = get_table_schema(cursor, table_name)
        ai_descriptions = generate_ai_descriptions(table_name, list(schema.keys()))
        type_hint.__doc__ = ai_descriptions["table_description"]
        for field in type_hint.__annotations__:
            if field in ai_descriptions["column_descriptions"]:
                type_hint.__annotations__[field] = (
                    type_hint.__annotations__[field],
                    ai_descriptions["column_descriptions"][field],
                )

    return type_hint


def update_generated_types(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()

    type_definitions = [
        "from typing import Any, Dict, List, TypedDict, Optional, Union\n"
        "from datetime import date, datetime\n\n"
    ]
    generated_classes = set()
    class_dependencies = {}

    # First pass: generate all type definitions and track dependencies
    for (table_name,) in tables:
        if table_name.startswith("magic_") or table_name.startswith("ai_"):
            schema = get_table_schema(cursor, table_name)
            clean_table_name = table_name.replace("magic_", "").replace("ai_", "")
            type_definition, dependencies = generate_type_definition(
                clean_table_name,
                schema,
                generated_classes,
            )
            class_name = (
                "".join(word.capitalize() for word in clean_table_name.split("_"))
                + "Result"
            )
            class_dependencies[class_name] = dependencies
            type_definitions.append(type_definition)

    # Topological sort to order classes
    sorted_classes = topological_sort(class_dependencies)

    # Reorder type definitions based on sorted classes
    ordered_type_definitions = []
    for class_name in sorted_classes:
        for type_def in type_definitions:
            if type_def.startswith(f"class {class_name}("):
                ordered_type_definitions.append(type_def)
                break

    # Get the directory of the script being run
    current_dir = os.getcwd()

    # Create the magictables_types directory if it doesn't exist
    types_dir = os.path.join(current_dir, "magictables_types")
    os.makedirs(types_dir, exist_ok=True)

    # Write the generated types to a file in the magictables_types directory
    with open(os.path.join(types_dir, "generated_types.py"), "w") as f:
        f.write(type_definitions[0])  # Write imports
        f.write("\n".join(ordered_type_definitions))

    # Update the references
    with open(os.path.join(types_dir, "generated_types.py"), "r") as f:
        content = f.read()

    content = content.replace("List['", "List[")
    content = content.replace("']", "]")

    with open(os.path.join(types_dir, "generated_types.py"), "w") as f:
        f.write(content)


def generate_type_definition(
    table_name: str,
    schema: Dict[
        str, Union[str, Dict[str, Union[str, Dict, Tuple[str, bool]]], Tuple[str, bool]]
    ],
    generated_classes: Set[str] = set(),
) -> Tuple[str, Set[str]]:
    class_name = "".join(word.capitalize() for word in table_name.split("_")) + "Result"
    if class_name in generated_classes:
        return "", set()

    generated_classes.add(class_name)
    fields = []
    nested_definitions = []
    dependencies = set()

    for column, dtype_info in schema.items():
        if not column:  # Skip unnamed columns
            continue
        if isinstance(dtype_info, dict):
            nested_class_name = (
                "".join(
                    word.capitalize() for word in f"{table_name}_{column}".split("_")
                )
                + "Result"
            )
            nested_type_definition, nested_deps = generate_type_definition(
                f"{table_name}_{column}", dtype_info, generated_classes
            )
            nested_definitions.append(nested_type_definition)
            fields.append(f"    {column}: List[{nested_class_name}]")
            dependencies.add(nested_class_name)
            dependencies.update(nested_deps)
        else:
            dtype, is_nullable = dtype_info
            field_type = get_field_type(dtype)
            if is_nullable:
                fields.append(f"    {column}: Optional[{field_type}]")
            else:
                fields.append(f"    {column}: {field_type}")

    class_definition = f"class {class_name}(TypedDict, total=False):\n"
    class_definition += "\n".join(fields)

    return "\n\n".join(nested_definitions + [class_definition]), dependencies


def get_field_type(dtype: str) -> str:
    if dtype == "TEXT":
        return "str"
    elif dtype == "INTEGER":
        return "int"
    elif dtype == "REAL":
        return "float"
    elif dtype == "BLOB":
        return "bytes"
    elif dtype == "BOOLEAN":
        return "bool"
    elif dtype == "DATE" or dtype == "DATETIME":
        return "datetime"
    else:
        return "Any"  # Fallback for unknown types


def topological_sort(graph: Dict[str, Set[str]]) -> List[str]:
    result = []
    seen = set()

    def dfs(node):
        if node in seen:
            return
        seen.add(node)
        for neighbor in graph.get(node, []):
            dfs(neighbor)
        result.append(node)

    for node in graph:
        dfs(node)

    return list(reversed(result))
