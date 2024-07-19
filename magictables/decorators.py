import functools
import hashlib
import json
import pandas as pd
import sqlite3
from typing import (
    Any,
    Callable,
    Optional,
    Union,
    List,
    Dict,
    TypeVar,
    cast,
    get_type_hints,
    Generic,
)
from contextlib import contextmanager

from magictables.database import (
    cache_results,
    cache_result,
    check_cache_and_get_new_items,
    create_table,
    get_cached_result,
    get_connection,
    infer_sqlite_type,
    sanitize_sql_name,
    update_table_schema,
)
from magictables.utils import call_ai_model, generate_ai_descriptions

T = TypeVar("T", bound=Callable[..., Any])

RowType = TypeVar("RowType")


class MagicDataFrame(pd.DataFrame, Generic[RowType]):
    def __init__(self, data=None, *args, **kwargs):
        super().__init__(data=data, *args, **kwargs)

    @property
    def _constructor(self):
        return MagicDataFrame

    def __getstate__(self):
        # Get the parent's __dict__ and remove the axis attributes
        state = super().__getstate__()
        for attr in self._internal_names:
            if attr in state:
                del state[attr]
        return state

    def __setstate__(self, state):
        super().__setstate__(state)


def generate_call_id(func: Callable, *args: Any, **kwargs: Any) -> str:
    """Generate a unique ID for a function call."""

    def serialize(obj):
        if isinstance(obj, MagicDataFrame):
            return obj.to_dict("records")
        elif isinstance(obj, pd.DataFrame):
            return obj.to_dict("records")
        raise TypeError(
            f"Object of type {obj.__class__.__name__} is not JSON serializable"
        )

    call_data = (func.__name__, args, kwargs)
    return hashlib.md5(
        json.dumps(call_data, sort_keys=True, default=serialize).encode()
    ).hexdigest()


def ensure_dataframe(result: Any) -> MagicDataFrame:
    """Ensure the result is a MagicDataFrame."""
    if isinstance(result, MagicDataFrame):
        return result
    elif isinstance(result, pd.DataFrame):
        return MagicDataFrame(result)
    elif isinstance(result, dict):
        return MagicDataFrame([result])
    elif isinstance(result, list) and all(isinstance(item, dict) for item in result):
        return MagicDataFrame(result)
    else:
        return MagicDataFrame({"result": [result]})


def mtable(func: Optional[Callable] = None) -> Callable[[T], T]:
    def decorator(f: T) -> T:
        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> MagicDataFrame:
            call_id = generate_call_id(f, *args, **kwargs)
            table_name = sanitize_sql_name(f"magic_{f.__name__}")

            with get_connection() as (conn, cursor):
                cached_result = get_cached_result(cursor, table_name, call_id)
                if cached_result is not None:
                    print(f"Cache hit for {f.__name__}")
                    return MagicDataFrame(cached_result)

                print(f"Cache miss for {f.__name__}")
                try:
                    result = f(*args, **kwargs)
                    result_df = ensure_dataframe(result)

                    # Check if call_id is already in the DataFrame
                    if "call_id" not in result_df.columns:
                        result_df["call_id"] = call_id

                    columns = [
                        (str(col), infer_sqlite_type(dtype))
                        for col, dtype in result_df.dtypes.items()
                    ]
                    create_table(cursor, table_name, columns)
                    cache_result(cursor, table_name, call_id, result_df)
                    conn.commit()

                    # Remove the 'call_id' column before returning
                    if "call_id" in result_df.columns:
                        result_df = result_df.drop(columns=["call_id"])

                    update_generated_types(conn)
                    type_hints = get_type_hints_for_table(conn, table_name)
                    RowType = create_typed_dict(
                        f"{f.__name__}Row", {k: eval(v) for k, v in type_hints.items()}
                    )
                    wrapper.__annotations__["return"] = MagicDataFrame[List[RowType]]

                    return result_df
                except Exception as e:
                    print(f"Error in {f.__name__}: {str(e)}")
                    conn.rollback()  # Rollback any changes made to the database
                    raise  # Re-raise the exception to be handled by the caller

        return cast(T, wrapper)

    return decorator if func is None else decorator(func)


def mai(
    func: Optional[Callable] = None,
    *,
    batch_size: int = 10,
    mode: str = "generate",
    query: Optional[str] = None,
) -> Callable[[T], T]:
    def decorator(f: T) -> T:
        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> MagicDataFrame:
            call_id = generate_call_id(f, *args, **kwargs)
            table_name = sanitize_sql_name(f"ai_{f.__name__}")

            with get_connection() as (conn, cursor):
                cached_result = get_cached_result(cursor, table_name, call_id)
                if cached_result is not None:
                    print(f"Cache hit for {f.__name__}")
                    return MagicDataFrame(cached_result)

                print(f"Cache miss for {f.__name__}")
                try:
                    result = f(*args, **kwargs)
                    result_df = ensure_dataframe(result)
                    data = result_df.to_dict("records")

                    columns = [
                        (str(col), infer_sqlite_type(dtype))
                        for col, dtype in result_df.dtypes.items()
                    ]
                    create_table(cursor, table_name, columns)
                    ai_results = process_batches(
                        cursor, table_name, data, batch_size, query
                    )
                    result_df = combine_results(result_df, ai_results, mode, query)

                    # Update the table schema before caching the result
                    new_columns = [
                        (str(col), infer_sqlite_type(dtype))
                        for col, dtype in result_df.dtypes.items()
                    ]
                    update_table_schema(cursor, table_name, new_columns)

                    # Clear existing data for this call_id
                    cursor.execute(
                        f"DELETE FROM {table_name} WHERE call_id = ?", (call_id,)
                    )

                    # Add call_id to the result_df before caching
                    result_df["call_id"] = call_id
                    cache_result(cursor, table_name, call_id, result_df)
                    conn.commit()

                    generate_ai_descriptions(table_name, result_df.columns.tolist())

                    update_generated_types(conn)
                    type_hints = get_type_hints_for_table(conn, table_name)
                    RowType = create_typed_dict(
                        f"{f.__name__}Row",
                        {k: eval(v) for k, v in type_hints.items()},
                    )
                    wrapper.__annotations__["return"] = MagicDataFrame[List[RowType]]

                    # Remove call_id before returning
                    result_df = result_df.drop(columns=["call_id"])
                    return MagicDataFrame(result_df)
                except Exception as e:
                    print(f"Error in {f.__name__}: {str(e)}")
                    conn.rollback()  # Rollback any changes made to the database
                    raise  # Re-raise the exception to be handled by the caller

        return cast(T, wrapper)

    if func is None:
        return decorator
    else:
        return decorator(func)


def combine_results(
    original_df: MagicDataFrame,
    ai_results: List[Dict[str, Any]],
    mode: str,
    query: Optional[str],
) -> MagicDataFrame:
    if mode == "generate":
        # Add more rows
        new_rows = pd.DataFrame(ai_results)
        return MagicDataFrame(pd.concat([original_df, new_rows], ignore_index=True))
    elif mode == "augment":
        # Replace or add new columns based on AI results
        result_df = original_df.copy()

        # Ensure we have the same number of AI results as original rows
        ai_results = ai_results[: len(result_df)]

        for i, ai_result in enumerate(ai_results):
            for key, value in ai_result.items():
                if key in result_df.columns:
                    # Replace existing data only if it's not None or NaN
                    if value is not None and pd.notna(value):
                        result_df.at[i, key] = value
                else:
                    # Add new column
                    result_df.at[i, key] = value

        return MagicDataFrame(result_df)
    else:
        raise ValueError("Invalid mode. Must be either 'generate' or 'augment'")


def process_batches(
    cursor: sqlite3.Cursor,
    table_name: str,
    data: List[Dict[str, Any]],
    batch_size: int,
    query: Optional[str] = None,
) -> List[Dict[str, Any]]:
    ai_results = []
    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        keys = [
            hashlib.md5(json.dumps(item, sort_keys=True).encode()).hexdigest()
            for item in batch
        ]
        cached_results, new_items, new_keys = check_cache_and_get_new_items(
            cursor, table_name, batch, keys
        )

        if new_items:
            new_results = call_ai_model(new_items, query)
            cache_results(cursor, table_name, new_keys, new_results)
            cached_results.update(dict(zip(new_keys, new_results)))

        ai_results.extend([cached_results[key] for key in keys])
    return ai_results


def update_generated_types(conn: sqlite3.Connection):
    """Update the generated types for all tables in the database."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()

        type_hints = {}
        for column in columns:
            col_name, col_type = column[1], column[2]
            python_type = sqlite_to_python_type(col_type)
            if python_type:
                type_hints[col_name] = python_type

        # Store the type hints in a separate metadata table
        store_type_hints(conn, table_name, type_hints)


def sqlite_to_python_type(sqlite_type: str) -> Optional[str]:
    """Convert SQLite type to Python type string."""
    type_mapping = {
        "INTEGER": "int",
        "REAL": "float",
        "TEXT": "str",
        "BLOB": "bytes",
        "BOOLEAN": "bool",
    }
    return type_mapping.get(sqlite_type.upper())


def store_type_hints(
    conn: sqlite3.Connection, table_name: str, type_hints: Dict[str, str]
):
    """Store type hints for a table in a metadata table."""
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS magic_type_hints
        (table_name TEXT, column_name TEXT, python_type TEXT,
         PRIMARY KEY (table_name, column_name))
    """
    )

    for column, python_type in type_hints.items():
        cursor.execute(
            """
            INSERT OR REPLACE INTO magic_type_hints (table_name, column_name, python_type)
            VALUES (?, ?, ?)
        """,
            (table_name, column, python_type),
        )

    conn.commit()


def get_type_hints_for_table(
    conn: sqlite3.Connection, table_name: str
) -> Dict[str, str]:
    """Retrieve stored type hints for a table."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT column_name, python_type FROM magic_type_hints
        WHERE table_name = ?
    """,
        (table_name,),
    )
    return dict(cursor.fetchall())


def create_typed_dict(class_name: str, fields: Dict[str, type]):
    from typing import TypedDict

    return TypedDict(class_name, fields)
