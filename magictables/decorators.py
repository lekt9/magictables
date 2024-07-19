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

    def transform(
        self, func: Callable[[pd.DataFrame], pd.DataFrame]
    ) -> "MagicDataFrame[RowType]":
        """Apply a custom transformation function."""
        return MagicDataFrame(func(self))

    def filter(self, condition: Union[str, Callable]) -> "MagicDataFrame[RowType]":
        """Filter the DataFrame based on a condition."""
        if isinstance(condition, str):
            return MagicDataFrame(self.query(condition))
        return MagicDataFrame(self[self.apply(condition, axis=1)])

    def select_columns(self, columns: List[str]) -> "MagicDataFrame[RowType]":
        """Select specific columns."""
        return MagicDataFrame(self[columns])

    def rename_columns(self, column_map: Dict[str, str]) -> "MagicDataFrame[RowType]":
        """Rename columns."""
        return MagicDataFrame(self.rename(columns=column_map))

    def to_dict_list(self) -> List[RowType]:
        """Convert to a list of dictionaries."""
        return self.to_dict("records")

    def to_json(self) -> str:
        """Convert to JSON string."""
        return self.to_json(orient="records")


def generate_call_id(func: Callable, *args: Any, **kwargs: Any) -> str:
    """Generate a unique ID for a function call."""
    call_data = (func.__name__, args, kwargs)
    return hashlib.md5(json.dumps(call_data, sort_keys=True).encode()).hexdigest()


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
                result = f(*args, **kwargs)
                result_df = ensure_dataframe(result)
                columns = [
                    (str(col), infer_sqlite_type(dtype))
                    for col, dtype in result_df.dtypes.items()
                ]
                create_table(cursor, table_name, columns)
                cache_result(cursor, table_name, call_id, result_df)
                conn.commit()

                update_generated_types(conn)
                type_hints = get_type_hints_for_table(conn, table_name)
                RowType = create_typed_dict(
                    f"{f.__name__}Row", {k: eval(v) for k, v in type_hints.items()}
                )
                wrapper.__annotations__["return"] = MagicDataFrame[List[RowType]]

            return result_df

        return cast(T, wrapper)

    return decorator if func is None else decorator(func)


def mai(batch_size: int = 10, mode: str = "generate") -> Callable[[T], T]:
    def decorator(func: T) -> T:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> MagicDataFrame:
            result = func(*args, **kwargs)
            result_df = ensure_dataframe(result)
            data = result_df.to_dict("records")

            table_name = f"ai_{func.__name__}"

            with get_connection() as (conn, cursor):
                columns = [
                    (str(col), infer_sqlite_type(dtype))
                    for col, dtype in result_df.dtypes.items()
                ]
                create_table(cursor, table_name, columns)
                ai_results = process_batches(cursor, table_name, data, batch_size)
                result_df = combine_results(result_df, ai_results, mode)

                # Update the table schema before caching the result
                new_columns = [
                    (str(col), infer_sqlite_type(dtype))
                    for col, dtype in result_df.dtypes.items()
                ]
                update_table_schema(cursor, table_name, new_columns)

                cache_result(
                    cursor,
                    table_name,
                    generate_call_id(func, *args, **kwargs),
                    result_df,
                )
                conn.commit()

                generate_ai_descriptions(table_name, result_df.columns.tolist())

                update_generated_types(conn)
                type_hints = get_type_hints_for_table(conn, table_name)
                RowType = create_typed_dict(
                    f"{func.__name__}Row", {k: eval(v) for k, v in type_hints.items()}
                )
                wrapper.__annotations__["return"] = MagicDataFrame[List[RowType]]

            return MagicDataFrame(result_df)

        return cast(T, wrapper)

    return decorator


def process_batches(
    cursor: sqlite3.Cursor, table_name: str, data: List[Dict[str, Any]], batch_size: int
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
            new_results = call_ai_model(new_items)
            cache_results(cursor, table_name, new_keys, new_results)
            cached_results.update(dict(zip(new_keys, new_results)))

        ai_results.extend([cached_results[key] for key in keys])
    return ai_results


def combine_results(
    original_df: MagicDataFrame, ai_results: List[Dict[str, Any]], mode: str
) -> MagicDataFrame:
    if mode == "generate":
        return MagicDataFrame(ai_results)
    elif mode == "augment":
        for idx, ai_result in enumerate(ai_results):
            for key, value in ai_result.items():
                original_df.loc[idx, f"ai_{key}"] = value
        return original_df
    else:
        raise ValueError("Invalid mode. Must be either 'generate' or 'augment'")


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
