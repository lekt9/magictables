from contextlib import contextmanager
import json
import logging
from typing import Any, Dict, List, Optional
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    inspect,
    select,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import polars as pl
from sqlalchemy import Table, Column, String, JSON

from magictables.utils import generate_row_id

MAGIC_DB = "sqlite:///magic.db"


class MagicDB:
    def __init__(self, db_path: str = MAGIC_DB):
        self.engine = create_engine(db_path)
        self.metadata = MetaData()
        self.Session = sessionmaker(bind=self.engine)

    @contextmanager
    def session_scope(self):
        session = self.Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def create_table_if_not_exists(self, table_name: str, columns: List[str]):
        if not self.engine.has_table(table_name):
            table = Table(
                table_name,
                self.metadata,
                Column("id", String, primary_key=True),
                Column("call_id", String),
                *(
                    Column(col, String)
                    for col in columns
                    if col not in ["id", "call_id"]
                ),
            )
            self.metadata.create_all(self.engine)
        else:
            with self.engine.connect() as connection:
                inspector = inspect(self.engine)
                existing_columns = set(
                    c["name"] for c in inspector.get_columns(table_name)
                )
                for col in columns:
                    if col not in existing_columns and col not in ["id", "call_id"]:
                        connection.execute(
                            f"ALTER TABLE {table_name} ADD COLUMN {col} STRING"
                        )

    def check_cache_and_get_new_items(
        self, table_name: str, batch: List[Dict[str, Any]], keys: List[str]
    ):
        self.create_table_if_not_exists(
            table_name, list(batch[0].keys()) if batch else []
        )

        with self.session_scope() as session:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            stmt = select(table).where(table.c.id.isin(keys))
            result = session.execute(stmt)
            cached_results = {row["id"]: dict(row) for row in result}

        new_items = [
            item for item, key in zip(batch, keys) if key not in cached_results
        ]
        new_keys = [key for key in keys if key not in cached_results]
        return cached_results, new_items, new_keys

    def insert_nested_data(self, table_name: str, data: pl.DataFrame, call_id: str):
        self.create_table_if_not_exists(table_name, data.columns)

        with self.session_scope() as session:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            for row in data.iter_rows(named=True):
                row_dict = row.copy()
                row_dict["call_id"] = call_id
                for col, value in row_dict.items():
                    if isinstance(value, (dict, list)):
                        row_dict[col] = json.dumps(value)
                    elif pl.Series([value]).is_null().all():
                        row_dict[col] = None
                    elif isinstance(value, (int, float)) and value == float("inf"):
                        row_dict[col] = None
                    elif isinstance(value, str) and value.lower() in (
                        "unknown",
                        "null",
                        "none",
                        "nan",
                    ):
                        row_dict[col] = None

                try:
                    stmt = sqlite_insert(table).values(**row_dict)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["id"], set_=row_dict
                    )
                    session.execute(stmt)
                except SQLAlchemyError as e:
                    logging.error(f"Error upserting row: {e}")
                    logging.error(f"Row data: {row_dict}")

    def reconstruct_nested_data(
        self, table_name: str, df: pl.DataFrame
    ) -> pl.DataFrame:
        for col in df.columns:
            if col.endswith("_id"):
                continue
            sample_value = df[col][0] if len(df) > 0 else None
            if isinstance(sample_value, str):
                try:
                    parsed_value = json.loads(sample_value)
                    if isinstance(parsed_value, (dict, list)):
                        nested_table_name = f"{table_name}_{col}"
                        if self.engine.has_table(nested_table_name):
                            nested_data = []
                            with self.session_scope() as session:
                                nested_table = Table(
                                    nested_table_name,
                                    self.metadata,
                                    autoload_with=self.engine,
                                )
                                for row in df.iter_rows(named=True):
                                    stmt = select(nested_table).where(
                                        nested_table.c[f"{table_name}_id"] == row["id"]
                                    )
                                    result = session.execute(stmt)
                                    nested_rows = result.fetchall()
                                    nested_df = pl.DataFrame(
                                        nested_rows, schema=result.keys()
                                    )
                                    nested_data.append(
                                        self.reconstruct_nested_data(
                                            nested_table_name, nested_df
                                        )
                                    )
                            df = df.with_column(pl.Series(name=col, values=nested_data))
                except json.JSONDecodeError:
                    pass
        return df

    def ensure_mappings_table_exists(self):
        if not self.engine.has_table("mappings"):
            mappings_table = Table(
                "mappings",
                self.metadata,
                Column("table_name", String, primary_key=True),
                Column("mapping", JSON),
            )
            self.metadata.create_all(self.engine, tables=[mappings_table])

    def store_mapping(self, table_name: str, mapping: dict):
        self.ensure_mappings_table_exists()
        with self.session_scope() as session:
            table = Table("mappings", self.metadata, autoload_with=self.engine)
            data = {
                "table_name": table_name,
                "mapping": json.dumps(mapping),
            }
            stmt = sqlite_insert(table).values(**data)
            stmt = stmt.on_conflict_do_update(index_elements=["table_name"], set_=data)
            session.execute(stmt)

    def get_mapping(self, table_name: str) -> Optional[dict]:
        self.ensure_mappings_table_exists()
        with self.session_scope() as session:
            table = Table("mappings", self.metadata, autoload_with=self.engine)
            stmt = select(table).where(table.c.table_name == table_name)
            result = session.execute(stmt).fetchone()
            if result:
                return json.loads(result["mapping"])
            return None

    def cache_results(self, table_name: str, df: pl.DataFrame, call_id: str) -> None:
        columns = df.columns
        self.create_table_if_not_exists(table_name, columns)

        logging.info(f"Caching results for table: {table_name}, call_id: {call_id}")
        logging.info(f"DataFrame shape: {df.shape}")

        with self.session_scope() as session:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            existing_columns = set(c.name for c in table.columns)

            for row in df.iter_rows(named=True):
                data = {
                    k: str(v) if v is not None else None
                    for k, v in row.items()
                    if k in existing_columns
                }
                data["call_id"] = call_id
                data["id"] = generate_row_id(data)  # Generate a unique id for each row
                try:
                    stmt = sqlite_insert(table).values(**data)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["id"],
                        set_={
                            col: stmt.excluded[col]
                            for col in data.keys()
                            if col in existing_columns
                        },
                    )
                    session.execute(stmt)
                    logging.info(f"Inserted/Updated row for {table_name}")
                except SQLAlchemyError as e:
                    logging.error(f"Error upserting row: {e}")
                    logging.error(f"Row data: {data}")

        logging.info(f"Finished caching results for {table_name}")

    def get_cached_result(
        self, table_name: str, call_id: str
    ) -> Optional[pl.DataFrame]:
        if not self.engine.has_table(table_name):
            return None

        with self.session_scope() as session:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            stmt = select(table).where(table.c.call_id == call_id)
            result = session.execute(stmt)

            rows = result.fetchall()
            column_names = result.keys()

            if not rows:
                return None

            df = pl.DataFrame(
                {col: [row[i] for row in rows] for i, col in enumerate(column_names)}
            )

        df = df.drop("call_id")

        # Convert numeric columns back to appropriate types
        for col in df.columns:
            if col not in ["id", "call_id"]:
                df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

        return df

    def get_cached_results(
        self, table_names: List[str], call_ids: List[str]
    ) -> Dict[str, List[pl.DataFrame]]:
        cached_results = {}
        for table_name in table_names:
            cached_results[table_name] = [
                self.get_cached_result(table_name, call_id) for call_id in call_ids
            ]
        return cached_results


magic_db = MagicDB()


def cache_mapping(func_name: str, mapping: Dict[str, Any]):
    magic_db.store_mapping(func_name, mapping)


def retrieve_mapping(func_name: str) -> Optional[Dict[str, Any]]:
    return magic_db.get_mapping(func_name)
