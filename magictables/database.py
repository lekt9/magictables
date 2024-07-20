# database.py

from contextlib import contextmanager
import json
import logging
from typing import Any, Dict, List, Optional, Union
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    select,
    insert,
    update,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

import pandas as pd

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

    def cache_results(self, table_name: str, df: pd.DataFrame, call_id: str) -> None:
        columns = df.columns.tolist()
        self.create_table_if_not_exists(table_name, columns)

        logging.info(f"Caching results for table: {table_name}, call_id: {call_id}")
        logging.info(f"DataFrame shape: {df.shape}")

        with self.session_scope() as session:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            for index, row in df.iterrows():
                data = row.to_dict()
                data = {k: str(v) if v is not None else None for k, v in data.items()}
                data["call_id"] = call_id
                try:
                    stmt = sqlite_insert(table).values(**data)
                    stmt = stmt.on_conflict_do_update(index_elements=["id"], set_=data)
                    session.execute(stmt)
                    logging.info(f"Inserted/Updated row {index} for {table_name}")
                except SQLAlchemyError as e:
                    logging.error(f"Error upserting row: {e}")
                    logging.error(f"Row data: {data}")

        logging.info(f"Finished caching results for {table_name}")

    def get_cached_result(
        self, table_name: str, call_id: str
    ) -> Optional[pd.DataFrame]:
        if not self.engine.has_table(table_name):
            return None

        with self.session_scope() as session:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            stmt = select(table).where(table.c.call_id == call_id)
            result = session.execute(stmt)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        if df.empty:
            return None

        df = df.drop(columns=["call_id"], errors="ignore")

        # Convert numeric columns back to appropriate types
        for col in df.columns:
            if col not in ["id", "call_id"]:
                df[col] = pd.to_numeric(df[col], errors="ignore")

        return df

    def check_cache_and_get_new_items(
        self, table_name: str, batch: List[Dict[str, Any]], keys: List[str]
    ):
        self.create_table_if_not_exists(
            table_name, list(batch[0].keys()) if batch else []
        )

        with self.session_scope() as session:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            stmt = select(table).where(table.c.id.in_(keys))
            result = session.execute(stmt)
            cached_results = {row["id"]: dict(row) for row in result}

        new_items = [
            item for item, key in zip(batch, keys) if key not in cached_results
        ]
        new_keys = [key for key in keys if key not in cached_results]
        return cached_results, new_items, new_keys

    def insert_nested_data(self, table_name: str, data: pd.DataFrame, call_id: str):
        self.create_table_if_not_exists(table_name, data.columns.tolist())

        with self.session_scope() as session:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            for _, row in data.iterrows():
                row_dict = row.to_dict()
                row_dict["call_id"] = call_id
                for col, value in row_dict.items():
                    if isinstance(value, (dict, list)):
                        row_dict[col] = json.dumps(value)
                    elif pd.isna(value):
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
        self, table_name: str, df: pd.DataFrame
    ) -> pd.DataFrame:
        for col in df.columns:
            if col.endswith("_id"):
                continue
            sample_value = df[col].iloc[0] if len(df) > 0 else None
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
                                for _, row in df.iterrows():
                                    stmt = select(nested_table).where(
                                        nested_table.c[f"{table_name}_id"] == row["id"]
                                    )
                                    result = session.execute(stmt)
                                    nested_rows = result.fetchall()
                                    nested_df = pd.DataFrame(
                                        nested_rows, columns=result.keys()
                                    )
                                    nested_data.append(
                                        self.reconstruct_nested_data(
                                            nested_table_name, nested_df
                                        )
                                    )
                            df[col] = nested_data
                except json.JSONDecodeError:
                    pass
        return df


magic_db = MagicDB()
