from sqlalchemy import create_engine, Column, Integer, String, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from typing import Dict, Any, Optional
import json

Base = declarative_base()


class Route(Base):
    __tablename__ = "routes"

    id = Column(Integer, primary_key=True)
    source_name = Column(String, nullable=False)
    route_name = Column(String, nullable=False)
    url = Column(String, nullable=False)
    query = Column(String, nullable=False)


class CachedResult(Base):
    __tablename__ = "cached_results"

    id = Column(Integer, primary_key=True)
    source_name = Column(String, nullable=False)
    call_id = Column(String, nullable=False)
    result = Column(JSON, nullable=False)


class Mapping(Base):
    __tablename__ = "mappings"

    id = Column(Integer, primary_key=True)
    source_name = Column(String, nullable=False, unique=True)
    mapping = Column(JSON, nullable=False)


class ShadowDB:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self.engine = create_engine("sqlite:///shadow.db")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def add_route(self, source_name: str, route_name: str, url: str, query: str):
        session = self.Session()
        route = Route(
            source_name=source_name, route_name=route_name, url=url, query=query
        )
        session.add(route)
        session.commit()
        session.close()

    def get_route(self, source_name: str, route_name: str) -> Dict[str, str]:
        session = self.Session()
        route = (
            session.query(Route)
            .filter_by(source_name=source_name, route_name=route_name)
            .first()
        )
        session.close()
        return {"url": route.url, "query": route.query} if route else {}

    def get_cached_result(
        self, source_name: str, call_id: str
    ) -> Optional[Dict[str, Any]]:
        session = self.Session()
        result = (
            session.query(CachedResult)
            .filter_by(source_name=source_name, call_id=call_id)
            .first()
        )
        session.close()
        return result.result if result else None

    def cache_result(self, source_name: str, call_id: str, result: Dict[str, Any]):
        session = self.Session()
        cached_result = CachedResult(
            source_name=source_name, call_id=call_id, result=result
        )
        session.add(cached_result)
        session.commit()
        session.close()

    def execute_sql(self, query: str) -> Dict[str, Any]:
        with self.engine.connect() as connection:
            result = connection.execute(query)
            return [dict(row) for row in result]

    def get_mapping(self, source_name: str) -> Optional[Dict[str, Any]]:
        session = self.Session()
        mapping = session.query(Mapping).filter_by(source_name=source_name).first()
        session.close()
        return mapping.mapping if mapping else None

    def save_mapping(self, source_name: str, mapping: Dict[str, Any]):
        session = self.Session()
        existing_mapping = (
            session.query(Mapping).filter_by(source_name=source_name).first()
        )
        if existing_mapping:
            existing_mapping.mapping = mapping
        else:
            new_mapping = Mapping(source_name=source_name, mapping=mapping)
            session.add(new_mapping)
        session.commit()
        session.close()

    def clear_cache(self, source_name: str):
        session = self.Session()
        session.query(CachedResult).filter_by(source_name=source_name).delete()
        session.commit()
        session.close()

    def clear_all_cache(self):
        session = self.Session()
        session.query(CachedResult).delete()
        session.commit()
        session.close()
