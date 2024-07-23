import asyncio
import logging
import time
import urllib.parse
import json
import os
from typing import List, Dict, Any, Optional

import aiofiles
from neo4j import AsyncDriver, AsyncGraphDatabase, Query, basic_auth

# logging.basicConfig(filename="example.log", level=logging.DEBUG)


class HybridResult:
    def __init__(self, data: List[Dict[str, Any]]):
        self.data = data
        self._index = 0

    async def single(self) -> Optional["HybridRecord"]:
        if self._index < len(self.data):
            record = self.data[self._index]
            self._index += 1
            return HybridRecord(record)
        return None

    async def consume(self) -> "HybridResultSummary":
        self._index = len(self.data)
        return HybridResultSummary(len(self.data))

    async def fetch(self, n: int) -> List["HybridRecord"]:
        records = []
        for _ in range(n):
            record = await self.single()
            if record is None:
                break
            records.append(record)
        return records

    def __aiter__(self):
        return self

    async def __anext__(self) -> "HybridRecord":
        record = await self.single()
        if record is None:
            raise StopAsyncIteration
        return record


class HybridRecord:
    def __init__(self, data: Dict[str, Any]):
        self.data = data

    def __getitem__(self, key):
        return self.data.get(key)

    def get(self, key, default=None):
        return self.data.get(key, default)


class HybridResultSummary:
    def __init__(self, affected_items: int):
        self.counters = HybridSummaryCounters(affected_items)


class HybridSummaryCounters:
    def __init__(self, affected_items: int):
        self.nodes_created = 0
        self.nodes_deleted = 0
        self.relationships_created = 0
        self.relationships_deleted = 0
        self.properties_set = 0
        self.labels_added = 0
        self.labels_removed = 0
        self.indexes_added = 0
        self.indexes_removed = 0
        self.constraints_added = 0
        self.constraints_removed = 0
        self._affected_items = affected_items

    def __getitem__(self, key):
        return getattr(self, key, 0)

    @property
    def contains_updates(self) -> bool:
        return self._affected_items > 0


class HybridDriver:
    def __init__(self, uri: str, user: str, password: str, cache_dir: str = "cache"):
        self.uri = uri
        self.user = user
        self.password = password
        self.cache_dir = cache_dir
        self._data: Dict[str, Any] = {}
        self._neo4j_driver: Optional[AsyncDriver] = None

        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

        # Load the cache when initializing the driver
        asyncio.create_task(self._load_cache())

    async def _load_cache(self):
        cache_file = os.path.join(self.cache_dir, "graph_cache.json")
        if os.path.exists(cache_file):
            try:
                async with aiofiles.open(cache_file, "r") as f:
                    content = await f.read()
                    if content.strip():  # Check if the content is not empty
                        self._data = json.loads(content)
                    else:
                        logging.warning(
                            f"Cache file {cache_file} is empty. Initializing empty cache."
                        )
                        self._data = {}
            except json.JSONDecodeError as e:
                logging.error(
                    f"Error decoding JSON from cache file {cache_file}: {str(e)}"
                )
                logging.info("Initializing empty cache.")
                self._data = {}
            except Exception as e:
                logging.error(f"Error reading cache file {cache_file}: {str(e)}")
                logging.info("Initializing empty cache.")
                self._data = {}
        else:
            logging.info(
                f"Cache file {cache_file} does not exist. Initializing empty cache."
            )
            self._data = {}

    async def _save_cache(self):
        cache_file = os.path.join(self.cache_dir, "graph_cache.json")
        async with aiofiles.open(cache_file, "w") as f:
            await f.write(json.dumps(self._data, indent=2))

    async def _get_neo4j_driver(self) -> AsyncDriver:
        if self._neo4j_driver is None:
            parsed_uri = urllib.parse.urlparse(self.uri)
            scheme = parsed_uri.scheme
            if scheme in ["bolt", "neo4j", "bolt+s"]:
                self._neo4j_driver = AsyncGraphDatabase.driver(
                    self.uri, auth=(self.user, self.password)
                )
            elif scheme in ["http", "https"]:
                self._neo4j_driver = AsyncGraphDatabase.driver(
                    self.uri,
                    auth=basic_auth(self.user, self.password),
                )
            else:
                raise ValueError(f"Unsupported URI scheme: {scheme}")
        return self._neo4j_driver

    def session(self, **kwargs):
        return HybridSession(self, **kwargs)

    async def close(self):
        await self._save_cache()
        if self._neo4j_driver:
            await self._neo4j_driver.close()


class HybridSession:
    def __init__(self, driver: HybridDriver, **kwargs):
        self.driver = driver
        self.kwargs = kwargs
        self._neo4j_session = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def _get_neo4j_session(self):
        if self._neo4j_session is None:
            neo4j_driver = await self.driver._get_neo4j_driver()
            self._neo4j_session = await neo4j_driver.session(**self.kwargs).__aenter__()
        return self._neo4j_session

    async def run(self, query: str, parameters: Dict[str, Any] = None, **kwargs):
        start_time = time.time()
        cache_used = False
        if not self.driver._data:
            await self.driver._load_cache()

        query_str = query.text if isinstance(query, Query) else query

        # Check if the query is a MATCH query that we can handle with the cache
        if "MATCH" in query_str:
            try:
                node_type = query_str.split("(")[1].split(":")[1].split(")")[0]
                cached_data = self.driver._data.get(node_type, [])
                if cached_data:
                    cache_used = True
                    end_time = time.time()
                    logging.info(
                        f"Cache hit for node type '{node_type}'. Query executed in {end_time - start_time:.4f} seconds."
                    )
                    return HybridResult(cached_data)
            except IndexError:
                # If we can't parse the node type, we'll fall back to using the database
                pass

        # If cache is empty or query can't be handled by cache, use Neo4j
        neo4j_session = await self._get_neo4j_session()
        result = await neo4j_session.run(query, parameters, **kwargs)

        # Store the result in the cache if it's a MATCH query
        if "MATCH" in query_str:
            try:
                node_type = query_str.split("(")[1].split(":")[1].split(")")[0]
                data = await result.data()
                self.driver._data[node_type] = data
                await self.driver._save_cache()
            except IndexError:
                # If we can't parse the node type, we won't cache the result
                pass
        end_time = time.time()
        execution_time = end_time - start_time
        cache_status = "Cache miss" if not cache_used else "Cache not applicable"
        logging.info(f"{cache_status}. Query executed in {execution_time:.4f} seconds.")

        return HybridResult(await result.data())

    async def close(self):
        if self._neo4j_session:
            await self._neo4j_session.__aexit__(None, None, None)
            self._neo4j_session = None

    def _get_query_type(self, query: str) -> str:
        query = query.strip().upper()
        if query.startswith(("MATCH", "RETURN", "CALL")):
            return "READ"
        elif query.startswith(("CREATE", "MERGE", "SET", "DELETE", "REMOVE")):
            return "WRITE"
        else:
            return "OTHER"
