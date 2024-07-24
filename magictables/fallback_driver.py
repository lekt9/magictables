from neo4j import AsyncDriver, AsyncGraphDatabase, Query, basic_auth
import spycy
import asyncio
import pickle
import os
import hashlib
class FallbackAsyncDriver(AsyncDriver):
    def __init__(self, uri, auth=None, cache_file="spycy_cache.pkl", **config):
        super().__init__(uri, auth, **config)
        self.neo4j_driver = AsyncGraphDatabase.driver(uri, auth=auth, **config)
        self.cache_file = cache_file
        self.graph = self._load_cache()

    def _load_cache(self):
        if os.path.exists(self.cache_file):
            with open(self.cache_file, "rb") as f:
                return pickle.load(f)
        return spycy.Graph()

    def _save_cache(self):
        with open(self.cache_file, "wb") as f:
            pickle.dump(self.graph, f)

    def _hash_query(self, query, parameters):
        query_str = query if isinstance(query, str) else query.text
        param_str = str(sorted(parameters.items())) if parameters else ""
        return hashlib.md5((query_str + param_str).encode()).hexdigest()

    async def execute_query(self, query, parameters=None, **kwargs):
        query_hash = self._hash_query(query, parameters)
        
        # Try to get result from cache
        cached_result = self.graph.query(f"MATCH (c:CachedQuery {{hash: '{query_hash}'}}) RETURN c.result")
        if cached_result:
            print("Using cached result")
            return self._create_async_result(cached_result[0]['c.result'])

        # If not in cache, try to execute on Neo4j
        try:
            neo4j_result = await self.neo4j_driver.execute_query(query, parameters, **kwargs)
            records = await neo4j_result.records()
            result_list = [dict(record) for record in records]

            # Cache the result
            cache_query = f"""
            CREATE (c:CachedQuery {{hash: '{query_hash}', result: {result_list}}})
            """
            self.graph.query(cache_query)
            self._save_cache()

            return self._create_async_result(result_list)
        except Exception as e:
            print(f"Neo4j query failed: {e}")
            # If Neo4j fails, try to execute on spycy
            spycy_result = self.graph.query(query, parameters)
            return self._create_async_result(spycy_result)

    def _create_async_result(self, result):
        async def result_to_records():
            for item in result:
                yield item

        class SpycyAsyncResult:
            def __init__(self, result):
                self.result = result

            async def records(self):
                return result_to_records()

        return SpycyAsyncResult(result)

    async def verify_connectivity(self):
        try:
            return await self.neo4j_driver.verify_connectivity()
        except:
            return True  # Assume spycy is always connected

    async def close(self):
        self._save_cache()
        await self.neo4j_driver.close()
class FallbackAsyncGraphDatabase:
    @staticmethod
    def driver(uri, auth=None, cache_file="spycy_cache.pkl", **config):
        return FallbackAsyncDriver(uri, auth, cache_file, **config)
