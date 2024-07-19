from .decorators import mtable, mai
from .query import (
    query_magic_db,
    get_table_info,
    join_magic_tables,
    QueryBuilder,
    execute_query,
)

__all__ = [
    "mtable",
    "mai",
    "query_magic_db",
    "get_table_info",
    "join_magic_tables",
    "QueryBuilder",
    "execute_query",
]
__version__ = "0.2.0"
