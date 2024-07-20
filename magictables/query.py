import dataset
from typing import List, Dict, Any, Union

MAGIC_DB = "sqlite:///magic.db"


class DatasetQueryBuilder:
    def __init__(self, table_name: str):
        self.db = dataset.connect(MAGIC_DB)
        self.table = self.db[table_name]
        self.filters = {}
        self.order = None
        self._limit = None
        self._offset = None

    def filter(self, **kwargs) -> "DatasetQueryBuilder":
        self.filters.update(kwargs)
        return self

    def order_by(self, order: str) -> "DatasetQueryBuilder":
        self.order = order
        return self

    def limit(self, limit: int) -> "DatasetQueryBuilder":
        self._limit = limit
        return self

    def offset(self, offset: int) -> "DatasetQueryBuilder":
        self._offset = offset
        return self

    def all(self) -> List[Dict[str, Any]]:
        return list(
            self.table.find(
                **self.filters,
                order_by=self.order,
                _limit=self._limit,
                _offset=self._offset,
            )
        )

    def first(self) -> Union[Dict[str, Any], None]:
        return self.table.find_one(**self.filters)

    def count(self) -> int:
        return self.table.count(**self.filters)

    def __del__(self):
        if hasattr(self, "db"):
            self.db.close()


def query(table_name: str) -> DatasetQueryBuilder:
    return DatasetQueryBuilder(table_name)
