import pandas as pd
import polars as pl
from magictables.magictable import MagicTable


class NotSoMagicTable(pd.DataFrame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    async def from_api(cls, *args, **kwargs):
        magic_table = await MagicTable.from_api(*args, **kwargs)
        return cls(magic_table.to_pandas())

    async def join_with_query(self, natural_query: str) -> "NotSoMagicTable":
        magic_table = await MagicTable.from_polars(pl.from_pandas(self), "temp_label")
        result = await magic_table.join_with_query(natural_query)
        return NotSoMagicTable(result.to_pandas())

    async def chain(self, *args, **kwargs) -> "NotSoMagicTable":
        magic_table = await MagicTable.from_polars(pl.from_pandas(self), "temp_label")
        result = await magic_table.chain(*args, **kwargs)
        return NotSoMagicTable(result.to_pandas())

    async def transform(self, natural_query: str) -> "NotSoMagicTable":
        magic_table = await MagicTable.from_polars(pl.from_pandas(self), "temp_label")
        result = await magic_table.transform(natural_query)
        return NotSoMagicTable(result.to_pandas())

    async def clear_all_data(self):
        magic_table = await MagicTable.from_polars(pl.from_pandas(self), "temp_label")
        await magic_table.clear_all_data()

    def __getattribute__(self, name):
        try:
            return super().__getattribute__(name)
        except AttributeError:
            if hasattr(MagicTable, name):

                async def wrapper(*args, **kwargs):
                    magic_table = await MagicTable.from_polars(
                        pl.from_pandas(self), "temp_label"
                    )
                    method = getattr(magic_table, name)
                    result = await method(*args, **kwargs)
                    if isinstance(result, MagicTable):
                        return NotSoMagicTable(result.to_pandas())
                    return result

                return wrapper
            raise

    def __await__(self):
        return self.__await_impl__().__await__()

    async def __await_impl__(self):
        return self
