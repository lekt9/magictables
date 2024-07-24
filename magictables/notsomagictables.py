import pandas as pd
from magictables import MagicTable


class NotSoMagicTable(MagicTable):
    @classmethod
    async def from_api(cls, *args, **kwargs):
        magic_table = await super().from_api(*args, **kwargs)
        return cls(magic_table.to_pandas())

    @classmethod
    async def from_polars(cls, df, label: str):
        magic_table = await super().from_polars(df, label)
        return cls(magic_table.to_pandas())

    def __init__(self, df=None):
        if df is None:
            df = pd.DataFrame()
        super().__init__(df)
        self._df = df

    def __getattribute__(self, name):
        try:
            attr = object.__getattribute__(self, name)
            if callable(attr):

                def wrapper(*args, **kwargs):
                    result = attr(*args, **kwargs)
                    if isinstance(result, MagicTable):
                        return NotSoMagicTable(result.to_pandas())
                    return result

                return wrapper
            return attr
        except AttributeError:
            # If the attribute is not found in NotSoMagicTable, try to get it from MagicTable
            return getattr(super(), name)

    def to_pandas(self):
        return self._df

    def __repr__(self):
        return f"NotSoMagicTable(\n{self.to_pandas().__repr__()}\n)"

    def __str__(self):
        return self.__repr__()

    @classmethod
    async def _get_existing_api_data(
        cls, api_url: str, include_embedding: bool = False
    ):
        # Create an instance with an empty DataFrame
        instance = cls()
        return await super(NotSoMagicTable, cls)._get_existing_api_data(
            api_url, include_embedding
        )
