import pandas as pd
from magictables import MagicTable


class NotSoMagicTable(MagicTable):
    def __init__(self, data=None):
        if data is None:
            super().__init__()
        elif isinstance(data, pd.DataFrame):
            super().__init__(data)
        elif isinstance(data, MagicTable):
            super().__init__(data.to_pandas())
            self.__dict__.update(data.__dict__)
        else:
            raise ValueError("Input must be None, a pandas DataFrame, or a MagicTable")

    def __getattribute__(self, name):
        try:
            attr = super().__getattribute__(name)
            if callable(attr):

                def wrapper(*args, **kwargs):
                    result = attr(*args, **kwargs)
                    if isinstance(result, MagicTable):
                        return NotSoMagicTable(result).to_pandas()
                    return result

                return wrapper
            return attr
        except AttributeError:
            if name == "_driver":
                return None
            raise

    @classmethod
    async def from_api(cls, *args, **kwargs):
        magic_table = await super().from_api(*args, **kwargs)
        return cls(magic_table).to_pandas()

    @classmethod
    async def from_polars(cls, df, label: str):
        magic_table = await super().from_polars(df, label)
        return cls(magic_table).to_pandas()

    def __repr__(self):
        return f"NotSoMagicTable(\n{self.to_pandas().__repr__()}\n)"

    def __str__(self):
        return self.__repr__()
