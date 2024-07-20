from .database import MagicDB, magic_db
from .decorators import mtable
from .chain import create_chain
from .mapping import store_mapping, get_stored_mapping
from .input import magic_input  # Add this line

__all__ = [
    "MagicDB",
    "magic_db",
    "mtable",
    "create_chain",
    "store_mapping",
    "get_stored_mapping",
    "magic_input",  # Add this line
]
