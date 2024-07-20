from .database import MagicDB, magic_db
from .decorators import mtable, mai
from .utils import ensure_dataframe, call_ai_model, generate_ai_descriptions

__all__ = [
    "MagicDB",
    "magic_db",
    "mtable",
    "mai",
    "ensure_dataframe",
    "call_ai_model",
    "generate_ai_descriptions",
]
