from .base import PaperSourceAdapter
from .ieee import IeeeAdapter
from .scopus import ScopusAdapter
from .springer import SpringerAdapter
from .wos import WosAdapter

__all__ = [
    "PaperSourceAdapter",
    "WosAdapter",
    "ScopusAdapter",
    "IeeeAdapter",
    "SpringerAdapter",
]

