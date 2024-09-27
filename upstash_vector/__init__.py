__version__ = "0.6.0"

from upstash_vector.client import Index, AsyncIndex
from upstash_vector.types import Vector

__all__ = ["Index", "AsyncIndex", "Vector"]
