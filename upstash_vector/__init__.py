__version__ = "0.8.0"

from upstash_vector.client import AsyncIndex, Index
from upstash_vector.types import Vector

__all__ = ["Index", "AsyncIndex", "Vector"]
