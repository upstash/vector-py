from dataclasses import dataclass
from typing import Optional, List, Dict


@dataclass
class Vector:
    id: str
    vector: List[float]
    metadata: Optional[Dict] = None


@dataclass
class FetchResult:
    id: str
    vector: Optional[List[float]] = None
    metadata: Optional[Dict] = None

    @classmethod
    def from_json(cls, obj: dict) -> "FetchResult":
        return cls(
            id=obj["id"],
            vector=obj.get("vector"),
            metadata=obj.get("metadata"),
        )


@dataclass
class QueryResult:
    id: str
    score: float
    vector: Optional[List[float]] = None
    metadata: Optional[Dict] = None

    @classmethod
    def from_json(cls, obj: dict) -> "QueryResult":
        return cls(
            id=obj["id"],
            score=obj["score"],
            vector=obj.get("vector"),
            metadata=obj.get("metadata"),
        )


@dataclass
class DeleteResult:
    deleted_count: int

    @classmethod
    def from_json(cls, obj: dict) -> "DeleteResult":
        return cls(deleted_count=obj["deleted"])


@dataclass
class RangeResult:
    next_cursor: str
    vectors: List[FetchResult]

    @classmethod
    def from_json(cls, obj: dict) -> "RangeResult":
        return cls(
            next_cursor=obj["nextCursor"],
            vectors=[FetchResult.from_json(v) for v in obj["vectors"]],
        )


@dataclass
class StatsResult:
    vector_count: int
    pending_vector_count: int
    index_size: int

    @classmethod
    def from_json(cls, obj: dict) -> "StatsResult":
        return cls(
            vector_count=obj["vectorCount"],
            pending_vector_count=obj["pendingVectorCount"],
            index_size=obj["indexSize"],
        )
