from dataclasses import dataclass
from typing import Optional, List, Dict, Union, Protocol


class SupportsToList(Protocol):
    def tolist(self) -> List[float]:
        ...


@dataclass
class Vector:
    id: Union[int, str]
    vector: Union[List[float], SupportsToList]
    metadata: Optional[Dict] = None


@dataclass
class Data:
    id: Union[int, str]
    data: str
    metadata: Optional[Dict] = None


@dataclass
class FetchResult:
    id: str
    vector: Optional[List[float]] = None
    metadata: Optional[Dict] = None

    @classmethod
    def _from_json(cls, obj: dict) -> "FetchResult":
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
    def _from_json(cls, obj: dict) -> "QueryResult":
        return cls(
            id=obj["id"],
            score=obj["score"],
            vector=obj.get("vector"),
            metadata=obj.get("metadata"),
        )


@dataclass
class DeleteResult:
    deleted: int

    @classmethod
    def _from_json(cls, obj: dict) -> "DeleteResult":
        return cls(deleted=obj["deleted"])


@dataclass
class RangeResult:
    next_cursor: str
    vectors: List[FetchResult]

    @classmethod
    def _from_json(cls, obj: dict) -> "RangeResult":
        return cls(
            next_cursor=obj["nextCursor"],
            vectors=[FetchResult._from_json(v) for v in obj["vectors"]],
        )


@dataclass
class NamespaceInfo:
    vector_count: int
    pending_vector_count: int

    @classmethod
    def _from_json(cls, obj: dict) -> "NamespaceInfo":
        return cls(
            vector_count=obj["vectorCount"],
            pending_vector_count=obj["pendingVectorCount"],
        )


@dataclass
class InfoResult:
    vector_count: int
    pending_vector_count: int
    index_size: int
    dimension: int
    similarity_function: str
    namespaces: Dict[str, NamespaceInfo]

    @classmethod
    def _from_json(cls, obj: dict) -> "InfoResult":
        return cls(
            vector_count=obj["vectorCount"],
            pending_vector_count=obj["pendingVectorCount"],
            index_size=obj["indexSize"],
            dimension=obj["dimension"],
            similarity_function=obj["similarityFunction"],
            namespaces={
                ns: NamespaceInfo._from_json(ns_info)
                for ns, ns_info in obj["namespaces"].items()
            },
        )
