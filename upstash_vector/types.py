import enum
from dataclasses import dataclass
from typing import Optional, List, Dict, TypedDict, Union, Protocol


class SupportsToList(Protocol):
    def tolist(self) -> List[float]:
        ...


@dataclass
class Vector:
    id: Union[int, str]
    vector: Union[List[float], SupportsToList]
    metadata: Optional[Dict] = None
    data: Optional[str] = None


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
    data: Optional[str] = None

    @classmethod
    def _from_json(cls, obj: dict) -> "FetchResult":
        return cls(
            id=obj["id"],
            vector=obj.get("vector"),
            metadata=obj.get("metadata"),
            data=obj.get("data"),
        )


@dataclass
class QueryResult:
    id: str
    score: float
    vector: Optional[List[float]] = None
    metadata: Optional[Dict] = None
    data: Optional[str] = None

    @classmethod
    def _from_json(cls, obj: dict) -> "QueryResult":
        return cls(
            id=obj["id"],
            score=obj["score"],
            vector=obj.get("vector"),
            metadata=obj.get("metadata"),
            data=obj.get("data"),
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


class MetadataUpdateMode(enum.Enum):
    """
    Whether to overwrite the whole metadata while updating
    it, or patch the metadata (insert new fields or update or delete existing fields)
    according to the `RFC 7396 JSON Merge Patch` algorithm.
    """

    OVERWRITE = "OVERWRITE"
    """Overwrite the metadata, and set it to a new value."""

    PATCH = "PATCH"
    """Patch the metadata according to Merge Patch algorithm."""


class QueryRequest(TypedDict, total=False):
    vector: Union[List[float], SupportsToList]
    """
    The vector value to query.
    
    Only and only one of `vector` or `data` fields must be provided.
    """

    data: str
    """
    Data to query for (after embedding it to a vector).
    
    Only and only one of `vector` or `data` fields must be provided.
    """

    top_k: int
    """
    How many vectors will be returned as the query result.
    
    When not specified, defaults to `10`.
    """

    include_vectors: bool
    """
    Whether the resulting `top_k` vectors will have their vector values or not.
    
    When not specified, defaults to `False`.
    """

    include_metadata: bool
    """
    Whether the resulting `top_k` vectors will have their metadata or not.
    
    When not specified, defaults to `False`.
    """

    include_data: bool
    """
    Whether the resulting `top_k` vectors will have their unstructured data or not.
    
    When not specified, defaults to `False`.
    """

    filter: str
    """
    Filter expression to narrow down the query results.
    
    When not specified, defaults to `""`(no filter).
    """
