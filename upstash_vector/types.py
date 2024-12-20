import enum
from dataclasses import dataclass
from typing import Dict, List, Optional, Protocol, Tuple, TypedDict, Union


class SupportsToList(Protocol):
    def tolist(self) -> List[float]: ...


@dataclass
class SparseVector:
    indices: Union[List[int], SupportsToList]
    """
    List of dimensions that have non-zero values.
    
    All the signed 32 bit integer range is valid
    as the dimension indices.
    """

    values: Union[List[float], SupportsToList]
    """
    Values of the non-zero dimensions.
    
    It must be of the same size as the `indices`.
    """

    @classmethod
    def _from_json(cls, obj: Optional[dict]) -> Optional["SparseVector"]:
        if not obj:
            return None

        return SparseVector(
            obj["indices"],
            obj["values"],
        )


TupleAsSparseVectorT = Tuple[
    Union[List[int], SupportsToList], Union[List[float], SupportsToList]
]


@dataclass
class Vector:
    id: Union[int, str]
    vector: Optional[Union[List[float], SupportsToList]] = None
    metadata: Optional[Dict] = None
    data: Optional[str] = None
    sparse_vector: Optional[Union[SparseVector, TupleAsSparseVectorT]] = None


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
    sparse_vector: Optional[SparseVector] = None

    @classmethod
    def _from_json(cls, obj: dict) -> "FetchResult":
        return cls(
            id=obj["id"],
            vector=obj.get("vector"),
            metadata=obj.get("metadata"),
            data=obj.get("data"),
            sparse_vector=SparseVector._from_json(obj.get("sparseVector")),
        )


@dataclass
class QueryResult:
    id: str
    score: float
    vector: Optional[List[float]] = None
    metadata: Optional[Dict] = None
    data: Optional[str] = None
    sparse_vector: Optional[SparseVector] = None

    @classmethod
    def _from_json(cls, obj: dict) -> "QueryResult":
        return cls(
            id=obj["id"],
            score=obj["score"],
            vector=obj.get("vector"),
            metadata=obj.get("metadata"),
            data=obj.get("data"),
            sparse_vector=SparseVector._from_json(obj.get("sparseVector")),
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


class WeightingStrategy(enum.Enum):
    """
    For sparse vectors, what kind of weighting strategy
    should be used while querying the matching non-zero
    dimension values of the query vector with the documents.

    If not provided, no weighting will be used.
    """

    IDF = "IDF"
    """
    Inverse document frequency.
    
    It is recommended to use this weighting strategy for
    BM25 sparse embedding models.
    
    It is calculated as
    
    ln(((N - n(q) + 0.5) / (n(q) + 0.5)) + 1) where
    N:    Total number of sparse vectors.
    n(q): Total number of sparse vectors having non-zero value 
          for that particular dimension.
    ln:   Natural logarithm
    
    The values of N and n(q) are maintained by Upstash as the
    vectors are indexed.
    """


class FusionAlgorithm(enum.Enum):
    """
    Fusion algorithm to use while fusing scores
    from dense and sparse components of a hybrid index.

    If not provided, defaults to `RRF`.
    """

    RRF = "RRF"
    """
    Reciprocal rank fusion.
    
    Each sorted score from the dense and sparse indexes are
    mapped to 1 / (rank + K), where rank is the order of the
    score in the dense or sparse scores and K is a constant
    with the value of 60.
    
    Then, scores from the dense and sparse components are
    deduplicated (i.e. if a score for the same vector is present
    in both dense and sparse scores, the mapped scores are
    added; otherwise individual mapped scores are used) 
    and the final result is returned as the topK values
    of this final list.
    
    In short, this algorithm just takes the order of the scores
    into consideration.
    """

    DBSF = "DBSF"
    """
    Distribution based score fusion.
    
    Each sorted score from the dense and sparse indexes are
    normalized as 
    (s - (mean - 3 * stddev)) / ((mean + 3 * stddev) - (mean - 3 * stddev))
    where s is the score, (mean - 3 * stddev) is the minimum,
    and (mean + 3 * stddev) is the maximum tail ends of the distribution.
    
    Then, scores from the dense and sparse components are
    deduplicated (i.e. if a score for the same vector is present
    in both dense and sparse scores, the normalized scores are
    added; otherwise individual normalized scores are used) 
    and the final result is returned as the topK values
    of this final list.
    
    In short, this algorithm takes distribution of the scores
    into consideration as well, as opposed to the `RRF`.
    """


class QueryMode(enum.Enum):
    """
    Query mode for hybrid indexes with Upstash-hosted
    embedding models.

    Specifies whether to run the query in only the
    dense index, only the sparse index, or in both.

    If not provided, defaults to `HYBRID`.
    """

    HYBRID = "HYBRID"
    """
    Runs the query in hybrid index mode, after embedding
    the raw text data into dense and sparse vectors.
    
    Query results from the dense and sparse index components
    of the hybrid index are fused before returning the result.
    """

    DENSE = "DENSE"
    """
    Runs the query in dense index mode, after embedding
    the raw text data into a dense vector.
    
    Only the query results from the dense index component
    of the hybrid index is returned.
    """

    SPARSE = "SPARSE"
    """
    Runs the query in sparse index mode, after embedding
    the raw text data into a sparse vector.
    
    Only the query results from the sparse index component
    of the hybrid index is returned.
    """


class QueryRequest(TypedDict, total=False):
    vector: Union[List[float], SupportsToList]
    """
    The vector value to query.
    
    It must be provided only for dense and hybrid indexes.
    """

    sparse_vector: Union[SparseVector, TupleAsSparseVectorT]
    """
    The sparse vector value to query.
    
    It must be provided only for sparse or hybrid indexes.
    """

    data: str
    """
    Data to query for (after embedding it to a vector/sparse vector).
    
    It must be provided only for indexes created with Upstash hosted models.
    When provided, `vector` or `sparse_vector` fields should not be set.
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

    weighting_strategy: WeightingStrategy
    """
    Weighting strategy to be used for sparse vectors.
    
    It must be provided only for sparse and hybrid indexes.
    
    When not specified, defaults to no extra weighting (i.e. 1.0).
    """

    fusion_algorithm: FusionAlgorithm
    """
    Fusion algorithm to use while fusing scores
    from dense and sparse components of a hybrid index.
    
    It must be provided only for hybrid indexes.
    
    When not specified, defaults to `RRF`.
    """

    query_mode: QueryMode
    """
    Specifies whether to run the query in only the
    dense index, only the sparse index, or in both.
    
    It must be provided only for hybrid indexes with 
    Upstash-hosted embedding models.
    
    When not specified, defaults to `HYBRID`.
    """
