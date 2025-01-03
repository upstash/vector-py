from typing import Any, Dict, List, Sequence, Tuple, Union

from upstash_vector.errors import ClientError
from upstash_vector.types import (
    Data,
    QueryRequest,
    SparseVector,
    SupportsToList,
    TupleAsSparseVectorT,
    Vector,
)


def sequence_to_vectors(
    vectors: Sequence[Union[dict, tuple, Vector, Data]],
) -> List[Union[Vector, Data]]:
    return [_parse_vector(vector) for vector in vectors]


def _parse_vector(vector: Union[dict, tuple, Vector, Data]) -> Union[Vector, Data]:
    if isinstance(vector, Vector):
        dense = vector.vector
        if dense is not None:
            vector.vector = to_list(dense)

        sparse = vector.sparse_vector
        if sparse is not None:
            vector.sparse_vector = to_sparse_vector(sparse)

        return vector
    elif isinstance(vector, Data):
        return vector
    elif isinstance(vector, tuple):
        return _parse_vector_from_tuple(vector)
    elif isinstance(vector, dict):
        return _parse_vector_from_dict(vector)
    else:
        raise ClientError(
            f"Given object type is undefined for converting to vector: {vector}"
        )


def to_list(maybe_list: Union[list, SupportsToList]) -> list:
    if isinstance(maybe_list, list):
        return maybe_list
    elif hasattr(maybe_list, "tolist") and callable(getattr(maybe_list, "tolist")):
        return maybe_list.tolist()

    raise TypeError(
        f"Expected a list or something can be converted to a "
        f"list(like numpy or pandas array) but got {type(maybe_list)}"
    )


def to_sparse_vector(
    maybe_sparse_vector: Union[SparseVector, TupleAsSparseVectorT],
) -> SparseVector:
    if isinstance(maybe_sparse_vector, SparseVector):
        maybe_sparse_vector.indices = to_list(maybe_sparse_vector.indices)
        maybe_sparse_vector.values = to_list(maybe_sparse_vector.values)
        return maybe_sparse_vector
    elif isinstance(maybe_sparse_vector, tuple):
        if len(maybe_sparse_vector) != 2:
            raise ClientError(
                "The tuple for sparse vector should contain two lists; "
                "one for indices, and one for values."
            )

        sparse = SparseVector(
            to_list(maybe_sparse_vector[0]), to_list(maybe_sparse_vector[1])
        )
        return sparse
    else:
        raise ClientError("`sparse_vector` must be a `SparseVector` or `tuple`.")


def _parse_vector_from_tuple(t: tuple) -> Union[Vector, Data]:
    if len(t) < 2:
        raise ClientError(
            "The tuple must contain at least two elements; "
            "one for id, and other for vector or sparse vector."
        )

    id = t[0]
    if isinstance(t[1], str):
        return _parse_data_from_tuple(t, id)

    if isinstance(t[1], (SparseVector, tuple)):
        return _parse_sparse_vector_from_tuple(t, id)

    dense = to_list(t[1])
    if len(t) > 2 and isinstance(t[2], (SparseVector, tuple)):
        return _parse_hybrid_vector_from_tuple(t, id, dense)

    return _parse_dense_vector_from_tuple(t, id, dense)


def _parse_data_from_tuple(t: tuple, id: str) -> Data:
    data = t[1]
    if len(t) > 2:
        metadata = t[2]
    else:
        metadata = None

    return Data(id=id, data=data, metadata=metadata)


def _parse_sparse_vector_from_tuple(t: tuple, id: str) -> Vector:
    sparse = to_sparse_vector(t[1])

    if len(t) > 2:
        metadata = t[2]
        if len(t) > 3:
            data = t[3]
        else:
            data = None
    else:
        metadata = None
        data = None

    return Vector(
        id=id,
        sparse_vector=sparse,
        metadata=metadata,
        data=data,
    )


def _parse_hybrid_vector_from_tuple(t: tuple, id: str, dense: List[float]) -> Vector:
    sparse = to_sparse_vector(t[2])

    if len(t) > 3:
        metadata = t[3]
        if len(t) > 4:
            data = t[4]
        else:
            data = None
    else:
        metadata = None
        data = None

    return Vector(
        id=id,
        vector=dense,
        sparse_vector=sparse,
        metadata=metadata,
        data=data,
    )


def _parse_dense_vector_from_tuple(t: tuple, id: str, dense: List[float]) -> Vector:
    if len(t) > 2:
        metadata = t[2]
        if len(t) > 3:
            data = t[3]
        else:
            data = None
    else:
        metadata = None
        data = None

    return Vector(
        id=id,
        vector=dense,
        metadata=metadata,
        data=data,
    )


def _parse_vector_from_dict(d: dict) -> Union[Vector, Data]:
    id = d["id"]
    vector = d.get("vector")
    sparse_vector = d.get("sparse_vector")
    data = d.get("data")
    metadata = d.get("metadata")

    if vector is None and sparse_vector is None and data is not None:
        return Data(id=id, data=data, metadata=metadata)

    if vector is None and sparse_vector is None:
        raise ClientError(
            "The dict for vector should contain `vector` "
            "and/or `sparse_vector` when it does not contain `data`."
        )

    if vector is not None:
        vector = to_list(vector)

    if sparse_vector is not None:
        sparse_vector = to_sparse_vector(sparse_vector)

    return Vector(
        id=id,
        vector=vector,
        sparse_vector=sparse_vector,
        metadata=metadata,
        data=data,
    )


def vectors_to_payload(
    vectors: List[Union[Vector, Data]],
) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Converts a list of Vector or Data to payload.

    The list can only contain one of the two types. Otherwise, raises
    an exception.

    Returns the payload and whether it is Vector or Data.
    """
    expecting_vectors = isinstance(vectors[0], Vector)
    payload = []
    for vector in vectors:
        if isinstance(vector, Vector):
            if not expecting_vectors:
                raise ClientError(
                    "All items should either have the `data` or the `vector` and/or `sparse_vector` field."
                    " Received items from both kinds. Please send them separately."
                )

            payload.append(_vector_to_payload(vector))
        else:
            if expecting_vectors:
                raise ClientError(
                    "All items should either have the `data` or the `vector` and/or `sparse_vector` field."
                    " Received items from both kinds. Please send them separately."
                )

            payload.append(
                {
                    "id": vector.id,
                    "data": vector.data,
                    "metadata": vector.metadata,
                }
            )

    return payload, expecting_vectors


def _vector_to_payload(vector: Vector) -> Dict[str, Any]:
    if vector.sparse_vector is not None:
        sparse = {
            "indices": vector.sparse_vector.indices,  # type: ignore[union-attr]
            "values": vector.sparse_vector.values,  # type: ignore[union-attr]
        }
    else:
        sparse = None

    return {
        "id": vector.id,
        "vector": vector.vector,
        "sparseVector": sparse,
        "metadata": vector.metadata,
        "data": vector.data,
    }


def query_requests_to_payload(
    queries: List[QueryRequest],
) -> Tuple[bool, List[Dict[str, Any]]]:
    has_data_query = False
    payloads = []

    for query in queries:
        payload: Dict[str, Any] = {}

        if "top_k" in query:
            payload["topK"] = query["top_k"]

        if "include_vectors" in query:
            payload["includeVectors"] = query["include_vectors"]

        if "include_metadata" in query:
            payload["includeMetadata"] = query["include_metadata"]

        if "include_data" in query:
            payload["includeData"] = query["include_data"]

        if "filter" in query:
            payload["filter"] = query["filter"]

        if "weighting_strategy" in query:
            payload["weightingStrategy"] = query["weighting_strategy"].value

        if "fusion_algorithm" in query:
            payload["fusionAlgorithm"] = query["fusion_algorithm"].value

        vector = query.get("vector")
        sparse_vector = query.get("sparse_vector")
        data = query.get("data")

        if data is not None:
            if vector is not None or sparse_vector is not None:
                raise ClientError(
                    "When the query contains `data`, it can't contain `vector` or `sparse_vector`."
                )

            has_data_query = True
            payload["data"] = data
        else:
            if vector is None and sparse_vector is None:
                raise ClientError(
                    "Query must contain at least one of `vector`, `sparse_vector`, or `data`."
                )

            if has_data_query:
                raise ClientError(
                    "`data` and `vector`/`sparse_vector` queries cannot be mixed in the same batch."
                )

            if vector is not None:
                payload["vector"] = to_list(vector)

            if sparse_vector is not None:
                sparse = to_sparse_vector(sparse_vector)
                payload["sparseVector"] = {
                    "indices": sparse.indices,
                    "values": sparse.values,
                }

        payloads.append(payload)

    return not has_data_query, payloads
