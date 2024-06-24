from typing import List, Union, Dict, Any, Optional, Tuple

from upstash_vector.errors import ClientError
from upstash_vector.types import Data, QueryRequest, Vector


def convert_to_list(obj):
    if isinstance(obj, list):
        return obj
    elif hasattr(obj, "tolist") and callable(getattr(obj, "tolist")):
        return obj.tolist()

    raise TypeError(
        f"Expected a list or something can be converted to a list(like numpy or pandas array) but got {type(obj)}"
    )


def _get_payload_element(
    id: Union[int, str],
    payload: Union[str, List[float]],
    metadata: Optional[Dict[str, Any]] = None,
    data: Optional[str] = None,
) -> Union[Vector, Data]:
    if isinstance(payload, str):
        return Data(id=id, data=payload, metadata=metadata)

    return Vector(id=id, vector=convert_to_list(payload), metadata=metadata, data=data)


def _get_payload_element_from_dict(
    id: Union[int, str],
    vector: Optional[List[float]] = None,
    data: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Union[Vector, Data]:
    if vector is None and data is None:
        raise ClientError(
            "Vector dict must have one of `vector` or `data` fields defined."
        )

    if vector is None:
        # data cannot be none at this point
        return Data(id=id, data=data, metadata=metadata)  # type:ignore[arg-type]

    return Vector(id=id, vector=convert_to_list(vector), metadata=metadata, data=data)


def _tuple_or_dict_to_vectors(vector) -> Union[Vector, Data]:
    if isinstance(vector, Vector):
        vector.vector = convert_to_list(vector.vector)
        return vector
    elif isinstance(vector, Data):
        return vector
    elif isinstance(vector, tuple):
        return _get_payload_element(*vector)
    elif isinstance(vector, dict):
        return _get_payload_element_from_dict(**vector)
    else:
        raise ClientError(
            f"Given object type is undefined for converting to vector: {vector}"
        )


def convert_to_vectors(vectors) -> List[Union[Vector, Data]]:
    return [_tuple_or_dict_to_vectors(vector) for vector in vectors]


def convert_to_payload(
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
        is_vector = isinstance(vector, Vector)
        if expecting_vectors != is_vector:
            raise ClientError(
                "All items should either have the `data` or the `vector` field."
                " Received items from both kinds. Please send them separately."
            )

        if is_vector:
            payload.append(
                {
                    "id": vector.id,
                    "vector": vector.vector,  # type: ignore[union-attr]
                    "metadata": vector.metadata,
                    "data": vector.data,
                }
            )
        else:
            payload.append(
                {
                    "id": vector.id,
                    "data": vector.data,
                    "metadata": vector.metadata,
                }
            )

    return payload, expecting_vectors


def convert_query_requests_to_payload(
    queries: List[QueryRequest],
) -> Tuple[bool, List[Dict[str, Any]]]:
    has_vector_query = False
    has_data_query = False

    payloads = []

    for query in queries:
        payload = {
            "topK": query.get("top_k", 10),
            "includeVectors": query.get("include_vectors", False),
            "includeMetadata": query.get("include_metadata", False),
            "includeData": query.get("include_data", False),
            "filter": query.get("filter", ""),
        }

        vector = query.get("vector")
        data = query.get("data")

        if data is None and vector is None:
            raise ClientError("either `data` or `vector` values must be given")
        if data is not None and vector is not None:
            raise ClientError(
                "`data` and `vector` values cannot be given at the same time"
            )

        if data is not None:
            if has_vector_query:
                raise ClientError(
                    "`data` and `vector` queries cannot be mixed in the same batch."
                )

            has_data_query = True
            payload["data"] = data
        else:
            if has_data_query:
                raise ClientError(
                    "`data` and `vector` queries cannot be mixed in the same batch."
                )

            has_vector_query = True
            payload["vector"] = convert_to_list(vector)

        payloads.append(payload)

    return has_vector_query, payloads
