from upstash_vector.types import Data, Vector
from upstash_vector.errors import ClientError
from typing import List, Union, Dict, Any, Optional, Tuple


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
) -> Union[Vector, Data]:
    if isinstance(payload, str):
        return Data(id=id, data=payload, metadata=metadata)

    return Vector(id=id, vector=convert_to_list(payload), metadata=metadata)


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

    if vector is not None and data is not None:
        raise ClientError("only one of `data` or `vector` field can be given.")

    if data is None:
        return Vector(id=id, vector=convert_to_list(vector), metadata=metadata)

    return Data(id=id, data=data, metadata=metadata)


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
    is_vector = isinstance(vectors[0], Vector)
    try:
        if is_vector:
            return [
                {"id": vector.id, "vector": vector.vector, "metadata": vector.metadata}  # type: ignore[union-attr]
                for vector in vectors
            ], is_vector
        else:
            return [
                {"id": vector.id, "data": vector.data, "metadata": vector.metadata}  # type: ignore[union-attr]
                for vector in vectors
            ], is_vector
    except AttributeError:
        raise ClientError(
            "All items should either have the `data` or the `vector` field."
            " Received items from both kinds. Please send them separately."
        )
