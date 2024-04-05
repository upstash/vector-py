from upstash_vector.types import Data, Vector
from upstash_vector.errors import ClientError
from typing import List, Union, Dict, Any


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
        metadata: Dict[str, Any] = None
) -> Union[Vector, Data]:

    if isinstance(payload, str):
        return Data(id=id, data=payload, metadata=metadata)

    return Vector(id=id, vector=convert_to_list(payload), metadata=metadata)


def _get_payload_element_from_dict(
        id: Union[int, str],
        vector: List[float] = None,
        data: str = None,
        metadata: Dict[str, Any] = None
) -> Union[Vector, Data]:
    is_vector, is_data = not vector is None, not data is None

    if not is_vector and not is_data:
        raise ClientError(
            "Vector dict must have one of `vector` or `data` fields defined."
        )

    if is_vector and is_data:
        raise ClientError("only one of `data` or `vector` field can be given.")

    if is_vector:
        return Vector(
            id=id, vector=convert_to_list(vector), metadata=metadata
        )

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


def convert_to_payload(vectors: List[Union[Vector, Data]]) -> List[Dict[str, Any]]:
    """
    Converts a list of Vector or Data to payload.

    The list can only contain one of the two types. Otherwise, raises
    an exception.
    """
    is_vector = isinstance(vectors[0], Vector)
    try:
        if is_vector:
            return [
                {"id": vector.id, "vector": vector.vector, "metadata": vector.metadata}
                for vector in vectors
            ]
        else:
            return [
                {"id": vector.id, "data": vector.data, "metadata": vector.metadata}
                for vector in vectors
            ]
    except AttributeError as exc:
        raise ClientError(
            "All items should either have the `data` or the `vector` field."
            " Received items from both kinds. Please send them seperately."
        ) from None
