from upstash_vector.types import Vector
from upstash_vector.errors import ClientError
from typing import List


def convert_to_list(obj):
    if isinstance(obj, list):
        return obj
    elif hasattr(obj, "tolist") and callable(getattr(obj, "tolist")):
        return obj.tolist()

    raise TypeError(
        f"Expected a list or something can be converted to a list(like numpy or pandas array) but got {type(obj)}"
    )


def _tuple_to_vector(vector) -> Vector:
    if len(vector) < 2 or len(vector) > 3:
        raise ClientError("Tuple must be in the format (id, vector, metadata)")

    if len(vector) == 2:
        return Vector(id=vector[0], vector=convert_to_list(vector[1]))

    return Vector(id=vector[0], vector=convert_to_list(vector[1]), metadata=vector[2])


def _dict_to_vector(vector) -> Vector:
    if vector["id"] is None or vector["vector"] is None:
        raise ClientError("Vector dict must have 'id' and 'vector' fields defined.")

    metadata = None
    if vector.get("metadata") is not None:
        metadata = vector["metadata"]

    return Vector(
        id=vector["id"], vector=convert_to_list(vector["vector"]), metadata=metadata
    )


def _tuple_or_dict_to_vectors(vector) -> Vector:
    if isinstance(vector, Vector):
        vector.vector = convert_to_list(vector.vector)
        return vector
    elif isinstance(vector, tuple):
        return _tuple_to_vector(vector)
    elif isinstance(vector, dict):
        return _dict_to_vector(vector)
    else:
        raise ClientError(
            f"Given object type is undefined for converting to vector: {vector}"
        )


def convert_to_vectors(vectors) -> List[Vector]:
    return [_tuple_or_dict_to_vectors(vector) for vector in vectors]
