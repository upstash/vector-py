from upstash_vector.types import Vector
from upstash_vector.errors import ClientError
from typing import List, Dict


def _convert_to_list(obj):
    class_name = obj.__class__.__name__

    if class_name == "list":
        return obj
    elif hasattr(obj, "tolist") and callable(getattr(obj, "tolist")):
        return obj.tolist()
    else:
        return list(obj)


def _tuple_to_vector(vector) -> Vector:
    if len(vector) < 2 or len(vector) > 3:
        raise ClientError("Tuple must be in the format (id, vector, metadata)")

    if len(vector) == 2:
        return Vector(id=vector[0], vector=_convert_to_list(vector[1]))

    return Vector(id=vector[0], vector=_convert_to_list(vector[1]), metadata=vector[2])


def _dict_to_vector(vector) -> Vector:
    if vector["id"] is None or vector["vector"] is None:
        raise ClientError("Vector dict must have 'id' and 'vector' fields defined.")

    metadata = None
    if vector.get("metadata") is not None:
        metadata = vector["metadata"]

    return Vector(
        id=vector["id"], vector=_convert_to_list(vector["vector"]), metadata=metadata
    )


def _tuple_or_dict_to_vectors(vector) -> Vector:
    if isinstance(vector, Vector):
        vector.vector = _convert_to_list(vector.vector)
        return vector
    elif isinstance(vector, tuple):
        return _tuple_to_vector(vector)
    elif isinstance(vector, Dict):
        return _dict_to_vector(vector)
    else:
        raise ClientError(
            f"Given object type is undefined for converting to vector: {vector}"
        )


def convert_to_vectors(vectors) -> List[Vector]:
    return [_tuple_or_dict_to_vectors(vector) for vector in vectors]
