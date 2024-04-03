from upstash_vector.types import DataPayload, Vector
from upstash_vector.errors import ClientError
from typing import List, Union


def convert_to_list(obj):
    if isinstance(obj, list):
        return obj
    elif hasattr(obj, "tolist") and callable(getattr(obj, "tolist")):
        return obj.tolist()

    raise TypeError(
        f"Expected a list or something can be converted to a list(like numpy or pandas array) but got {type(obj)}"
    )


def _tuple_to_vector(vector) -> Union[Vector, DataPayload]:
    if len(vector) < 2 or len(vector) > 3:
        raise ClientError("Tuple must be in the format (id, vector, metadata) or (id, data, metadata)")

    metadata = None
    if len(vector) == 3:
        metadata = vector[2]

    if isinstance(vector[1], str):
        return DataPayload(id=vector[0], data=vector[1], metadata=metadata)    

    return Vector(id=vector[0], vector=convert_to_list(vector[1]), metadata=metadata)


def _dict_to_vector(vector) -> Union[Vector, DataPayload]:
    if vector["id"] is None or (vector["vector"] is None and vector["data"] is None):
        raise ClientError("Vector dict must have 'id' and 'vector' or 'data' fields defined.")
    
    if vector["vector"] is not None and vector["data"] is not None:
        raise ClientError("either data or vector field can be given.")

    metadata = None
    if vector.get("metadata") is not None:
        metadata = vector["metadata"]

    if vector["vector"] is not None:
        return Vector(
            id=vector["id"], vector=convert_to_list(vector["vector"]), metadata=metadata
        )

    return DataPayload(
        id=vector["id"], data=vector["data"], metadata=metadata
    )
    
    
def _tuple_or_dict_to_vectors(vector) -> Union[Vector, DataPayload]:
    if isinstance(vector, Vector):
        vector.vector = convert_to_list(vector.vector)
        return vector
    elif isinstance(vector, DataPayload):
        return vector
    elif isinstance(vector, tuple):
        return _tuple_to_vector(vector)
    elif isinstance(vector, dict):
        return _dict_to_vector(vector)
    else:
        raise ClientError(
            f"Given object type is undefined for converting to vector: {vector}"
        )


def convert_to_vectors(vectors) -> List[Union[Vector, DataPayload]]:
    return [_tuple_or_dict_to_vectors(vector) for vector in vectors]
