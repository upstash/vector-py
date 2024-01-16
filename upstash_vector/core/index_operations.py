# Define vector operations here:
# Upsert and query functions and signatures

from typing import Union, List, Dict
from upstash_vector.errors import ClientError

from upstash_vector.types import (
    FetchResponse,
    IdT,
    QuerySingularResponse,
    SingleVectorResponse,
    Vector,
    RangeResponse,
    DeleteResponse,
    QueryResponse,
    VectorT,
    ResponseStr,
)
from upstash_vector.utils import convert_to_vectors


UPSERT_PATH = "/upsert"
QUERY_PATH = "/query"
DELETE_PATH = "/delete"
RESET_PATH = "/reset"
RANGE_PATH = "/range"
FETCH_PATH = "/fetch"

include_vectors_json_field = "includeVectors"
include_metadata_json_field = "includeMetadata"
top_k_json_field = "topK"


class IndexOperations:
    def _execute_request(self, payload, path):
        raise NotImplementedError("execute_request")

    def upsert(
        self,
        vectors: Union[List[Dict], List[tuple], List[Vector]],
    ) -> ResponseStr:
        """
        Upserts(update or insert) vectors. There are 3 ways to upsert vector.

        Example usages:

        ```python
        res = index.upsert(
            vectors=[
                ("id1", [0.1, 0.2], {"metadata_field": "metadata_value"}),
                ("id2", [0.3,0.4])
            ]
        )

        # OR

        res = index.upsert(
            vectors=[
                {"id": "id3", "vector": [0.1, 0.2], "metadata": {"metadata_f": "metadata_v"}},
                {"id": "id4", "vector": [0.5, 0.6]},
            ]
        )

        # OR

        from upstash_vector import Vector
        res = index.upsert(
            vectors=[
                Vector(id="id5", vector=[1, 2], metadata={"metadata_f": "metadata_v"}),
                Vector(id="id6", vector=[6, 7]),
            ]
        )
        ```
        """
        vectors = convert_to_vectors(vectors)
        payload = [
            {"id": vector.id, "vector": vector.vector, "metadata": vector.metadata}
            for vector in vectors
        ]

        return self._execute_request(payload=payload, path=UPSERT_PATH)

    def query(
        self,
        vector: VectorT,
        top_k: int = 10,
        include_vectors: bool = False,
        include_metadata: bool = False,
    ) -> QueryResponse:
        """
        Query a vector from the index.

        Response.vector returns None if the vector does not exist.

        :param vector: list of floats for the values of vector.
        :param top_k: number that indicates how many vectors will be returned as the query result.
        :param include_vectors: bool value that indicates whether the resulting top_k vectors will have their vector values shown.
        :param include_metadata: bool value that indicates whether the resulting top_k vectors will have their metadata shown.

        Example usage:

        ```python
        query_res = index.query(
            vector=[0.6, 0.9],
            top_k=3,
            include_vectors=True,
            include_metadata=True,
        )
        vector = query_res.vector
        ```
        """
        payload = {
            "vector": vector,
            top_k_json_field: top_k,
            include_vectors_json_field: include_vectors,
            include_metadata_json_field: include_metadata,
        }
        return [
            QuerySingularResponse(obj)
            for obj in self._execute_request(payload=payload, path=QUERY_PATH)
        ]

    def delete(self, ids: Union[IdT, List[IdT]]) -> DeleteResponse:
        """
        Deletes the given vector(s) with given ids.

        Response.deleted_count returns deleted vector amount.

        :param ids: Singular or list of ids of vector(s) to be deleted from the index.

        Example usage:

        ```python
        # deletes vectors with ids "0", "1", "2"
        index.delete(["0", "1", "2"])

        # deletes single vector
        index.delete("0")
        ```
        """
        if not isinstance(ids, List):
            ids = [ids]
        return DeleteResponse(self._execute_request(payload=ids, path=DELETE_PATH))

    def reset(self) -> ResponseStr:
        """
        Resets the index. All vectors are removed.

        Example usage:

        ```python
        index.reset()
        ```
        """
        return self._execute_request(path=RESET_PATH, payload=None)

    def range(
        self,
        cursor: str = "",
        limit: int = 1,
        include_vectors: bool = False,
        include_metadata: bool = False,
    ) -> RangeResponse:
        """
        Scans the vectors starting from `cursor`, returns at most `limit` many vectors.

        :param cursor: marker that indicates where the scanning was left off when running through all existing vectors.
        :param limit: limits how many vectors will be fetched with the request.
        :param include_vectors: bool value that indicates whether the resulting top_k vectors will have their vector values shown.
        :param include_metadata: bool value that indicates whether the resulting top_k vectors will have their metadata shown.

        Example usage:

        ```python
        res = index.range(cursor="cursor", limit=4, include_vectors=True, include_metadata=True)
        ```
        """
        if limit <= 0:
            raise ClientError("limit must be greater than 0")

        payload = {
            "cursor": cursor,
            "limit": limit,
            include_vectors_json_field: include_vectors,
            include_metadata_json_field: include_metadata,
        }
        return RangeResponse(self._execute_request(payload=payload, path=RANGE_PATH))

    def fetch(
        self,
        ids: List[IdT],
        include_vectors: bool = False,
        include_metadata: bool = False,
    ) -> FetchResponse:
        """
        Fetches details of a vector.

        :param ids: List of vector ids to fetch details of.
        :param include_vectors: bool value that indicates whether the resulting top_k vectors will have their vector values shown.
        :param include_metadata: bool value that indicates whether the resulting top_k vectors will have their metadata shown.

        Example usage:

        ```python
        res = index.fetch(["id1", "id2"], include_vectors=True, include_metadata=True)
        ```
        """
        payload = {
            "ids": ids,
            include_vectors_json_field: include_vectors,
            include_metadata_json_field: include_metadata,
        }
        return [
            SingleVectorResponse(vector) if vector is not None else None
            for vector in self._execute_request(payload=payload, path=FETCH_PATH)
        ]
