# Define vector operations here:
# Upsert and query functions and signatures

from typing import Any

ResponseType = Any


class IndexOperations:
    def execute_request(self, payload, path) -> ResponseType:
        raise NotImplementedError("execute_request")

    # TODO: define list size here, maybe?
    # TODO: maybe, also define payload type
    def upsert(self, id: str | int, vector: list[float]) -> ResponseType:
        payload = {"id": id, "vector": vector}
        path = "/upsert"
        return self.execute_request(payload=payload, path=path)

    def query(self, vector: list[float], top_k: int, include_vectors: bool):
        payload = {"vector": vector, "topK": top_k, "includeVectors": include_vectors}
        path = "/query"
        return self.execute_request(payload=payload, path=path)

    def delete(self, id_list: list[int|str]):
        payload = id_list
        path = "/delete"
        return self.execute_request(payload=payload, path=path)
