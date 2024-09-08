from typing import List, Optional, Dict, Any
from upstash_vector.types import QueryResult
from upstash_vector.errors import ClientError

RESUMABLE_QUERY_VECTOR_PATH = "/resumable-query"
RESUMABLE_QUERY_DATA_PATH = "/resumable-query-data"
RESUMABLE_QUERY_NEXT_PATH = "/resumable-query-next"
RESUMABLE_QUERY_END_PATH = "/resumable-query-end"


class ResumableQuery:
    def __init__(
        self, payload: Dict[str, Any], client, namespace: Optional[str] = None
    ):
        self.payload = payload
        self.client = client
        self.namespace = namespace
        self.uuid = Optional[str]

    async def async_start(self) -> List[QueryResult]:
        if "vector" in self.payload:
            path = RESUMABLE_QUERY_VECTOR_PATH
        elif "data" in self.payload:
            path = RESUMABLE_QUERY_DATA_PATH
        else:
            raise ClientError("Payload must contain either 'vector' or 'data' key.")

        if self.namespace:
            path = f"{path}/{self.namespace}"

        result = await self.client._execute_request_async(
            payload=self.payload, path=path
        )
        self.uuid = result["uuid"]
        return result["scores"]

    def start(self) -> List[QueryResult]:
        if "vector" in self.payload:
            path = RESUMABLE_QUERY_VECTOR_PATH
        elif "data" in self.payload:
            path = RESUMABLE_QUERY_DATA_PATH
        else:
            raise ClientError("Payload must contain either 'vector' or 'data' key.")

        if self.namespace:
            path = f"{path}/{self.namespace}"

        result = self.client._execute_request(payload=self.payload, path=path)

        self.uuid = result["uuid"]
        if not self.uuid:
            raise ClientError("Resumable query could not be started.")

        return [QueryResult._from_json(obj) for obj in result.get("scores", [])]

    def fetch_next(self, additional_k: int) -> List[QueryResult]:
        if self.uuid is None:
            raise ClientError(
                "Resumable query has not been started. Call start() first."
            )
        payload = {"uuid": self.uuid, "additionalK": additional_k}
        result = self.client._execute_request(
            payload=payload, path=RESUMABLE_QUERY_NEXT_PATH
        )
        return [QueryResult._from_json(obj) for obj in result]

    async def async_fetch_next(self, additional_k: int) -> List[QueryResult]:
        if not self.uuid:
            raise ClientError(
                "Resumable query has not been started. Call start() first."
            )
        payload = {"uuid": self.uuid, "additionalK": additional_k}
        result = await self.client._execute_request_async(
            payload=payload, path=RESUMABLE_QUERY_NEXT_PATH
        )
        return [QueryResult._from_json(obj) for obj in result]

    def stop(self) -> str:
        if not self.uuid:
            raise ClientError(
                "Resumable query has not been started. Call start() first."
            )
        payload = {"uuid": self.uuid}
        result = self.client._execute_request(
            payload=payload, path=RESUMABLE_QUERY_END_PATH
        )
        self.uuid = None
        return result

    async def async_stop(self) -> str:
        if not self.uuid:
            raise ClientError(
                "Resumable query has not been started. Call start() first."
            )
        payload = {"uuid": self.uuid}
        result = await self.client._execute_request_async(
            payload=payload, path=RESUMABLE_QUERY_END_PATH
        )
        self.uuid = None
        return result
