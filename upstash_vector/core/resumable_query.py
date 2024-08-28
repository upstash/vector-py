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

    async def start(self) -> Dict[str, Any]:
        path = (
            RESUMABLE_QUERY_VECTOR_PATH
            if "vector" in self.payload
            else RESUMABLE_QUERY_DATA_PATH
        )
        if self.namespace:
            path = f"{self.namespace}/{path}"
        result = await self.client._execute_request_async(
            payload=self.payload, path=path
        )
        self.uuid = result["uuid"]
        return result

    async def fetch_next(self, additional_k: int) -> List[QueryResult]:
        if not self.uuid:
            raise ClientError(
                "Resumable query has not been started. Call start() first."
            )
        payload = {"uuid": self.uuid, "additionalK": additional_k}
        result = await self.client._execute_request_async(
            payload=payload, path=RESUMABLE_QUERY_NEXT_PATH
        )
        return [QueryResult._from_json(obj) for obj in result]

    async def stop(self) -> str:
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
