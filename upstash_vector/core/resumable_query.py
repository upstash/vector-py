from typing import List, Optional, Dict, Any
from upstash_vector.types import QueryResult
from upstash_vector.errors import ClientError

RESUMABLE_QUERY_VECTOR_PATH = "/resumable-query"
RESUMABLE_QUERY_DATA_PATH = "/resumable-query-data"
RESUMABLE_QUERY_NEXT_PATH = "/resumable-query-next"
RESUMABLE_QUERY_END_PATH = "/resumable-query-end"


class ResumableQuery:
    """
    A class representing a resumable query for vector search operations.

    This class allows for starting, fetching next results, and stopping a resumable query.
    It supports both synchronous and asynchronous operations.

    Attributes:
        payload (Dict[str, Any]): The query payload.
        client: The client object for executing requests.
        namespace (Optional[str]): The namespace for the query.
        uuid (Optional[str]): The unique identifier for the resumable query session.
    """

    def __init__(
        self, payload: Dict[str, Any], client, namespace: Optional[str] = None
    ):
        """
        Initialize a ResumableQuery instance.

        Args:
            payload (Dict[str, Any]): The query payload.
            client: The client object for executing requests.
            namespace (Optional[str]): The namespace for the query. Defaults to None.
        """
        self.payload = payload
        self.client = client
        self.namespace = namespace
        self.uuid = None

    def __enter__(self):
        """
        Start the resumable query asynchronously.
        Enter the runtime context related to this object.
        The with statement will bind this method's return value to the target(s)
        specified in the as clause of the statement, if any.
        Returns:
            self: The ResumableQuery instance.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the runtime context related to this object.
        The parameters describe the exception that caused the context to be exited.
        If the context was exited without an exception, all three arguments will be None.
        Args:
            exc_type: The exception type if an exception was raised in the context, else None.
            exc_value: The exception instance if an exception was raised in the context, else None.
            traceback: The traceback if an exception was raised in the context, else None.
        """
        self.stop()

    async def __aenter__(self):
        """
        Enter the runtime context related to this object asynchronously.
        Returns:
            self: The ResumableQuery instance.
        """
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """
        Exit the runtime context related to this object asynchronously.
        Args:
            exc_type: The exception type if an exception was raised in the context, else None.
            exc_value: The exception instance if an exception was raised in the context, else None.
            traceback: The traceback if an exception was raised in the context, else None.
        """
        await self.async_stop()

    async def async_start(self) -> List[QueryResult]:
        """
        Start the resumable query asynchronously.

        Returns:
            List[QueryResult]: The initial query results.

        Raises:
            ClientError: If the payload doesn't contain 'vector' or 'data' key.
        """
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
        """
        Start the resumable query synchronously.

        Returns:
            List[QueryResult]: The initial query results.

        Raises:
            ClientError: If the payload doesn't contain 'vector' or 'data' key,
                         or if the resumable query couldn't be started.
        """
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
        """
        Fetch the next batch of results synchronously.

        Args:
            additional_k (int): The number of additional results to fetch.

        Returns:
            List[QueryResult]: The next batch of query results.

        Raises:
            ClientError: If the resumable query hasn't been started.
        """
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
        """
        Fetch the next batch of results asynchronously.

        Args:
            additional_k (int): The number of additional results to fetch.

        Returns:
            List[QueryResult]: The next batch of query results.

        Raises:
            ClientError: If the resumable query hasn't been started.
        """
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
        """
        Stop the resumable query synchronously.

        Returns:
            str: The result of stopping the query.

        Raises:
            ClientError: If the resumable query hasn't been started.
        """
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
        """
        Stop the resumable query asynchronously.

        Returns:
            str: The result of stopping the query.

        Raises:
            ClientError: If the resumable query hasn't been started.
        """
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
