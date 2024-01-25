from httpx import Client, AsyncClient
from typing import Any
from os import environ

from upstash_vector.http import (
    execute_with_parameters,
    execute_with_parameters_async,
    generate_headers,
)
from upstash_vector.core.index_operations import IndexOperations, AsyncIndexOperations


class Index(IndexOperations):
    """
    An Upstash Vector client that uses the Upstash Vector API to manage index operations.

    Initialization example:

    ```python
    from upstash_vector import Index

    index = Index(url=<url>, token=<token>)

    # alternatively, configure retry mechanism as well

    # retry 5 times, waiting 100ms between consequent requests
    index = Index(url=<url>, token=<token>, retries=5, retry_interval=0.1)
    ```
    """

    def __init__(
        self, url: str, token: str, retries: int = 3, retry_interval: float = 1.0
    ):
        self._url = url
        self._client = Client()
        self._retries = retries
        self._retry_interval = retry_interval
        self._headers = generate_headers(token)

    def _execute_request(self, payload: Any = "", path: str = ""):
        url_with_path = f"{self._url}{path}"
        return execute_with_parameters(
            url=url_with_path,
            client=self._client,
            headers=self._headers,
            retries=self._retries,
            retry_interval=self._retry_interval,
            payload=payload,
        )

    @classmethod
    def from_env(cls, retries: int = 3, retry_interval: float = 1.0) -> "Index":
        """
        Load the credentials from environment, and returns a client.
        """

        return cls(
            environ["UPSTASH_VECTOR_REST_URL"],
            environ["UPSTASH_VECTOR_REST_TOKEN"],
            retries,
            retry_interval,
        )


class AsyncIndex(AsyncIndexOperations):
    """
    An Asynchronous Upstash Vector client that uses the Upstash Vector API to manage index operations.

    Initialization example:

    ```python
    from upstash_vector import AsyncIndex

    index = AsyncIndex(url=<url>, token=<token>)

    # alternatively, configure retry mechanism as well

    # retry 5 times, waiting 100ms between consequent requests
    index = AsyncIndex(url=<url>, token=<token>, retries=5, retry_interval=0.1)
    ```
    """

    def __init__(
        self,
        url: str,
        token: str,
        retries: int = 3,
        retry_interval: float = 1.0,
    ):
        self._url = url
        self._headers = generate_headers(token)
        self._client = AsyncClient()
        self._retries = retries
        self._retry_interval = retry_interval

    async def _execute_request_async(self, payload: Any = "", path: str = ""):
        url_with_path = f"{self._url}{path}"
        return await execute_with_parameters_async(
            client=self._client,
            url=url_with_path,
            headers=self._headers,
            retries=self._retries,
            retry_interval=self._retry_interval,
            payload=payload,
        )

    @classmethod
    def from_env(cls, retries: int = 3, retry_interval: float = 1.0) -> "AsyncIndex":
        """
        Load the credentials from environment, and returns a client.
        """

        return cls(
            environ["UPSTASH_VECTOR_REST_URL"],
            environ["UPSTASH_VECTOR_REST_TOKEN"],
            retries,
            retry_interval,
        )
