from httpx import AsyncClient, AsyncHTTPTransport
from typing import Any
from os import environ

from upstash_vector.http import execute_with_parameters_async, generate_headers
from upstash_vector.core.index_operations import AsyncIndexOperations


class AsyncIndex(AsyncIndexOperations):
    """
    An Asynchronous Upstash Vector client that uses the Upstash Vector API to manage index operations.

    Initialization example:

    ```python
    from upstash_vector import AsyncIndex

    index = AsyncIndex(url=<url>, token=<token>)

    # alternatively, configure retry mechanism as well

    # retry 5 times
    index = AsyncIndex(url=<url>, token=<token>, retries=5)
    ```
    """

    def __init__(
        self,
        url: str,
        token: str,
        retries: int = 3,
    ):
        self._url = url
        self._headers = generate_headers(token)
        self._client = AsyncClient(transport=AsyncHTTPTransport(retries=retries))

    async def _execute_request_async(self, payload: Any = "", path: str = ""):
        url_with_path = f"{self._url}{path}"
        return await execute_with_parameters_async(
            client=self._client,
            url=url_with_path,
            headers=self._headers,
            payload=payload,
        )

    @classmethod
    def from_env(cls, retries: int = 3) -> "AsyncIndex":
        """
        Load the credentials from environment, and returns a client.
        """

        return cls(
            environ["UPSTASH_VECTOR_REST_URL"],
            environ["UPSTASH_VECTOR_REST_TOKEN"],
            retries,
        )
