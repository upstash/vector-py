from requests import Session
from upstash_vector.http import execute_with_parameters, generate_headers
from upstash_vector.core.index_operations import IndexOperations
from typing import Any
from os import environ


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
        self._retries = retries
        self._retry_interval = retry_interval  # Seconds
        self._session = Session()

        self._headers = generate_headers(token)

    def _execute_request(self, payload: Any = "", path: str = ""):
        url_with_path = f"{self._url}{path}"
        return execute_with_parameters(
            url=url_with_path,
            session=self._session,
            headers=self._headers,
            retry_interval=self._retry_interval,
            retries=self._retries,
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
