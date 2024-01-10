from requests import Session
from upstash_vector.core.index_operations import IndexOperations
from typing import Any


class Index(IndexOperations):
    def __init__(self, url: str, token: str):
        self._url = url
        self._session = Session()

        # TODO: refactor headers to another func maybe
        self._headers = {
            "Authorization": f"Bearer {token}",
        }

    # TODO: define payload type maybe?
    def execute_request(self, payload: Any, path: str):
        # TODO: send request here.
        url_with_path = f"{self._url}{path}"
        return self._session.post(
            url=url_with_path, headers=self._headers, json=payload
        ).json()
