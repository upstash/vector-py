import os
import time
from typing import Any, Dict
from httpx import AsyncClient
from requests import Session
from platform import python_version

from upstash_vector import __version__
from upstash_vector.errors import UpstashError


def generate_headers(token) -> Dict[str, str]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Upstash-Telemetry-Sdk": f"upstash-vector-py@v{__version__}",
        "Upstash-Telemetry-Runtime": f"python@v{python_version()}",
    }

    if os.getenv("VERCEL"):
        platform = "vercel"
    elif os.getenv("AWS_REGION"):
        platform = "aws"
    else:
        platform = "unknown"

    headers["Upstash-Telemetry-Platform"] = platform

    return headers


def execute_with_parameters(
    url: str,
    session: Session,
    headers: Dict[str, str],
    retry_interval: float,
    retries: int,
    payload: Any,
) -> Any:
    last_error = None
    response = None
    for attempts_left in range(max(0, retries), -1, -1):
        try:
            response = session.post(url=url, headers=headers, json=payload).json()
            break

        except Exception as e:
            last_error = e
            if attempts_left > 0:
                time.sleep(retry_interval)

    if response is None:
        assert last_error is not None
        raise last_error

    if response.get("error"):
        raise UpstashError(response["error"])

    return response["result"]


async def execute_with_parameters_async(
    client: AsyncClient,
    url: str,
    headers: Dict[str, str],
    payload: Any,
) -> Any:
    request = await client.post(url=url, headers=headers, json=payload)
    response = request.json()

    if response.get("error"):
        raise UpstashError(response["error"])

    return response["result"]
