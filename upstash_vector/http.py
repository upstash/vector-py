import asyncio
import os
import time
from platform import python_version
from typing import Any, Dict

from httpx import Client, AsyncClient

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
    client: Client,
    headers: Dict[str, str],
    retries: int,
    retry_interval: float,
    payload: Any,
) -> Any:
    response = None
    last_error = None

    for attempts_left in range(max(0, retries), -1, -1):
        try:
            response = client.post(url=url, headers=headers, json=payload)
            break

        except Exception as e:
            last_error = e
            if attempts_left > 0:
                time.sleep(retry_interval)

    if response is None:
        assert last_error is not None
        raise last_error

    body = response.json()
    if "error" in body:
        raise UpstashError(body["error"])

    return body["result"]


async def execute_with_parameters_async(
    client: AsyncClient,
    url: str,
    headers: Dict[str, str],
    retries: int,
    retry_interval: float,
    payload: Any,
) -> Any:
    response = None
    last_error = None

    for attempts_left in range(max(0, retries), -1, -1):
        try:
            response = await client.post(url=url, headers=headers, json=payload)
            break

        except Exception as e:
            last_error = e
            if attempts_left > 0:
                await asyncio.sleep(retry_interval)

    if response is None:
        assert last_error is not None
        raise last_error

    body = response.json()
    if "error" in body:
        raise UpstashError(body["error"])

    return body["result"]
