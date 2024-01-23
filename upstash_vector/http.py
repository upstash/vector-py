import os
from typing import Any, Dict
from httpx import Client, AsyncClient
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
    client: Client,
    headers: Dict[str, str],
    payload: Any,
) -> Any:
    response = client.post(url=url, headers=headers, json=payload).json()

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
