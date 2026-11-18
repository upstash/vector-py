import httpx
import pytest

from upstash_vector import AsyncIndex, Index
from upstash_vector.core.index_operations import _path_for
from upstash_vector.errors import ClientError

BASE = "https://vector.example.com"


def test_default_namespace():
    assert _path_for("", "/query") == "/query"


@pytest.mark.parametrize("namespace", ["my-namespace_1", "tenant.a", "a..b", ".hidden"])
def test_ordinary_namespace_is_sent_as_before(namespace):
    assert _path_for(namespace, "/query") == f"/query/{namespace}"


@pytest.mark.parametrize(
    "namespace,encoded",
    [
        ("a?b", "a%3Fb"),
        ("a#b", "a%23b"),
        ("a b", "a%20b"),
        ("user:1@example.com", "user%3A1%40example.com"),
        ("a+b=c", "a%2Bb%3Dc"),
        ("..?all", "..%3Fall"),
        (".. ", "..%20"),
    ],
)
def test_namespace_stays_a_single_path_segment(namespace, encoded):
    assert _path_for(namespace, "/query") == f"/query/{encoded}"


@pytest.mark.parametrize(
    "namespace", ["a/b", "a\\b", "../reset?all", "100%", "%2e%2e", "a%2Fb"]
)
def test_namespace_with_slash_or_percent_is_not_accepted(namespace):
    with pytest.raises(ClientError, match="Namespace cannot contain"):
        _path_for(namespace, "/query")


@pytest.mark.parametrize("namespace", [".", ".."])
def test_dot_namespace_is_not_accepted(namespace):
    with pytest.raises(ClientError, match='Namespace cannot be "." or ".."'):
        _path_for(namespace, "/query")


def test_requested_urls():
    urls = []

    def handler(request: httpx.Request) -> httpx.Response:
        urls.append(str(request.url))
        return httpx.Response(200, json={"result": []})

    index = Index(url=BASE, token="token", retries=0, allow_telemetry=False)
    index._client = httpx.Client(transport=httpx.MockTransport(handler))

    index.fetch(["1"], namespace="user:1")
    index.fetch(["1"], namespace="tenant.a")
    index.reset(all=True)
    assert urls == [
        f"{BASE}/fetch/user%3A1",
        f"{BASE}/fetch/tenant.a",
        f"{BASE}/reset?all",
    ]

    with pytest.raises(ClientError):
        index.fetch(["1"], namespace="../reset?all")
    with pytest.raises(ClientError):
        index.delete_namespace("..")
    assert len(urls) == 3


@pytest.mark.asyncio
async def test_requested_urls_async():
    urls = []

    def handler(request: httpx.Request) -> httpx.Response:
        urls.append(str(request.url))
        return httpx.Response(200, json={"result": []})

    index = AsyncIndex(url=BASE, token="token", retries=0, allow_telemetry=False)
    index._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    await index.fetch(["1"], namespace="user:1")
    assert urls == [f"{BASE}/fetch/user%3A1"]

    with pytest.raises(ClientError):
        await index.fetch(["1"], namespace="../reset?all")
    with pytest.raises(ClientError):
        await index.delete_namespace("..")
    assert len(urls) == 1
