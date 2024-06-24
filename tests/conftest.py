from os import environ

import pytest
import pytest_asyncio

from tests import NAMESPACES
from upstash_vector import Index, AsyncIndex


@pytest.fixture
def index():
    idx = Index(environ["URL"], environ["TOKEN"])
    for ns in NAMESPACES:
        idx.reset(namespace=ns)
    return idx


@pytest_asyncio.fixture
async def async_index():
    idx = AsyncIndex(environ["URL"], environ["TOKEN"])
    for ns in NAMESPACES:
        await idx.reset(namespace=ns)
    return idx


@pytest.fixture
def embedding_index():
    idx = Index(environ["EMBEDDING_URL"], environ["EMBEDDING_TOKEN"])
    for ns in NAMESPACES:
        idx.reset(namespace=ns)
    return idx


@pytest_asyncio.fixture
async def async_embedding_index():
    idx = AsyncIndex(environ["EMBEDDING_URL"], environ["EMBEDDING_TOKEN"])
    for ns in NAMESPACES:
        await idx.reset(namespace=ns)
    return idx
