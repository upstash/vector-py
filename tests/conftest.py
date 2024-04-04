import pytest
import pytest_asyncio

from os import environ

from upstash_vector import Index, AsyncIndex


@pytest.fixture
def index():
    idx = Index(environ["URL"], environ["TOKEN"])
    idx.reset()
    return idx


@pytest_asyncio.fixture
async def async_index():
    idx = AsyncIndex(environ["URL"], environ["TOKEN"])
    await idx.reset()
    return idx


@pytest.fixture
def embedding_index():
    idx = Index(environ["EMBEDDING_URL"], environ["EMBEDDING_TOKEN"])
    idx.reset()
    return idx


@pytest_asyncio.fixture
async def async_embedding_index():
    idx = AsyncIndex(environ["EMBEDDING_URL"], environ["EMBEDDING_TOKEN"])
    await idx.reset()
    return idx

