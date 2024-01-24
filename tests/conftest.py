import pytest
import pytest_asyncio

from os import environ

from upstash_vector import Index, AsyncIndex

pytest_plugins = ("pytest_asyncio",)


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
