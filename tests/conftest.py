import pytest
from os import environ
from upstash_vector import Index, AsyncIndex


@pytest.fixture
def index():
    idx = Index(environ["URL"], environ["TOKEN"])
    idx.reset()
    return idx


@pytest.fixture
async def async_index():
    idx = AsyncIndex(environ["URL"], environ["TOKEN"])
    await idx.reset()
    return idx
