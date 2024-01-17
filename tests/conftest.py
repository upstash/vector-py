import pytest
from os import environ
from upstash_vector import Index


@pytest.fixture
def index():
    idx = Index(environ["URL"], environ["TOKEN"])
    idx.reset()
    return idx
