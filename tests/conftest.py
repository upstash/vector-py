import pytest
import pytest_asyncio

from tests import (
    EMBEDDING_INDEX_TOKEN,
    EMBEDDING_INDEX_URL,
    HYBRID_EMBEDDING_INDEX_TOKEN,
    HYBRID_EMBEDDING_INDEX_URL,
    HYBRID_INDEX_TOKEN,
    HYBRID_INDEX_URL,
    INDEX_TOKEN,
    INDEX_URL,
    NAMESPACES,
    SPARSE_INDEX_TOKEN,
    SPARSE_INDEX_URL,
)
from upstash_vector import AsyncIndex, Index


@pytest.fixture
def index():
    idx = Index(INDEX_URL, INDEX_TOKEN)
    for ns in NAMESPACES:
        idx.reset(namespace=ns)
    return idx


@pytest_asyncio.fixture
async def async_index():
    idx = AsyncIndex(INDEX_URL, INDEX_TOKEN)
    for ns in NAMESPACES:
        await idx.reset(namespace=ns)
    return idx


@pytest.fixture
def sparse_index():
    idx = Index(SPARSE_INDEX_URL, SPARSE_INDEX_TOKEN)
    for ns in NAMESPACES:
        idx.reset(namespace=ns)
    return idx


@pytest_asyncio.fixture
async def async_sparse_index():
    idx = AsyncIndex(SPARSE_INDEX_URL, SPARSE_INDEX_TOKEN)
    for ns in NAMESPACES:
        await idx.reset(namespace=ns)
    return idx


@pytest.fixture
def hybrid_index():
    idx = Index(HYBRID_INDEX_URL, HYBRID_INDEX_TOKEN)
    for ns in NAMESPACES:
        idx.reset(namespace=ns)
    return idx


@pytest_asyncio.fixture
async def async_hybrid_index():
    idx = AsyncIndex(HYBRID_INDEX_URL, HYBRID_INDEX_TOKEN)
    for ns in NAMESPACES:
        await idx.reset(namespace=ns)
    return idx


@pytest.fixture
def embedding_index():
    idx = Index(EMBEDDING_INDEX_URL, EMBEDDING_INDEX_TOKEN)
    for ns in NAMESPACES:
        idx.reset(namespace=ns)
    return idx


@pytest_asyncio.fixture
async def async_embedding_index():
    idx = AsyncIndex(EMBEDDING_INDEX_URL, EMBEDDING_INDEX_TOKEN)
    for ns in NAMESPACES:
        await idx.reset(namespace=ns)
    return idx


@pytest.fixture
def hybrid_embedding_index():
    idx = Index(HYBRID_EMBEDDING_INDEX_URL, HYBRID_EMBEDDING_INDEX_TOKEN)
    for ns in NAMESPACES:
        idx.reset(namespace=ns)
    return idx


@pytest_asyncio.fixture
async def async_hybrid_embedding_index():
    idx = AsyncIndex(HYBRID_EMBEDDING_INDEX_URL, HYBRID_EMBEDDING_INDEX_TOKEN)
    for ns in NAMESPACES:
        await idx.reset(namespace=ns)
    return idx
