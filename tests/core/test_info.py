import pytest

from tests import (
    NAMESPACES,
    assert_eventually,
    assert_eventually_async,
    ensure_ns_exists,
    ensure_ns_exists_async,
)
from upstash_vector import AsyncIndex, Index


def test_info(index: Index):
    for ns in NAMESPACES:
        ensure_ns_exists(index, ns)

    info = index.info()

    assert info.vector_count == 0
    assert info.pending_vector_count == 0
    assert info.dimension == 2
    assert info.similarity_function == "COSINE"
    assert len(info.namespaces) == len(NAMESPACES)
    for ns in NAMESPACES:
        assert ns in info.namespaces
        assert info.namespaces[ns].vector_count == 0
        assert info.namespaces[ns].pending_vector_count == 0

    for ns in NAMESPACES:
        index.upsert([{"id": "foo", "vector": [0, 1]}], namespace=ns)

    def assertion():
        i = index.info()
        assert i.vector_count == len(NAMESPACES)

        for ns in NAMESPACES:
            assert i.namespaces[ns].vector_count == 1

    assert_eventually(assertion)


def test_info_dense_index(index: Index):
    info = index.info()

    assert info.dense_index is not None
    assert info.dense_index.dimension == 2
    assert info.dense_index.similarity_function == "COSINE"
    assert info.dense_index.embedding_model == ""


def test_info_sparse_index(sparse_index: Index):
    info = sparse_index.info()

    assert info.sparse_index is not None
    assert info.sparse_index.embedding_model == ""


def test_info_hybrid_index(hybrid_index: Index):
    info = hybrid_index.info()

    assert info.dense_index is not None
    assert info.dense_index.dimension == 2
    assert info.dense_index.similarity_function == "COSINE"
    assert info.dense_index.embedding_model == ""

    assert info.sparse_index is not None
    assert info.sparse_index.embedding_model == ""


@pytest.mark.asyncio
async def test_info_async(async_index: AsyncIndex):
    for ns in NAMESPACES:
        await ensure_ns_exists_async(async_index, ns)

    info = await async_index.info()

    assert info.vector_count == 0
    assert info.pending_vector_count == 0
    assert info.dimension == 2
    assert info.similarity_function == "COSINE"
    assert len(info.namespaces) == len(NAMESPACES)
    for ns in NAMESPACES:
        assert ns in info.namespaces
        assert info.namespaces[ns].vector_count == 0
        assert info.namespaces[ns].pending_vector_count == 0

    for ns in NAMESPACES:
        await async_index.upsert([{"id": "foo", "vector": [0, 1]}], namespace=ns)

    async def assertion():
        i = await async_index.info()
        assert i.vector_count == len(NAMESPACES)

        for ns in NAMESPACES:
            assert i.namespaces[ns].vector_count == 1

    await assert_eventually_async(assertion)


@pytest.mark.asyncio
async def test_info_dense_index_async(async_index: AsyncIndex):
    info = await async_index.info()

    assert info.dense_index is not None
    assert info.dense_index.dimension == 2
    assert info.dense_index.similarity_function == "COSINE"
    assert info.dense_index.embedding_model == ""


@pytest.mark.asyncio
async def test_info_sparse_index_async(async_sparse_index: AsyncIndex):
    info = await async_sparse_index.info()

    assert info.sparse_index is not None
    assert info.sparse_index.embedding_model == ""


@pytest.mark.asyncio
async def test_info_hybrid_index_async(async_hybrid_index: AsyncIndex):
    info = await async_hybrid_index.info()

    assert info.dense_index is not None
    assert info.dense_index.dimension == 2
    assert info.dense_index.similarity_function == "COSINE"
    assert info.dense_index.embedding_model == ""

    assert info.sparse_index is not None
    assert info.sparse_index.embedding_model == ""
