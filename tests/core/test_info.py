import pytest

from tests import (
    assert_eventually,
    assert_eventually_async,
    NAMESPACES,
    ensure_ns_exists,
    ensure_ns_exists_async,
)
from upstash_vector import Index, AsyncIndex


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
