import pytest

from tests import assert_eventually, assert_eventually_async
from upstash_vector import Index, AsyncIndex


def test_info(index: Index):
    info = index.info()

    assert info.vector_count == 0
    assert info.pending_vector_count == 0
    assert info.dimension == 2
    assert info.similarity_function == "COSINE"

    index.upsert([{"id": "foo", "vector": [0, 1]}])

    def assertion():
        assert index.info().vector_count == 1

    assert_eventually(assertion)


@pytest.mark.asyncio
async def test_info_async(async_index: AsyncIndex):
    info = await async_index.info()

    assert info.vector_count == 0
    assert info.pending_vector_count == 0
    assert info.dimension == 2
    assert info.similarity_function == "COSINE"

    await async_index.upsert([{"id": "foo", "vector": [0, 1]}])

    async def assertion():
        assert (await async_index.info()).vector_count == 1

    await assert_eventually_async(assertion)
