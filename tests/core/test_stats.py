from typing import Awaitable
import pytest

from tests import assert_eventually, assert_eventually_async
from upstash_vector import Index, AsyncIndex


def test_stats(index: Index):
    stats = index.stats()

    assert stats.vector_count == 0
    assert stats.pending_vector_count == 0

    index.upsert([{"id": "foo", "vector": [0, 1]}])

    def assertion():
        assert index.stats().vector_count == 1

    assert_eventually(assertion)


@pytest.mark.asyncio
async def test_stats_async(async_index_needs_await: Awaitable[AsyncIndex]):
    async_index = await async_index_needs_await
    stats = await async_index.stats()

    assert stats.vector_count == 0
    assert stats.pending_vector_count == 0

    await async_index.upsert([{"id": "foo", "vector": [0, 1]}])

    async def assertion():
        assert (await async_index.stats()).vector_count == 1

    await assert_eventually_async(assertion)
