import pytest
from pytest import raises
import random

from upstash_vector import Index, AsyncIndex
from upstash_vector.errors import ClientError


def test_range(index: Index):
    vectors = [
        {"id": f"id-{i}", "vector": [random.random() for _ in range(2)]}
        for i in range(20)
    ]

    index.upsert(vectors=vectors)

    res = index.range(cursor="", limit=4, include_vectors=True)
    assert len(res.vectors) == 4
    assert res.next_cursor != ""

    while res.next_cursor != "":
        res = index.range(cursor=res.next_cursor, limit=8, include_vectors=True)
        assert len(res.vectors) == 8

    with raises(ClientError):
        index.range(cursor="0", limit=0, include_vectors=True)


@pytest.mark.asyncio
async def test_range_async(async_index: AsyncIndex):
    vectors = [
        {"id": f"id-{i}", "vector": [random.random() for _ in range(2)]}
        for i in range(20)
    ]

    await async_index.upsert(vectors=vectors)

    res = await async_index.range(cursor="", limit=4, include_vectors=True)
    assert len(res.vectors) == 4
    assert res.next_cursor != ""

    while res.next_cursor != "":
        res = await async_index.range(
            cursor=res.next_cursor, limit=8, include_vectors=True
        )
        assert len(res.vectors) == 8

    with raises(ClientError):
        await async_index.range(cursor="0", limit=0, include_vectors=True)
