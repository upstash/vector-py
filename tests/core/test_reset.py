import pytest
from typing import Awaitable
from upstash_vector import Index, AsyncIndex


def test_reset(index: Index):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "id2"
    v2_values = [0.3, 0.4]

    index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
        ]
    )

    res = index.fetch(ids=[v1_id, v2_id], include_vectors=True, include_metadata=True)
    assert len(res) == 2
    assert res[0] is not None
    assert res[1] is not None

    index.reset()

    res = index.fetch(ids=[v1_id, v2_id], include_vectors=True, include_metadata=True)
    assert res[0] is None
    assert res[1] is None


@pytest.mark.asyncio
async def test_reset_async(async_index: AsyncIndex):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "id2"
    v2_values = [0.3, 0.4]

    await async_index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
        ]
    )

    res = await async_index.fetch(
        ids=[v1_id, v2_id], include_vectors=True, include_metadata=True
    )
    assert len(res) == 2
    assert res[0] is not None
    assert res[1] is not None

    await async_index.reset()

    res = await async_index.fetch(
        ids=[v1_id, v2_id], include_vectors=True, include_metadata=True
    )
    assert res[0] is None
    assert res[1] is None
