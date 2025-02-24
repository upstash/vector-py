import pytest

from tests import NAMESPACES
from upstash_vector import AsyncIndex, Index


@pytest.mark.parametrize("ns", NAMESPACES)
def test_delete(index: Index, ns: str):
    v1_id = "delete-id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "delete-id2"
    v2_values = [0.3, 0.4]

    v3_id = "delete-id3"
    v3_values = [0.5, 0.6]

    index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
            (v3_id, v3_values),
        ],
        namespace=ns,
    )

    res = index.fetch(
        ids=[v1_id, v2_id, v3_id],
        include_vectors=True,
        include_metadata=True,
        namespace=ns,
    )
    assert len(res) == 3
    assert res[0] is not None
    assert res[1] is not None
    assert res[2] is not None

    del_res = index.delete(ids=v1_id, namespace=ns)
    assert del_res.deleted == 1

    res = index.fetch(
        ids=[v1_id, v2_id],
        include_vectors=True,
        include_metadata=True,
        namespace=ns,
    )
    assert res[0] is None
    assert res[1] is not None

    index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
            (v3_id, v3_values),
        ],
        namespace=ns,
    )

    del_res = index.delete(ids=[v1_id, v2_id], namespace=ns)
    assert del_res.deleted == 2

    res = index.fetch(
        ids=[v1_id, v2_id, v3_id],
        include_vectors=True,
        include_metadata=True,
        namespace=ns,
    )
    assert res[0] is None
    assert res[1] is None
    assert res[2] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_delete_async(async_index: AsyncIndex, ns: str):
    v1_id = "delete-id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "delete-id2"
    v2_values = [0.3, 0.4]

    v3_id = "delete-id3"
    v3_values = [0.5, 0.6]

    await async_index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
            (v3_id, v3_values),
        ],
        namespace=ns,
    )

    res = await async_index.fetch(
        ids=[v1_id, v2_id, v3_id],
        include_vectors=True,
        include_metadata=True,
        namespace=ns,
    )
    assert len(res) == 3
    assert res[0] is not None
    assert res[1] is not None
    assert res[2] is not None

    del_res = await async_index.delete(ids=v1_id, namespace=ns)
    assert del_res.deleted == 1

    res = await async_index.fetch(
        ids=[v1_id, v2_id],
        include_vectors=True,
        include_metadata=True,
        namespace=ns,
    )
    assert res[0] is None
    assert res[1] is not None

    await async_index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
            (v3_id, v3_values),
        ],
        namespace=ns,
    )

    del_res = await async_index.delete(ids=[v1_id, v2_id], namespace=ns)
    assert del_res.deleted == 2

    res = await async_index.fetch(
        ids=[v1_id, v2_id, v3_id],
        include_vectors=True,
        include_metadata=True,
        namespace=ns,
    )
    assert res[0] is None
    assert res[1] is None
    assert res[2] is not None


@pytest.mark.parametrize("ns", NAMESPACES)
def test_delete_prefix(index: Index, ns: str):
    index.upsert(
        vectors=[
            ("id-00", [0.1, 0.2]),
            ("id-01", [0.1, 0.3]),
            ("id-10", [0.1, 0.4]),
            ("id-11", [0.1, 0.5]),
        ],
        namespace=ns,
    )

    result = index.delete(
        prefix="id-0",
        namespace=ns,
    )

    assert result.deleted == 2

    vectors = index.fetch(
        ids=["id-00", "id-01", "id-10", "id-11"],
        namespace=ns,
    )
    assert len(vectors) == 4
    assert vectors[0] is None
    assert vectors[1] is None
    assert vectors[2] is not None
    assert vectors[3] is not None


@pytest.mark.parametrize("ns", NAMESPACES)
def test_delete_filter(index: Index, ns: str):
    index.upsert(
        vectors=[
            ("id-00", [0.1, 0.2], {"meta": 0}),
            ("id-01", [0.1, 0.3], {"meta": 1}),
            ("id-10", [0.1, 0.4], {"meta": 2}),
            ("id-11", [0.1, 0.5], {"meta": 3}),
        ],
        namespace=ns,
    )

    result = index.delete(
        filter="meta >= 2",
        namespace=ns,
    )

    assert result.deleted == 2

    vectors = index.fetch(
        ids=["id-00", "id-01", "id-10", "id-11"],
        namespace=ns,
    )
    assert len(vectors) == 4
    assert vectors[0] is not None
    assert vectors[1] is not None
    assert vectors[2] is None
    assert vectors[3] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_delete_prefix_async(async_index: AsyncIndex, ns: str):
    await async_index.upsert(
        vectors=[
            ("id-00", [0.1, 0.2]),
            ("id-01", [0.1, 0.3]),
            ("id-10", [0.1, 0.4]),
            ("id-11", [0.1, 0.5]),
        ],
        namespace=ns,
    )

    result = await async_index.delete(
        prefix="id-0",
        namespace=ns,
    )

    assert result.deleted == 2

    vectors = await async_index.fetch(
        ids=["id-00", "id-01", "id-10", "id-11"],
        namespace=ns,
    )
    assert len(vectors) == 4
    assert vectors[0] is None
    assert vectors[1] is None
    assert vectors[2] is not None
    assert vectors[3] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_delete_filter_async(async_index: AsyncIndex, ns: str):
    await async_index.upsert(
        vectors=[
            ("id-00", [0.1, 0.2], {"meta": 0}),
            ("id-01", [0.1, 0.3], {"meta": 1}),
            ("id-10", [0.1, 0.4], {"meta": 2}),
            ("id-11", [0.1, 0.5], {"meta": 3}),
        ],
        namespace=ns,
    )

    result = await async_index.delete(
        filter="meta >= 2",
        namespace=ns,
    )

    assert result.deleted == 2

    vectors = await async_index.fetch(
        ids=["id-00", "id-01", "id-10", "id-11"],
        namespace=ns,
    )
    assert len(vectors) == 4
    assert vectors[0] is not None
    assert vectors[1] is not None
    assert vectors[2] is None
    assert vectors[3] is None
