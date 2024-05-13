import pytest
from tests import NAMESPACES
from upstash_vector import Index, AsyncIndex


@pytest.mark.parametrize("ns", NAMESPACES)
def test_reset(index: Index, ns: str):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "id2"
    v2_values = [0.3, 0.4]

    index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
        ],
        namespace=ns,
    )

    res = index.fetch(
        ids=[v1_id, v2_id],
        include_vectors=True,
        include_metadata=True,
        namespace=ns,
    )
    assert len(res) == 2
    assert res[0] is not None
    assert res[1] is not None

    index.reset(namespace=ns)

    res = index.fetch(
        ids=[v1_id, v2_id],
        include_vectors=True,
        include_metadata=True,
        namespace=ns,
    )
    assert res[0] is None
    assert res[1] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_reset_async(async_index: AsyncIndex, ns: str):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "id2"
    v2_values = [0.3, 0.4]

    await async_index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
        ],
        namespace=ns,
    )

    res = await async_index.fetch(
        ids=[v1_id, v2_id],
        include_vectors=True,
        include_metadata=True,
        namespace=ns,
    )
    assert len(res) == 2
    assert res[0] is not None
    assert res[1] is not None

    await async_index.reset(namespace=ns)

    res = await async_index.fetch(
        ids=[v1_id, v2_id],
        include_vectors=True,
        include_metadata=True,
        namespace=ns,
    )
    assert res[0] is None
    assert res[1] is None


def test_reset_all(index: Index):
    for ns in NAMESPACES:
        index.upsert([("id-0", [0.1, 0.2])], namespace=ns)

    for ns in NAMESPACES:
        res = index.fetch("id-0", namespace=ns)
        assert len(res) == 1
        assert res[0] is not None

    index.reset(all=True)

    for ns in NAMESPACES:
        res = index.fetch("id-0", namespace=ns)
        assert len(res) == 1
        assert res[0] is None


@pytest.mark.asyncio
async def test_reset_all_async(async_index: AsyncIndex):
    for ns in NAMESPACES:
        await async_index.upsert([("id-0", [0.1, 0.2])], namespace=ns)

    for ns in NAMESPACES:
        res = await async_index.fetch("id-0", namespace=ns)
        assert len(res) == 1
        assert res[0] is not None

    await async_index.reset(all=True)

    for ns in NAMESPACES:
        res = await async_index.fetch("id-0", namespace=ns)
        assert len(res) == 1
        assert res[0] is None
