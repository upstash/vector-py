import pytest

from tests import NAMESPACES
from upstash_vector import Index, AsyncIndex


@pytest.mark.parametrize("ns", NAMESPACES)
def test_fetch_with_vectors_with_metadata(index: Index, ns: str):
    v1_id = "v1-id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "v1-id2"
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

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values


@pytest.mark.parametrize("ns", NAMESPACES)
def test_fetch_with_vectors_without_metadata(index: Index, ns: str):
    v1_id = "v2-id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "v2-id2"
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
        include_metadata=False,
        namespace=ns,
    )

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata is None
    assert res[0].vector == v1_values

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values


@pytest.mark.parametrize("ns", NAMESPACES)
def test_fetch_without_vectors_without_metadata(index: Index, ns: str):
    v1_id = "v3-id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "v3-id2"
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
        include_vectors=False,
        include_metadata=False,
        namespace=ns,
    )

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata is None
    assert res[0].vector is None

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector is None


@pytest.mark.parametrize("ns", NAMESPACES)
def test_fetch_single(index: Index, ns: str):
    v1_id = "v4-id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "v4-id2"
    v2_values = [0.3, 0.4]

    index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
        ],
        namespace=ns,
    )

    res = index.fetch(
        ids=v1_id, include_vectors=True, include_metadata=True, namespace=ns
    )

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_fetch_with_vectors_with_metadata_async(async_index: AsyncIndex, ns: str):
    v1_id = "v1-id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "v1-id2"
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

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_fetch_with_vectors_without_metadata_async(
    async_index: AsyncIndex, ns: str
):
    v1_id = "v2-id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "v2-id2"
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
        include_metadata=False,
        namespace=ns,
    )

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata is None
    assert res[0].vector == v1_values

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_fetch_without_vectors_without_metadata_async(
    async_index: AsyncIndex, ns: str
):
    v1_id = "v3-id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "v3-id2"
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
        include_vectors=False,
        include_metadata=False,
        namespace=ns,
    )

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata is None
    assert res[0].vector is None

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector is None


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_fetch_single_async(async_index: AsyncIndex, ns: str):
    v1_id = "v4-id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "v4-id2"
    v2_values = [0.3, 0.4]

    await async_index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
        ],
        namespace=ns,
    )

    res = await async_index.fetch(
        ids=v1_id,
        include_vectors=True,
        include_metadata=True,
        namespace=ns,
    )

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values
