import pytest

from tests import NAMESPACES
from upstash_vector import AsyncIndex, Index
from upstash_vector.types import SparseVector


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


@pytest.mark.parametrize("ns", NAMESPACES)
def test_fetch_with_data(index: Index, ns: str):
    v1_id = "v1-id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_data = "data1"
    v1_values = [0.1, 0.2]

    v2_id = "v1-id2"
    v2_values = [0.3, 0.4]

    v3_id = "v1-id3"
    v3_values = [0.5, 0.6]
    v3_data = "data3"

    index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata, v1_data),
            (v2_id, v2_values),
            (v3_id, v3_values, None, v3_data),
        ],
        namespace=ns,
    )

    res = index.fetch(
        ids=[v1_id, v2_id, v3_id],
        include_vectors=True,
        include_metadata=True,
        include_data=True,
        namespace=ns,
    )

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values
    assert res[0].data == v1_data

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values
    assert res[1].data is None

    assert res[2] is not None
    assert res[2].id == v3_id
    assert res[2].metadata is None
    assert res[2].vector == v3_values
    assert res[2].data == v3_data


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


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_fetch_with_data_async(async_index: AsyncIndex, ns: str):
    v1_id = "v1-id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_data = "data1"
    v1_values = [0.1, 0.2]

    v2_id = "v1-id2"
    v2_values = [0.3, 0.4]

    v3_id = "v1-id3"
    v3_values = [0.5, 0.6]
    v3_data = "data3"

    await async_index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata, v1_data),
            (v2_id, v2_values),
            (v3_id, v3_values, None, v3_data),
        ],
        namespace=ns,
    )

    res = await async_index.fetch(
        ids=[v1_id, v2_id, v3_id],
        include_vectors=True,
        include_metadata=True,
        include_data=True,
        namespace=ns,
    )

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values
    assert res[0].data == v1_data

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values
    assert res[1].data is None

    assert res[2] is not None
    assert res[2].id == v3_id
    assert res[2].metadata is None
    assert res[2].vector == v3_values
    assert res[2].data == v3_data


@pytest.mark.parametrize("ns", NAMESPACES)
def test_fetch_sparse(sparse_index: Index, ns: str):
    sparse_index.upsert(
        vectors=[
            ("id0", ([0, 1], [0.1, 0.2])),
            ("id1", ([1, 2], [0.2, 0.3]), {"key": "value"}),
            ("id2", ([2, 3], [0.3, 0.4]), {"key": "value"}, "data"),
        ],
        namespace=ns,
    )

    res = sparse_index.fetch(
        ids=["id0", "id1", "id2", "id3"],
        include_vectors=True,
        include_metadata=True,
        include_data=True,
        namespace=ns,
    )

    assert len(res) == 4

    assert res[0] is not None
    assert res[0].id == "id0"
    assert res[0].metadata is None
    assert res[0].sparse_vector == SparseVector([0, 1], [0.1, 0.2])

    assert res[1] is not None
    assert res[1].id == "id1"
    assert res[1].metadata == {"key": "value"}
    assert res[1].sparse_vector == SparseVector([1, 2], [0.2, 0.3])

    assert res[2] is not None
    assert res[2].id == "id2"
    assert res[2].metadata == {"key": "value"}
    assert res[2].data == "data"
    assert res[2].sparse_vector == SparseVector([2, 3], [0.3, 0.4])

    assert res[3] is None


@pytest.mark.parametrize("ns", NAMESPACES)
def test_fetch_hybrid_index(hybrid_index: Index, ns: str):
    hybrid_index.upsert(
        vectors=[
            ("id0", [0.1, 0.2], ([0, 1], [0.1, 0.2])),
            ("id1", [0.2, 0.3], ([1, 2], [0.2, 0.3]), {"key": "value"}),
            ("id2", [0.3, 0.4], ([2, 3], [0.3, 0.4]), {"key": "value"}, "data"),
        ],
        namespace=ns,
    )

    res = hybrid_index.fetch(
        ids=["id0", "id1", "id2", "id3"],
        include_vectors=True,
        include_metadata=True,
        include_data=True,
        namespace=ns,
    )

    assert len(res) == 4

    assert res[0] is not None
    assert res[0].id == "id0"
    assert res[0].metadata is None
    assert res[0].vector == [0.1, 0.2]
    assert res[0].sparse_vector == SparseVector([0, 1], [0.1, 0.2])

    assert res[1] is not None
    assert res[1].id == "id1"
    assert res[1].metadata == {"key": "value"}
    assert res[1].vector == [0.2, 0.3]
    assert res[1].sparse_vector == SparseVector([1, 2], [0.2, 0.3])

    assert res[2] is not None
    assert res[2].id == "id2"
    assert res[2].metadata == {"key": "value"}
    assert res[2].data == "data"
    assert res[2].vector == [0.3, 0.4]
    assert res[2].sparse_vector == SparseVector([2, 3], [0.3, 0.4])

    assert res[3] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_fetch_sparse_async(async_sparse_index: AsyncIndex, ns: str):
    await async_sparse_index.upsert(
        vectors=[
            ("id0", ([0, 1], [0.1, 0.2])),
            ("id1", ([1, 2], [0.2, 0.3]), {"key": "value"}),
            ("id2", ([2, 3], [0.3, 0.4]), {"key": "value"}, "data"),
        ],
        namespace=ns,
    )

    res = await async_sparse_index.fetch(
        ids=["id0", "id1", "id2", "id3"],
        include_vectors=True,
        include_metadata=True,
        include_data=True,
        namespace=ns,
    )

    assert len(res) == 4

    assert res[0] is not None
    assert res[0].id == "id0"
    assert res[0].metadata is None
    assert res[0].sparse_vector == SparseVector([0, 1], [0.1, 0.2])

    assert res[1] is not None
    assert res[1].id == "id1"
    assert res[1].metadata == {"key": "value"}
    assert res[1].sparse_vector == SparseVector([1, 2], [0.2, 0.3])

    assert res[2] is not None
    assert res[2].id == "id2"
    assert res[2].metadata == {"key": "value"}
    assert res[2].data == "data"
    assert res[2].sparse_vector == SparseVector([2, 3], [0.3, 0.4])

    assert res[3] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_fetch_hybrid_async(async_hybrid_index: AsyncIndex, ns: str):
    await async_hybrid_index.upsert(
        vectors=[
            ("id0", [0.1, 0.2], ([0, 1], [0.1, 0.2])),
            ("id1", [0.2, 0.3], ([1, 2], [0.2, 0.3]), {"key": "value"}),
            ("id2", [0.3, 0.4], ([2, 3], [0.3, 0.4]), {"key": "value"}, "data"),
        ],
        namespace=ns,
    )

    res = await async_hybrid_index.fetch(
        ids=["id0", "id1", "id2", "id3"],
        include_vectors=True,
        include_metadata=True,
        include_data=True,
        namespace=ns,
    )

    assert len(res) == 4

    assert res[0] is not None
    assert res[0].id == "id0"
    assert res[0].metadata is None
    assert res[0].vector == [0.1, 0.2]
    assert res[0].sparse_vector == SparseVector([0, 1], [0.1, 0.2])

    assert res[1] is not None
    assert res[1].id == "id1"
    assert res[1].metadata == {"key": "value"}
    assert res[1].vector == [0.2, 0.3]
    assert res[1].sparse_vector == SparseVector([1, 2], [0.2, 0.3])

    assert res[2] is not None
    assert res[2].id == "id2"
    assert res[2].metadata == {"key": "value"}
    assert res[2].data == "data"
    assert res[2].vector == [0.3, 0.4]
    assert res[2].sparse_vector == SparseVector([2, 3], [0.3, 0.4])

    assert res[3] is None
