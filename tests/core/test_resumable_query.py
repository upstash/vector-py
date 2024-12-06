import pytest

from tests import NAMESPACES, assert_eventually, assert_eventually_async
from upstash_vector import AsyncIndex, Index
from upstash_vector.errors import UpstashError
from upstash_vector.types import FusionAlgorithm, SparseVector, WeightingStrategy


@pytest.mark.parametrize("ns", NAMESPACES)
def test_resumable_query(index: Index, ns: str):
    index.upsert(
        vectors=[
            ("id1", [0.1, 0.2], {"field": "value1"}),
            ("id2", [0.3, 0.4], {"field": "value2"}),
            ("id3", [0.5, 0.6], {"field": "value3"}),
        ],
        namespace=ns,
    )

    def assertion():
        result, handle = index.resumable_query(
            vector=[0.1, 0.2],
            top_k=2,
            include_metadata=True,
            include_vectors=True,
            namespace=ns,
        )

        with handle:
            assert isinstance(result, list)

            assert len(result) > 0
            assert result[0].metadata is not None
            assert result[0].vector is not None

        with pytest.raises(UpstashError):
            handle.fetch_next(1)

        with pytest.raises(UpstashError):
            handle.stop()

    assert_eventually(assertion)


@pytest.mark.parametrize("ns", NAMESPACES)
def test_resumable_query_with_data(embedding_index: Index, ns: str):
    embedding_index.upsert(
        vectors=[
            ("id1", "Hello world", {"field": "value1"}),
            ("id2", "Vector databases", {"field": "value2"}),
        ],
        namespace=ns,
    )

    def assertion():
        result, handle = embedding_index.resumable_query(
            data="Hello",
            top_k=1,
            include_metadata=True,
            namespace=ns,
        )

        with handle:
            assert len(result) == 1
            assert result[0].id == "id1"

    assert_eventually(assertion)


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_resumable_query_async(async_index: AsyncIndex, ns: str):
    await async_index.upsert(
        vectors=[
            ("id1", [0.1, 0.2], {"field": "value1"}),
            ("id2", [0.3, 0.4], {"field": "value2"}),
            ("id3", [0.5, 0.6], {"field": "value3"}),
        ],
        namespace=ns,
    )

    async def assertion():
        result, handle = await async_index.resumable_query(
            vector=[0.1, 0.2],
            top_k=2,
            include_metadata=True,
            include_vectors=True,
            namespace=ns,
        )

        async with handle:
            assert isinstance(result, list)
            assert len(result) > 0
            assert result[0].metadata is not None
            assert result[0].vector is not None

            next_results = await handle.fetch_next(1)
            assert isinstance(next_results, list)
            assert len(next_results) == 1

        with pytest.raises(UpstashError):
            await handle.fetch_next(1)

        with pytest.raises(UpstashError):
            await handle.stop()

    await assert_eventually_async(assertion)


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_resumable_query_with_data_async(
    async_embedding_index: AsyncIndex, ns: str
):
    await async_embedding_index.upsert(
        vectors=[
            ("id1", "Hello world", {"field": "value1"}),
            ("id2", "Vector databases", {"field": "value2"}),
        ],
        namespace=ns,
    )

    async def assertion():
        result, handle = await async_embedding_index.resumable_query(
            data="Hello",
            top_k=1,
            include_metadata=True,
            namespace=ns,
        )

        async with handle:
            assert len(result) == 1
            assert result[0].id == "id1"

    await assert_eventually_async(assertion)


@pytest.mark.parametrize("ns", NAMESPACES)
def test_resumable_query_fetch_next(index: Index, ns: str):
    index.upsert(
        vectors=[
            ("id1", [0.1, 0.2], {"field": "value1"}),
            ("id2", [0.3, 0.4], {"field": "value2"}),
            ("id3", [0.5, 0.6], {"field": "value3"}),
            ("id4", [0.7, 0.8], {"field": "value4"}),
            ("id5", [0.9, 1.0], {"field": "value5"}),
        ],
        namespace=ns,
    )

    def assertion():
        result, handle = index.resumable_query(
            vector=[0.1, 0.2],
            top_k=2,
            include_metadata=True,
            namespace=ns,
        )

        with handle:
            assert len(result) == 2
            assert result[0].id == "id1"
            assert result[1].id == "id2"

            # Fetch next 2 results
            next_results_1 = handle.fetch_next(2)
            assert len(next_results_1) == 2
            assert next_results_1[0].id == "id3"
            assert next_results_1[1].id == "id4"

            # Fetch next 1 result
            next_results_2 = handle.fetch_next(1)
            assert len(next_results_2) == 1
            assert next_results_2[0].id == "id5"

            # Try to fetch more, should return empty list
            next_results_3 = handle.fetch_next(1)
            assert len(next_results_3) == 0

    assert_eventually(assertion)


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_resumable_query_multiple_fetch_async(async_index: AsyncIndex, ns: str):
    await async_index.upsert(
        vectors=[
            ("id1", [0.1, 0.2], {"field": "value1"}),
            ("id2", [0.3, 0.4], {"field": "value2"}),
            ("id3", [0.5, 0.6], {"field": "value3"}),
            ("id4", [0.7, 0.8], {"field": "value4"}),
            ("id5", [0.9, 1.0], {"field": "value5"}),
        ],
        namespace=ns,
    )

    async def assertion():
        result, handle = await async_index.resumable_query(
            vector=[0.1, 0.2],
            top_k=2,
            include_metadata=True,
            namespace=ns,
        )

        async with handle:
            assert len(result) == 2

            next_results_1 = await handle.fetch_next(2)
            assert len(next_results_1) == 2

            next_results_2 = await handle.fetch_next(1)
            assert len(next_results_2) == 1

    await assert_eventually_async(assertion)


@pytest.mark.parametrize("ns", NAMESPACES)
def test_resumable_query_sparse(sparse_index: Index, ns: str):
    sparse_index.upsert(
        vectors=[
            ("id0", ([0, 1], [0.3, 0.1])),
            ("id1", ([0, 2], [0.2, 0.1]), {"key": "value"}),
            ("id2", ([0, 3], [0.1, 0.1]), {"key": "value"}, "data"),
        ],
        namespace=ns,
    )

    def assertion():
        result, handle = sparse_index.resumable_query(
            sparse_vector=([0], [0.1]),
            top_k=2,
            include_vectors=True,
            include_metadata=True,
            include_data=True,
            namespace=ns,
        )

        with handle:
            assert len(result) == 2
            assert result[0].id == "id0"
            assert result[0].sparse_vector == SparseVector([0, 1], [0.3, 0.1])
            assert result[1].id == "id1"
            assert result[1].metadata == {"key": "value"}
            assert result[1].sparse_vector == SparseVector([0, 2], [0.2, 0.1])

            next_result = handle.fetch_next(1)
            assert len(next_result) == 1
            assert next_result[0].id == "id2"
            assert next_result[0].metadata == {"key": "value"}
            assert next_result[0].data == "data"
            assert next_result[0].sparse_vector == SparseVector([0, 3], [0.1, 0.1])

    assert_eventually(assertion)


@pytest.mark.parametrize("ns", NAMESPACES)
def test_resumable_query_hybrid(hybrid_index: Index, ns: str):
    hybrid_index.upsert(
        vectors=[
            ("id0", [0.9, 0.9], ([0, 1], [0.3, 0.1])),
            ("id1", [0.8, 0.9], ([0, 2], [0.2, 0.1]), {"key": "value"}),
            ("id2", [0.7, 0.9], ([0, 3], [0.1, 0.1]), {"key": "value"}, "data"),
        ],
        namespace=ns,
    )

    def assertion():
        result, handle = hybrid_index.resumable_query(
            vector=[0.1, 0.1],
            sparse_vector=([0], [0.1]),
            top_k=2,
            include_vectors=True,
            include_metadata=True,
            include_data=True,
            weighting_strategy=WeightingStrategy.IDF,
            fusion_algorithm=FusionAlgorithm.DBSF,
            namespace=ns,
        )

        with handle:
            assert len(result) == 2
            assert result[0].id == "id0"
            assert result[0].vector == [0.9, 0.9]
            assert result[0].sparse_vector == SparseVector([0, 1], [0.3, 0.1])
            assert result[1].id == "id1"
            assert result[1].metadata == {"key": "value"}
            assert result[1].vector == [0.8, 0.9]
            assert result[1].sparse_vector == SparseVector([0, 2], [0.2, 0.1])

            next_result = handle.fetch_next(1)
            assert len(next_result) == 1
            assert next_result[0].id == "id2"
            assert next_result[0].metadata == {"key": "value"}
            assert next_result[0].data == "data"
            assert next_result[0].vector == [0.7, 0.9]
            assert next_result[0].sparse_vector == SparseVector([0, 3], [0.1, 0.1])

    assert_eventually(assertion)


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_resumable_query_sparse_async(async_sparse_index: AsyncIndex, ns: str):
    await async_sparse_index.upsert(
        vectors=[
            ("id0", ([0, 1], [0.3, 0.1])),
            ("id1", ([0, 2], [0.2, 0.1]), {"key": "value"}),
            ("id2", ([0, 3], [0.1, 0.1]), {"key": "value"}, "data"),
        ],
        namespace=ns,
    )

    async def assertion():
        result, handle = await async_sparse_index.resumable_query(
            sparse_vector=([0], [0.1]),
            top_k=2,
            include_vectors=True,
            include_metadata=True,
            include_data=True,
            namespace=ns,
        )

        async with handle:
            assert len(result) == 2
            assert result[0].id == "id0"
            assert result[0].sparse_vector == SparseVector([0, 1], [0.3, 0.1])
            assert result[1].id == "id1"
            assert result[1].metadata == {"key": "value"}
            assert result[1].sparse_vector == SparseVector([0, 2], [0.2, 0.1])

            next_result = await handle.fetch_next(1)
            assert len(next_result) == 1
            assert next_result[0].id == "id2"
            assert next_result[0].metadata == {"key": "value"}
            assert next_result[0].data == "data"
            assert next_result[0].sparse_vector == SparseVector([0, 3], [0.1, 0.1])

    await assert_eventually_async(assertion)


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_resumable_query_hybrid_async(async_hybrid_index: AsyncIndex, ns: str):
    await async_hybrid_index.upsert(
        vectors=[
            ("id0", [0.9, 0.9], ([0, 1], [0.3, 0.1])),
            ("id1", [0.8, 0.9], ([0, 2], [0.2, 0.1]), {"key": "value"}),
            ("id2", [0.7, 0.9], ([0, 3], [0.1, 0.1]), {"key": "value"}, "data"),
        ],
        namespace=ns,
    )

    async def assertion():
        result, handle = await async_hybrid_index.resumable_query(
            vector=[0.1, 0.1],
            sparse_vector=([0], [0.1]),
            top_k=2,
            include_vectors=True,
            include_metadata=True,
            include_data=True,
            weighting_strategy=WeightingStrategy.IDF,
            fusion_algorithm=FusionAlgorithm.DBSF,
            namespace=ns,
        )

        async with handle:
            assert len(result) == 2
            assert result[0].id == "id0"
            assert result[0].vector == [0.9, 0.9]
            assert result[0].sparse_vector == SparseVector([0, 1], [0.3, 0.1])
            assert result[1].id == "id1"
            assert result[1].metadata == {"key": "value"}
            assert result[1].vector == [0.8, 0.9]
            assert result[1].sparse_vector == SparseVector([0, 2], [0.2, 0.1])

            next_result = await handle.fetch_next(1)
            assert len(next_result) == 1
            assert next_result[0].id == "id2"
            assert next_result[0].metadata == {"key": "value"}
            assert next_result[0].data == "data"
            assert next_result[0].vector == [0.7, 0.9]
            assert next_result[0].sparse_vector == SparseVector([0, 3], [0.1, 0.1])

    await assert_eventually_async(assertion)
