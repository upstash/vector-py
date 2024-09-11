import pytest
from upstash_vector import Index, AsyncIndex
from upstash_vector.errors import ClientError
from upstash_vector.types import QueryResult
from tests import assert_eventually_async, assert_eventually, NAMESPACES
import time


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

    time.sleep(1)

    query, initial_results = index.resumable_query(
        vector=[0.1, 0.2],
        top_k=2,
        include_metadata=True,
        include_vectors=True,
        namespace=ns,
    )

    assert isinstance(initial_results, list)
    assert len(initial_results) > 0
    assert isinstance(initial_results[0], QueryResult)
    assert hasattr(initial_results[0], "id")
    assert hasattr(initial_results[0], "metadata")

    stop_result = query.stop()
    assert stop_result == "Success"

    with pytest.raises(ClientError):
        query.fetch_next(1)

    with pytest.raises(ClientError):
        query.stop()


@pytest.mark.parametrize("ns", NAMESPACES)
def test_resumable_query_with_data(embedding_index: Index, ns: str):
    embedding_index.upsert(
        vectors=[
            ("id1", "Hello world", {"field": "value1"}),
            ("id2", "Vector databases", {"field": "value2"}),
        ],
        namespace=ns,
    )

    time.sleep(1)

    query, results = embedding_index.resumable_query(
        data="Hello",
        top_k=1,
        include_metadata=True,
        namespace=ns,
    )

    assert len(results) == 1
    assert isinstance(results[0], QueryResult)
    assert results[0].id == "id1"

    stop_result = query.stop()
    assert stop_result == "Success"


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
        query, initial_results = await async_index.resumable_query(
            vector=[0.1, 0.2],
            top_k=2,
            include_metadata=True,
            include_vectors=True,
            namespace=ns,
        )

        assert isinstance(initial_results, list)
        assert len(initial_results) > 0
        assert isinstance(initial_results[0], QueryResult)
        assert hasattr(initial_results[0], "id")
        assert hasattr(initial_results[0], "metadata")

        next_results = await query.async_fetch_next(1)
        assert isinstance(next_results, list)
        assert len(next_results) == 1

        stop_result = await query.async_stop()
        assert stop_result == "Success"

        with pytest.raises(ClientError):
            await query.async_fetch_next(1)

        with pytest.raises(ClientError):
            await query.async_stop()

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
        query, results = await async_embedding_index.resumable_query(
            data="Hello",
            top_k=1,
            include_metadata=True,
            namespace=ns,
        )

        assert len(results) == 1
        assert isinstance(results[0], QueryResult)
        assert results[0].id == "id1"

        stop_result = await query.async_stop()
        assert stop_result == "Success"

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
        query, initial_results = index.resumable_query(
            vector=[0.1, 0.2],
            top_k=2,
            include_metadata=True,
            namespace=ns,
        )

        assert len(initial_results) == 2
        assert initial_results[0].id == "id1"
        assert initial_results[1].id == "id2"

        # Fetch next 2 results
        next_results_1 = query.fetch_next(2)
        assert len(next_results_1) == 2
        assert next_results_1[0].id == "id3"
        assert next_results_1[1].id == "id4"

        # Fetch next 1 result
        next_results_2 = query.fetch_next(1)
        assert len(next_results_2) == 1
        assert next_results_2[0].id == "id5"

        # Try to fetch more, should return empty list
        next_results_3 = query.fetch_next(1)
        assert len(next_results_3) == 0

        query.stop()

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
        query, initial_results = await async_index.resumable_query(
            vector=[0.1, 0.2],
            top_k=2,
            include_metadata=True,
            namespace=ns,
        )

        assert len(initial_results) == 2

        next_results_1 = await query.async_fetch_next(2)
        assert len(next_results_1) == 2

        next_results_2 = await query.async_fetch_next(1)
        assert len(next_results_2) == 1

        stop_result = await query.async_stop()
        assert stop_result == "Success"

    await assert_eventually_async(assertion)


@pytest.mark.parametrize("ns", NAMESPACES)
def test_resumable_query_context_manager(index: Index, ns: str):
    index.upsert(
        vectors=[
            ("id1", [0.1, 0.2], {"field": "value1"}),
            ("id2", [0.3, 0.4], {"field": "value2"}),
            ("id3", [0.5, 0.6], {"field": "value3"}),
        ],
        namespace=ns,
    )

    time.sleep(1)

    def assertion():
        query, initial_results = index.resumable_query(
            vector=[0.1, 0.2],
            top_k=2,
            include_metadata=True,
            include_vectors=True,
            namespace=ns,
        )

        assert isinstance(initial_results, list)
        assert len(initial_results) > 0
        assert isinstance(initial_results[0], QueryResult)
        assert hasattr(initial_results[0], "id")
        assert hasattr(initial_results[0], "metadata")

        next_results = query.fetch_next(1)
        assert isinstance(next_results, list)
        assert len(next_results) == 1

        query.stop()

        # The query should be stopped
        with pytest.raises(ClientError):
            query.fetch_next(1)

    assert_eventually(assertion)


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_resumable_query_context_manager_async(async_index: AsyncIndex, ns: str):
    await async_index.upsert(
        vectors=[
            ("id1", [0.1, 0.2], {"field": "value1"}),
            ("id2", [0.3, 0.4], {"field": "value2"}),
            ("id3", [0.5, 0.6], {"field": "value3"}),
        ],
        namespace=ns,
    )

    async def assertion():
        query, initial_results = await async_index.resumable_query(
            vector=[0.1, 0.2],
            top_k=2,
            include_metadata=True,
            include_vectors=True,
            namespace=ns,
        )

        assert isinstance(initial_results, list)
        assert len(initial_results) > 0
        assert isinstance(initial_results[0], QueryResult)
        assert hasattr(initial_results[0], "id")
        assert hasattr(initial_results[0], "metadata")

        next_results = await query.async_fetch_next(1)
        assert isinstance(next_results, list)
        assert len(next_results) == 1

        await query.async_stop()

        # The query should be stopped
        with pytest.raises(ClientError):
            await query.async_fetch_next(1)

    await assert_eventually_async(assertion)


# @pytest.mark.parametrize("ns", NAMESPACES)
# def test_resumable_query_context_manager(index: Index, ns: str):
#     index.upsert(
#         vectors=[
#             ("id1", [0.1, 0.2], {"field": "value1"}),
#             ("id2", [0.3, 0.4], {"field": "value2"}),
#             ("id3", [0.5, 0.6], {"field": "value3"}),
#         ],
#         namespace=ns,
#     )

#     time.sleep(1)

#     def assertion():
#         with index.resumable_query(
#             vector=[0.1, 0.2],
#             top_k=2,
#             include_metadata=True,
#             include_vectors=True,
#             namespace=ns,
#         ) as query:
#             initial_results = query._start()
#             assert isinstance(initial_results, list)
#             assert len(initial_results) > 0
#             assert isinstance(initial_results[0], QueryResult)
#             assert hasattr(initial_results[0], "id")
#             assert hasattr(initial_results[0], "metadata")

#             next_results = query.fetch_next(1)
#             assert isinstance(next_results, list)
#             assert len(next_results) == 1

#         # The query should be automatically stopped when exiting the context
#         with pytest.raises(ClientError):
#             query.fetch_next(1)

#     assert_eventually(assertion)
