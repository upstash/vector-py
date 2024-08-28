import pytest
from upstash_vector import Index, AsyncIndex
from tests import assert_eventually, assert_eventually_async, NAMESPACES


@pytest.mark.parametrize("ns", NAMESPACES)
def test_resumable_query(index: Index, ns: str):
    # Prepare test data
    vectors = [
        ("id1", [0.1, 0.2], {"metadata_field": "value1"}),
        ("id2", [0.3, 0.4], {"metadata_field": "value2"}),
        ("id3", [0.5, 0.6], {"metadata_field": "value3"}),
        ("id4", [0.7, 0.8], {"metadata_field": "value4"}),
        ("id5", [0.9, 1.0], {"metadata_field": "value5"}),
    ]
    index.upsert(vectors=vectors, namespace=ns)

    def assertion():
        # Create a resumable query
        query = index.resumable_query(
            vector=[0.1, 0.2],
            top_k=5,
            include_metadata=True,
            include_vectors=True,
            namespace=ns,
            max_idle=3600,
        )

        # Start the query
        start_result = query.start()
        assert "uuid" in start_result
        assert len(start_result["scores"]) == 5

        # Fetch results in batches
        batch1 = query.fetch_next(2)
        assert len(batch1) == 2
        assert batch1[0].id == "id1"
        assert batch1[0].score == 1.0
        assert batch1[0].metadata == {"metadata_field": "value1"}
        assert batch1[0].vector == [0.1, 0.2]

        batch2 = query.fetch_next(2)
        assert len(batch2) == 2
        assert batch2[0].id != batch1[0].id
        assert batch2[0].id != batch1[1].id

        # Stop the query
        stop_result = query.stop()
        assert stop_result == "Success"

        # Trying to fetch after stopping should raise an error
        with pytest.raises(Exception):
            query.fetch_next(1)

    assert_eventually(assertion)


# @pytest.mark.asyncio
# @pytest.mark.parametrize("ns", NAMESPACES)
# async def test_resumable_query_async(async_index: AsyncIndex, ns: str):
#     # Prepare test data
#     vectors = [
#         ("id1", [0.1, 0.2], {"metadata_field": "value1"}),
#         ("id2", [0.3, 0.4], {"metadata_field": "value2"}),
#         ("id3", [0.5, 0.6], {"metadata_field": "value3"}),
#         ("id4", [0.7, 0.8], {"metadata_field": "value4"}),
#         ("id5", [0.9, 1.0], {"metadata_field": "value5"}),
#     ]
#     await async_index.upsert(vectors=vectors, namespace=ns)

#     async def assertion():
#         # Create a resumable query
#         query = await async_index.resumable_query(
#             vector=[0.1, 0.2],
#             top_k=5,
#             include_metadata=True,
#             include_vectors=True,
#             namespace=ns,
#             max_idle=3600,
#         )

#         # Start the query
#         start_result = await query.start()
#         assert "uuid" in start_result
#         assert len(start_result["scores"]) == 5

#         # Fetch results in batches
#         batch1 = await query.fetch_next(2)
#         assert len(batch1) == 2
#         assert batch1[0].id == "id1"
#         assert batch1[0].score == 1.0
#         assert batch1[0].metadata == {"metadata_field": "value1"}
#         assert batch1[0].vector == [0.1, 0.2]

#         batch2 = await query.fetch_next(2)
#         assert len(batch2) == 2
#         assert batch2[0].id != batch1[0].id
#         assert batch2[0].id != batch1[1].id

#         # Stop the query
#         stop_result = await query.stop()
#         assert stop_result == "Success"

#         # Trying to fetch after stopping should raise an error
#         with pytest.raises(Exception):
#             await query.fetch_next(1)

#     await assert_eventually_async(assertion)


# @pytest.mark.parametrize("ns", NAMESPACES)
# def test_resumable_query_with_filter(index: Index, ns: str):
#     # Prepare test data
#     vectors = [
#         ("id1", [0.1, 0.2], {"category": "A"}),
#         ("id2", [0.3, 0.4], {"category": "B"}),
#         ("id3", [0.5, 0.6], {"category": "A"}),
#         ("id4", [0.7, 0.8], {"category": "B"}),
#         ("id5", [0.9, 1.0], {"category": "A"}),
#     ]
#     index.upsert(vectors=vectors, namespace=ns)

#     def assertion():
#         query = index.resumable_query(
#             vector=[0.1, 0.2],
#             top_k=5,
#             include_metadata=True,
#             filter="category = 'A'",
#             namespace=ns,
#         )

#         start_result = query.start()
#         assert len(start_result["scores"]) == 3

#         results = query.fetch_next(3)
#         assert len(results) == 3
#         assert all(result.metadata["category"] == "A" for result in results)

#         query.stop()

#     assert_eventually(assertion)


# @pytest.mark.asyncio
# @pytest.mark.parametrize("ns", NAMESPACES)
# async def test_resumable_query_with_data(async_embedding_index: AsyncIndex, ns: str):
#     # Prepare test data
#     vectors = [
#         ("id1", "Hello world", {"category": "greeting"}),
#         ("id2", "Goodbye world", {"category": "farewell"}),
#         ("id3", "Hello there", {"category": "greeting"}),
#         ("id4", "See you later", {"category": "farewell"}),
#         ("id5", "Hi everyone", {"category": "greeting"}),
#     ]
#     await async_embedding_index.upsert(vectors=vectors, namespace=ns)

#     async def assertion():
#         query = await async_embedding_index.resumable_query(
#             data="Hello",
#             top_k=5,
#             include_metadata=True,
#             include_data=True,
#             namespace=ns,
#         )

#         start_result = await query.start()
#         assert len(start_result["scores"]) == 5

#         results = await query.fetch_next(3)
#         assert len(results) == 3
#         assert all(result.data is not None for result in results)
#         assert results[0].data in ["Hello world", "Hello there"]

#         await query.stop()

#     await assert_eventually_async(assertion)


# @pytest.mark.parametrize("ns", NAMESPACES)
# def test_resumable_query_error_handling(index: Index, ns: str):
#     # Prepare test data
#     vectors = [("id1", [0.1, 0.2])]
#     index.upsert(vectors=vectors, namespace=ns)

#     def assertion():
#         # Test invalid top_k
#         with pytest.raises(ValueError):
#             index.resumable_query(vector=[0.1, 0.2], top_k=0, namespace=ns)

#         # Test missing vector and data
#         with pytest.raises(ValueError):
#             index.resumable_query(top_k=1, namespace=ns)

#         # Test both vector and data provided
#         with pytest.raises(ValueError):
#             index.resumable_query(vector=[0.1, 0.2], data="test", top_k=1, namespace=ns)

#         # Test fetching before starting
#         query = index.resumable_query(vector=[0.1, 0.2], top_k=1, namespace=ns)
#         with pytest.raises(Exception):
#             query.fetch_next(1)

#         # Test stopping before starting
#         with pytest.raises(Exception):
#             query.stop()

#     assert_eventually(assertion)
