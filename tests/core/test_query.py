import pytest
from tests import assert_eventually, assert_eventually_async

from upstash_vector import Index, AsyncIndex


def test_query_with_vectors_with_metadata(index: Index):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "id2"
    v2_values = [0.3, 0.4]

    v3_id = "id3"
    v3_values = [0.5, 0.6]

    v4_id = "id4"
    v4_values = [0.7, 0.8]
    v4_metadata = {"metadata_field_4": "metadata_value_4"}

    v5_id = "id5"
    v5_values = [124, 0.8]
    v5_metadata = {"metadata_field_5": "metadata_value_5"}

    index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
            (v3_id, v3_values),
            (v4_id, v4_values, v4_metadata),
            (v5_id, v5_values, v5_metadata),
        ]
    )

    def assertion():
        query_res = index.query(
            v1_values, top_k=1, include_metadata=True, include_vectors=True
        )
        assert len(query_res) == 1

        assert query_res[0].id == v1_id
        assert query_res[0].metadata == v1_metadata
        assert query_res[0].score == 1
        assert query_res[0].vector == v1_values

        query_res = index.query(
            v1_values, top_k=2, include_metadata=True, include_vectors=True
        )
        assert len(query_res) == 2

        assert query_res[1].id == v2_id
        assert query_res[1].metadata is None
        assert query_res[1].score < 1
        assert query_res[1].vector == v2_values

        query_res = index.query(
            v1_values, top_k=5, include_metadata=True, include_vectors=True
        )
        assert len(query_res) == 5

    assert_eventually(assertion)


def test_query_with_vectors_without_metadata(index: Index):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "id2"
    v2_values = [0.3, 0.4]

    v3_id = "id3"
    v3_values = [0.5, 0.6]

    v4_id = "id4"
    v4_values = [0.7, 0.8]
    v4_metadata = {"metadata_field_4": "metadata_value_4"}

    v5_id = "id5"
    v5_values = [124, 0.8]
    v5_metadata = {"metadata_field_5": "metadata_value_5"}

    index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
            (v3_id, v3_values),
            (v4_id, v4_values, v4_metadata),
            (v5_id, v5_values, v5_metadata),
        ]
    )

    def assertion():
        query_res = index.query(
            v1_values, top_k=1, include_metadata=False, include_vectors=True
        )
        assert len(query_res) == 1

        assert query_res[0].id == v1_id
        assert query_res[0].metadata is None
        assert query_res[0].score == 1
        assert query_res[0].vector == v1_values

        query_res = index.query(
            v1_values, top_k=2, include_metadata=False, include_vectors=True
        )
        assert len(query_res) == 2

        assert query_res[1].id == v2_id
        assert query_res[1].metadata is None
        assert query_res[1].score < 1
        assert query_res[1].vector == v2_values

        query_res = index.query(
            v1_values, top_k=5, include_metadata=False, include_vectors=True
        )
        assert len(query_res) == 5

    assert_eventually(assertion)


def test_query_without_vectors_without_metadata(index: Index):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "id2"
    v2_values = [0.3, 0.4]

    v3_id = "id3"
    v3_values = [0.5, 0.6]

    v4_id = "id4"
    v4_values = [0.7, 0.8]
    v4_metadata = {"metadata_field_4": "metadata_value_4"}

    v5_id = "id5"
    v5_values = [124, 0.8]
    v5_metadata = {"metadata_field_5": "metadata_value_5"}

    index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
            (v3_id, v3_values),
            (v4_id, v4_values, v4_metadata),
            (v5_id, v5_values, v5_metadata),
        ]
    )

    def assertion():
        query_res = index.query(
            v1_values, top_k=1, include_metadata=False, include_vectors=False
        )
        assert len(query_res) == 1

        assert query_res[0].id == v1_id
        assert query_res[0].metadata is None
        assert query_res[0].score == 1
        assert query_res[0].vector is None

        query_res = index.query(
            v1_values, top_k=2, include_metadata=False, include_vectors=False
        )
        assert len(query_res) == 2

        assert query_res[1].id == v2_id
        assert query_res[1].metadata is None
        assert query_res[1].score < 1
        assert query_res[1].vector is None

        query_res = index.query(
            v1_values, top_k=5, include_metadata=False, include_vectors=False
        )
        assert len(query_res) == 5

    assert_eventually(assertion)


@pytest.mark.asyncio
async def test_query_with_vectors_with_metadata_async(async_index: AsyncIndex):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "id2"
    v2_values = [0.3, 0.4]

    v3_id = "id3"
    v3_values = [0.5, 0.6]

    v4_id = "id4"
    v4_values = [0.7, 0.8]
    v4_metadata = {"metadata_field_4": "metadata_value_4"}

    v5_id = "id5"
    v5_values = [124, 0.8]
    v5_metadata = {"metadata_field_5": "metadata_value_5"}

    await async_index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
            (v3_id, v3_values),
            (v4_id, v4_values, v4_metadata),
            (v5_id, v5_values, v5_metadata),
        ]
    )

    async def assertion():
        query_res = await async_index.query(
            v1_values, top_k=1, include_metadata=True, include_vectors=True
        )
        assert len(query_res) == 1

        assert query_res[0].id == v1_id
        assert query_res[0].metadata == v1_metadata
        assert query_res[0].score == 1
        assert query_res[0].vector == v1_values

        query_res = await async_index.query(
            v1_values, top_k=2, include_metadata=True, include_vectors=True
        )
        assert len(query_res) == 2

        assert query_res[1].id == v2_id
        assert query_res[1].metadata is None
        assert query_res[1].score < 1
        assert query_res[1].vector == v2_values

        query_res = await async_index.query(
            v1_values, top_k=5, include_metadata=True, include_vectors=True
        )
        assert len(query_res) == 5

    await assert_eventually_async(assertion)


@pytest.mark.asyncio
async def test_query_with_vectors_without_metadata_async(async_index: AsyncIndex):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "id2"
    v2_values = [0.3, 0.4]

    v3_id = "id3"
    v3_values = [0.5, 0.6]

    v4_id = "id4"
    v4_values = [0.7, 0.8]
    v4_metadata = {"metadata_field_4": "metadata_value_4"}

    v5_id = "id5"
    v5_values = [124, 0.8]
    v5_metadata = {"metadata_field_5": "metadata_value_5"}

    await async_index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
            (v3_id, v3_values),
            (v4_id, v4_values, v4_metadata),
            (v5_id, v5_values, v5_metadata),
        ]
    )

    async def assertion():
        query_res = await async_index.query(
            v1_values, top_k=1, include_metadata=False, include_vectors=True
        )
        assert len(query_res) == 1

        assert query_res[0].id == v1_id
        assert query_res[0].metadata is None
        assert query_res[0].score == 1
        assert query_res[0].vector == v1_values

        query_res = await async_index.query(
            v1_values, top_k=2, include_metadata=False, include_vectors=True
        )
        assert len(query_res) == 2

        assert query_res[1].id == v2_id
        assert query_res[1].metadata is None
        assert query_res[1].score < 1
        assert query_res[1].vector == v2_values

        query_res = await async_index.query(
            v1_values, top_k=5, include_metadata=False, include_vectors=True
        )
        assert len(query_res) == 5

    await assert_eventually_async(assertion)


@pytest.mark.asyncio
async def test_query_without_vectors_without_metadata_async(async_index: AsyncIndex):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "id2"
    v2_values = [0.3, 0.4]

    v3_id = "id3"
    v3_values = [0.5, 0.6]

    v4_id = "id4"
    v4_values = [0.7, 0.8]
    v4_metadata = {"metadata_field_4": "metadata_value_4"}

    v5_id = "id5"
    v5_values = [124, 0.8]
    v5_metadata = {"metadata_field_5": "metadata_value_5"}

    await async_index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
            (v3_id, v3_values),
            (v4_id, v4_values, v4_metadata),
            (v5_id, v5_values, v5_metadata),
        ]
    )

    async def assertion():
        query_res = await async_index.query(
            v1_values, top_k=1, include_metadata=False, include_vectors=False
        )
        assert len(query_res) == 1

        assert query_res[0].id == v1_id
        assert query_res[0].metadata is None
        assert query_res[0].score == 1
        assert query_res[0].vector is None

        query_res = await async_index.query(
            v1_values, top_k=2, include_metadata=False, include_vectors=False
        )
        assert len(query_res) == 2

        assert query_res[1].id == v2_id
        assert query_res[1].metadata is None
        assert query_res[1].score < 1
        assert query_res[1].vector is None

        query_res = await async_index.query(
            v1_values, top_k=5, include_metadata=False, include_vectors=False
        )
        assert len(query_res) == 5

    await assert_eventually_async(assertion)
