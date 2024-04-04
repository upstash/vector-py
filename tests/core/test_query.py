import pytest
from tests import assert_eventually, assert_eventually_async

from upstash_vector import Index, AsyncIndex

import numpy as np
import pandas as pd


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


def test_query_with_numpy_and_pandas_vectors(index: Index):
    index.upsert(
        vectors=[
            ("id-0", [0.1, 0.2]),
            ("id-1", [0.7, 0.8]),
        ]
    )

    def assertion():
        query_res = index.query(np.array([0.1, 0.2]), top_k=1)
        assert len(query_res) == 1

        assert query_res[0].id == "id-0"
        assert query_res[0].score == 1

        query_res = index.query(pd.array([0.7, 0.8]), top_k=1)
        assert len(query_res) == 1

        assert query_res[0].id == "id-1"
        assert query_res[0].score == 1

    assert_eventually(assertion)


def test_query_with_filtering(index: Index):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value", "foo": "bar"}
    v1_values = [0.1, 0.2]

    v2_id = "id2"
    v2_metadata = {"metadata_field": "metadata_value", "foo": "nay"}
    v2_values = [0.1, 0.2]

    v3_id = "id3"
    v3_metadata = {"metadata_field": "metadata_value", "foo": "bar"}
    v3_values = [0.2, 0.3]

    index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values, v2_metadata),
            (v3_id, v3_values, v3_metadata),
        ]
    )

    def assertion():
        query_res = index.query(
            v1_values, top_k=2, include_metadata=True, include_vectors=True
        )
        assert len(query_res) == 2

        assert query_res[0].id == v1_id
        assert query_res[0].metadata == v1_metadata
        assert query_res[0].score == 1
        assert query_res[0].vector == v1_values

        assert query_res[1].id == v2_id
        assert query_res[1].metadata == v2_metadata
        assert query_res[1].score == 1
        assert query_res[1].vector == v2_values

        query_res = index.query(
            v1_values,
            top_k=2,
            include_metadata=True,
            include_vectors=True,
            filter="foo = 'bar'",
        )
        assert len(query_res) == 2

        assert query_res[0].id == v1_id
        assert query_res[0].score == 1
        assert query_res[0].metadata == v1_metadata
        assert query_res[0].vector == v1_values

        assert query_res[1].id == v3_id
        assert query_res[1].score < 1
        assert query_res[1].metadata == v3_metadata
        assert query_res[1].vector == v3_values

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


@pytest.mark.asyncio
async def test_query_with_numpy_and_pandas_vectors_async(async_index: AsyncIndex):
    await async_index.upsert(
        vectors=[
            ("id-0", [0.1, 0.2]),
            ("id-1", [0.7, 0.8]),
        ]
    )

    async def assertion():
        query_res = await async_index.query(np.array([0.1, 0.2]), top_k=1)
        assert len(query_res) == 1

        assert query_res[0].id == "id-0"
        assert query_res[0].score == 1

        query_res = await async_index.query(pd.array([0.7, 0.8]), top_k=1)
        assert len(query_res) == 1

        assert query_res[0].id == "id-1"
        assert query_res[0].score == 1

    await assert_eventually_async(assertion)


@pytest.mark.asyncio
async def test_query_with_filtering_async(async_index: AsyncIndex):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value", "foo": "bar"}
    v1_values = [0.1, 0.2]

    v2_id = "id2"
    v2_metadata = {"metadata_field": "metadata_value", "foo": "nay"}
    v2_values = [0.1, 0.2]

    v3_id = "id3"
    v3_metadata = {"metadata_field": "metadata_value", "foo": "bar"}
    v3_values = [0.2, 0.3]

    await async_index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values, v2_metadata),
            (v3_id, v3_values, v3_metadata),
        ]
    )

    async def assertion():
        query_res = await async_index.query(
            v1_values, top_k=2, include_metadata=True, include_vectors=True
        )
        assert len(query_res) == 2

        assert query_res[0].id == v1_id
        assert query_res[0].metadata == v1_metadata
        assert query_res[0].score == 1
        assert query_res[0].vector == v1_values

        assert query_res[1].id == v2_id
        assert query_res[1].metadata == v2_metadata
        assert query_res[1].score == 1
        assert query_res[1].vector == v2_values

        query_res = await async_index.query(
            v1_values,
            top_k=2,
            include_metadata=True,
            include_vectors=True,
            filter="foo = 'bar'",
        )
        assert len(query_res) == 2

        assert query_res[0].id == v1_id
        assert query_res[0].score == 1
        assert query_res[0].metadata == v1_metadata
        assert query_res[0].vector == v1_values

        assert query_res[1].id == v3_id
        assert query_res[1].score < 1
        assert query_res[1].metadata == v3_metadata
        assert query_res[1].vector == v3_values

    await assert_eventually_async(assertion)


def test_query_with_data(index: Index):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_data = "Hello-world"

    index.upsert(
        vectors=[
            (v1_id, v1_data, v1_metadata),
        ]
    )

    def assertion():
        query_res = index.query(
            data=v1_data, top_k=1, include_metadata=True, include_vectors=True
        )
        assert len(query_res) == 1

        assert query_res[0].id == v1_id
        assert query_res[0].metadata == v1_metadata
        assert query_res[0].score == 1

    assert_eventually(assertion)


@pytest.mark.asyncio
async def test_query_with_data_async(async_index: AsyncIndex):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_data = "Hello-world"

    await async_index.upsert(
        vectors=[
            (v1_id, v1_data, v1_metadata),
        ]
    )

    async def assertion():
        query_res = await async_index.query(
            data=v1_data, top_k=1, include_metadata=True, include_vectors=True
        )
        assert len(query_res) == 1

        assert query_res[0].id == v1_id
        assert query_res[0].metadata == v1_metadata
        assert query_res[0].score == 1

    await assert_eventually_async(assertion)


def test_query_with_multiple_data(index: Index):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "cookie"}
    v1_data = "cookie"

    v2_id = "id2"
    v2_metadata = {"metadata_field": "car"}
    v2_data = "car"

    index.upsert(
        vectors=[
            (v1_id, v1_data, v1_metadata),
            (v2_id, v2_data, v2_metadata),
        ]
    )

    def assertion():
        query_res = index.query(
            data=v1_data, top_k=1, include_metadata=True, include_vectors=True
        )
        assert len(query_res) == 1

        assert query_res[0].id == v1_id
        assert query_res[0].metadata == v1_metadata
        assert query_res[0].score == 1

        query_res = index.query(
            data=v2_data, top_k=1, include_metadata=True, include_vectors=True
        )
        assert len(query_res) == 1

        assert query_res[0].id == v2_id
        assert query_res[0].metadata == v2_metadata
        assert query_res[0].score == 1

        query_res = index.query(
            data=v1_data, top_k=2, include_metadata=False, include_vectors=True
        )
        assert len(query_res) == 2

    assert_eventually(assertion)


@pytest.mark.asyncio
async def test_query_with_multiple_data_async(async_index: AsyncIndex):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "cookie"}
    v1_data = "cookie"

    v2_id = "id2"
    v2_metadata = {"metadata_field": "car"}
    v2_data = "car"

    await async_index.upsert(
        vectors=[
            (v1_id, v1_data, v1_metadata),
            (v2_id, v2_data, v2_metadata),
        ]
    )

    async def assertion():
        query_res = await async_index.query(
            data=v1_data, top_k=1, include_metadata=True, include_vectors=True
        )
        assert len(query_res) == 1

        assert query_res[0].id == v1_id
        assert query_res[0].metadata == v1_metadata
        assert query_res[0].score == 1

        query_res = await async_index.query(
            data=v2_data, top_k=1, include_metadata=True, include_vectors=True
        )
        assert len(query_res) == 1

        assert query_res[0].id == v2_id
        assert query_res[0].metadata == v2_metadata
        assert query_res[0].score == 1

        query_res = await async_index.query(
            data=v1_data, top_k=2, include_metadata=False, include_vectors=True
        )
        assert len(query_res) == 2

    await assert_eventually_async(assertion)
