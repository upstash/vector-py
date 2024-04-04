import pytest
from pytest import raises

from upstash_vector import Index, AsyncIndex
from upstash_vector.errors import UpstashError, ClientError
from upstash_vector.types import Data, Vector

import numpy as np
import pandas as pd


def test_upsert_tuple(index: Index):
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

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values


@pytest.mark.asyncio
async def test_upsert_tuple_async(async_index: AsyncIndex):
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

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values


def test_upsert_dict(index: Index):
    v1_id = "dict_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "dict_id2"
    v2_values = [0.3, 0.4]

    index.upsert(
        vectors=[
            {"id": v1_id, "vector": v1_values, "metadata": v1_metadata},
            {"id": v2_id, "vector": v2_values},
        ]
    )

    res = index.fetch(ids=[v1_id, v2_id], include_vectors=True, include_metadata=True)

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values


@pytest.mark.asyncio
async def test_upsert_dict_async(async_index: AsyncIndex):
    v1_id = "dict_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "dict_id2"
    v2_values = [0.3, 0.4]

    await async_index.upsert(
        vectors=[
            {"id": v1_id, "vector": v1_values, "metadata": v1_metadata},
            {"id": v2_id, "vector": v2_values},
        ]
    )

    res = await async_index.fetch(
        ids=[v1_id, v2_id], include_vectors=True, include_metadata=True
    )

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values


def test_upsert_vector(index: Index):
    v1_id = "vector_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "vector_id2"
    v2_values = [0.3, 0.4]

    index.upsert(
        vectors=[
            Vector(id=v1_id, vector=v1_values, metadata=v1_metadata),
            Vector(id=v2_id, vector=v2_values),
        ]
    )

    res = index.fetch(ids=[v1_id, v2_id], include_vectors=True, include_metadata=True)

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values


@pytest.mark.asyncio
async def test_upsert_vector_async(async_index: AsyncIndex):
    v1_id = "vector_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "vector_id2"
    v2_values = [0.3, 0.4]

    await async_index.upsert(
        vectors=[
            Vector(id=v1_id, vector=v1_values, metadata=v1_metadata),
            Vector(id=v2_id, vector=v2_values),
        ]
    )

    res = await async_index.fetch(
        ids=[v1_id, v2_id], include_vectors=True, include_metadata=True
    )

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values


def test_upsert_tuple_with_numpy(index: Index):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = np.array([0.1, 0.2])

    v2_id = "id2"
    v2_values = np.array([0.3, 0.4])

    index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
        ]
    )

    res = index.fetch(ids=[v1_id, v2_id], include_vectors=True, include_metadata=True)

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values.tolist()

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values.tolist()


def test_upsert_dict_with_numpy(index: Index):
    v1_id = "dict_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = np.array([0.1, 0.2])

    v2_id = "dict_id2"
    v2_values = np.array([0.3, 0.4])

    index.upsert(
        vectors=[
            {"id": v1_id, "vector": v1_values, "metadata": v1_metadata},
            {"id": v2_id, "vector": v2_values},
        ]
    )

    res = index.fetch(ids=[v1_id, v2_id], include_vectors=True, include_metadata=True)

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values.tolist()

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values.tolist()


def test_upsert_vector_with_numpy(index: Index):
    v1_id = "vector_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = np.array([0.1, 0.2])

    v2_id = "vector_id2"
    v2_values = np.array([0.3, 0.4])

    index.upsert(
        vectors=[
            Vector(id=v1_id, vector=v1_values, metadata=v1_metadata),
            Vector(id=v2_id, vector=v2_values),
        ]
    )

    res = index.fetch(ids=[v1_id, v2_id], include_vectors=True, include_metadata=True)

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values.tolist()

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values.tolist()


def test_upsert_tuple_with_pandas(index: Index):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = pd.array([0.1, 0.2])

    v2_id = "id2"
    v2_values = pd.array([0.3, 0.4])

    assert v2_values == [0.3, 0.4]

    index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
        ]
    )

    res = index.fetch(ids=[v1_id, v2_id], include_vectors=True, include_metadata=True)

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values


def test_upsert_dict_with_pandas(index: Index):
    v1_id = "dict_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = pd.array([0.1, 0.2])

    v2_id = "dict_id2"
    v2_values = pd.array([0.3, 0.4])

    index.upsert(
        vectors=[
            {"id": v1_id, "vector": v1_values, "metadata": v1_metadata},
            {"id": v2_id, "vector": v2_values},
        ]
    )

    res = index.fetch(ids=[v1_id, v2_id], include_vectors=True, include_metadata=True)

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values


def test_upsert_vector_with_pandas(index: Index):
    v1_id = "vector_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = pd.array([0.1, 0.2])

    v2_id = "vector_id2"
    v2_values = pd.array([0.3, 0.4])

    index.upsert(
        vectors=[
            Vector(id=v1_id, vector=v1_values, metadata=v1_metadata),
            Vector(id=v2_id, vector=v2_values),
        ]
    )

    res = index.fetch(ids=[v1_id, v2_id], include_vectors=True, include_metadata=True)

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values


def test_upsert_data(index: Index):
    v1_id = "vector_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_data = "Hello-World"

    v2_id = "vector_id2"
    v2_data = "Goodbye-World"

    index.upsert(
        vectors=[
            Data(id=v1_id, data=v1_data, metadata=v1_metadata),
            Data(id=v2_id, data=v2_data),
        ]
    )

    res = index.fetch(ids=[v1_id, v2_id], include_vectors=True, include_metadata=True)

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None


@pytest.mark.asyncio
async def test_upsert_data_async(async_index: AsyncIndex):
    v1_id = "vector_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_data = "Hello-World"

    v2_id = "vector_id2"
    v2_data = "Goodbye-World"

    await async_index.upsert(
        vectors=[
            Data(id=v1_id, data=v1_data, metadata=v1_metadata),
            Data(id=v2_id, data=v2_data),
        ]
    )

    res = await async_index.fetch(
        ids=[v1_id, v2_id], include_vectors=True, include_metadata=True
    )

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None


def test_upsert_data_with_vectors(index: Index):
    v1_id = "vector_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_data = "Hello-World"

    v2_id = "vector_id2"
    v2_data = "Goodbye-World"

    with raises(UpstashError):
        index.upsert(
            vectors=[
                Data(id=v1_id, data=v1_data, metadata=v1_metadata),
                Data(id=v2_id, data=v2_data),
            ]
        )


@pytest.mark.asyncio
async def test_upsert_data_with_vectors_async(async_index: AsyncIndex):
    v1_id = "vector_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_data = "Hello-World"

    v2_id = "vector_id2"
    v2_values = [0.1, 0.2]

    with raises(UpstashError):
        await async_index.upsert(
            vectors=[
                Data(id=v1_id, data=v1_data, metadata=v1_metadata),
                Vector(id=v2_id, vector=v2_values),
            ]
        )


def test_invalid_payload(index: Index):
    v1_id = "id1"
    v1_data = "cookie"

    v2_id = "id2"
    v2_vector = [2, 2]

    with raises(ClientError):
        index.upsert(
            vectors=[
                Data(v1_id, v1_data, v1_metadata),
                (v2_id, v2_vector, v2_metadata),
            ]
        )


@pytest.mark.asyncio
async def test_invalid_payload_async(async_index: AsyncIndex):
    v1_id = "id1"
    v1_data = "cookie"

    v2_id = "id2"
    v2_vector = [2, 2]

    with raises(ClientError):
        await async_index.upsert(
            vectors=[
                Data(v1_id, v1_data, v1_metadata),
                (v2_id, v2_vector, v2_metadata),
            ]
        )
