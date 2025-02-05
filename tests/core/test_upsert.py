import numpy as np
import pandas as pd
import pytest
from pytest import raises

from tests import NAMESPACES
from upstash_vector import AsyncIndex, Index
from upstash_vector.errors import ClientError
from upstash_vector.types import Data, SparseVector, Vector


@pytest.mark.parametrize("ns", NAMESPACES)
def test_upsert_tuple(index: Index, ns: str):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "id2"
    v2_values = [0.3, 0.4]

    v3_id = "id3"
    v3_values = [0.5, 0.6]
    v3_data = "data-value"

    index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
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
    assert res[0].data is None

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
async def test_upsert_tuple_async(async_index: AsyncIndex, ns: str):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "id2"
    v2_values = [0.3, 0.4]

    v3_id = "id3"
    v3_values = [0.5, 0.6]
    v3_data = "data-value"

    await async_index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
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
    assert res[0].data is None

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
def test_upsert_dict(index: Index, ns: str):
    v1_id = "dict_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "dict_id2"
    v2_values = [0.3, 0.4]

    v3_id = "id3"
    v3_values = [0.5, 0.6]
    v3_data = "data-value"

    index.upsert(
        vectors=[
            {"id": v1_id, "vector": v1_values, "metadata": v1_metadata},
            {"id": v2_id, "vector": v2_values},
            {"id": v3_id, "vector": v3_values, "data": v3_data},
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
    assert res[0].data is None

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
async def test_upsert_dict_async(async_index: AsyncIndex, ns: str):
    v1_id = "dict_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "dict_id2"
    v2_values = [0.3, 0.4]

    v3_id = "id3"
    v3_values = [0.5, 0.6]
    v3_data = "data-value"

    await async_index.upsert(
        vectors=[
            {"id": v1_id, "vector": v1_values, "metadata": v1_metadata},
            {"id": v2_id, "vector": v2_values},
            {"id": v3_id, "vector": v3_values, "data": v3_data},
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
    assert res[0].data is None

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
def test_upsert_vector(index: Index, ns: str):
    v1_id = "vector_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "vector_id2"
    v2_values = [0.3, 0.4]

    v3_id = "id3"
    v3_values = [0.5, 0.6]
    v3_data = "data-value"

    index.upsert(
        vectors=[
            Vector(id=v1_id, vector=v1_values, metadata=v1_metadata),
            Vector(id=v2_id, vector=v2_values),
            Vector(id=v3_id, vector=v3_values, data=v3_data),
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
    assert res[0].data is None

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
async def test_upsert_vector_async(async_index: AsyncIndex, ns: str):
    v1_id = "vector_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "vector_id2"
    v2_values = [0.3, 0.4]

    v3_id = "id3"
    v3_values = [0.5, 0.6]
    v3_data = "data-value"

    await async_index.upsert(
        vectors=[
            Vector(id=v1_id, vector=v1_values, metadata=v1_metadata),
            Vector(id=v2_id, vector=v2_values),
            Vector(id=v3_id, vector=v3_values, data=v3_data),
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
    assert res[0].data is None

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
def test_upsert_tuple_with_numpy(index: Index, ns: str):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = np.array([0.1, 0.2])

    v2_id = "id2"
    v2_values = np.array([0.3, 0.4])

    v3_id = "id3"
    v3_values = np.array([0.5, 0.6])
    v3_data = "data-value"

    index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
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
    assert res[0].vector == v1_values.tolist()
    assert res[0].data is None

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values.tolist()
    assert res[1].data is None

    assert res[2] is not None
    assert res[2].id == v3_id
    assert res[2].metadata is None
    assert res[2].vector == v3_values.tolist()
    assert res[2].data == v3_data


@pytest.mark.parametrize("ns", NAMESPACES)
def test_upsert_dict_with_numpy(index: Index, ns: str):
    v1_id = "dict_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = np.array([0.1, 0.2])

    v2_id = "dict_id2"
    v2_values = np.array([0.3, 0.4])

    v3_id = "dict_id3"
    v3_values = np.array([0.5, 0.6])
    v3_data = "data-value"

    index.upsert(
        vectors=[
            {"id": v1_id, "vector": v1_values, "metadata": v1_metadata},
            {"id": v2_id, "vector": v2_values},
            {"id": v3_id, "vector": v3_values, "data": v3_data},
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
    assert res[0].vector == v1_values.tolist()
    assert res[0].data is None

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values.tolist()
    assert res[1].data is None

    assert res[2] is not None
    assert res[2].id == v3_id
    assert res[2].metadata is None
    assert res[2].vector == v3_values.tolist()
    assert res[2].data == v3_data


@pytest.mark.parametrize("ns", NAMESPACES)
def test_upsert_vector_with_numpy(index: Index, ns: str):
    v1_id = "vector_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = np.array([0.1, 0.2])

    v2_id = "vector_id2"
    v2_values = np.array([0.3, 0.4])

    v3_id = "vector_id3"
    v3_values = np.array([0.5, 0.6])
    v3_data = "data-value"

    index.upsert(
        vectors=[
            Vector(id=v1_id, vector=v1_values, metadata=v1_metadata),
            Vector(id=v2_id, vector=v2_values),
            Vector(id=v3_id, vector=v3_values, data=v3_data),
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
    assert res[0].vector == v1_values.tolist()
    assert res[0].data is None

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].vector == v2_values.tolist()
    assert res[1].data is None

    assert res[2] is not None
    assert res[2].id == v3_id
    assert res[2].metadata is None
    assert res[2].vector == v3_values.tolist()
    assert res[2].data == v3_data


@pytest.mark.parametrize("ns", NAMESPACES)
def test_upsert_tuple_with_pandas(index: Index, ns: str):
    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = pd.array([0.1, 0.2])

    v2_id = "id2"
    v2_values = pd.array([0.3, 0.4])

    v3_id = "id3"
    v3_values = pd.array([0.5, 0.6])
    v3_data = "data-value"

    index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
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
    assert res[0].data is None

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
def test_upsert_dict_with_pandas(index: Index, ns: str):
    v1_id = "dict_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = pd.array([0.1, 0.2])

    v2_id = "dict_id2"
    v2_values = pd.array([0.3, 0.4])

    v3_id = "dict_id3"
    v3_values = pd.array([0.5, 0.6])
    v3_data = "data-value"

    index.upsert(
        vectors=[
            {"id": v1_id, "vector": v1_values, "metadata": v1_metadata},
            {"id": v2_id, "vector": v2_values},
            {"id": v3_id, "vector": v3_values, "data": v3_data},
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
    assert res[0].data is None

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
def test_upsert_vector_with_pandas(index: Index, ns: str):
    v1_id = "vector_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = pd.array([0.1, 0.2])

    v2_id = "vector_id2"
    v2_values = pd.array([0.3, 0.4])

    v3_id = "vector_id3"
    v3_values = pd.array([0.5, 0.6])
    v3_data = "data-value"

    index.upsert(
        vectors=[
            Vector(id=v1_id, vector=v1_values, metadata=v1_metadata),
            Vector(id=v2_id, vector=v2_values),
            Vector(id=v3_id, vector=v3_values, data=v3_data),
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
    assert res[0].data is None

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
def test_upsert_data(embedding_index: Index, ns: str):
    v1_id = "vector_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_data = "Hello-World"

    v2_id = "vector_id2"
    v2_data = "Goodbye-World"

    embedding_index.upsert(
        vectors=[
            Data(id=v1_id, data=v1_data, metadata=v1_metadata),
            Data(id=v2_id, data=v2_data),
        ],
        namespace=ns,
    )

    res = embedding_index.fetch(
        ids=[v1_id, v2_id],
        include_vectors=True,
        include_metadata=True,
        include_data=True,
        namespace=ns,
    )

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].data == v1_data

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].data == v2_data


def test_upsert_to_none_namespace(embedding_index: Index):
    v_id = "none_namespace_vector_id"
    v1_data = "Hello-World"

    embedding_index.upsert(
        vectors=[
            Data(id=v_id, data=v1_data),
        ],
        namespace=None,  # type: ignore[arg-type]
    )

    # test if the data is indeed inserted to default namespace with namespace=None config
    res = embedding_index.fetch(ids=[v_id], include_data=True, namespace="")

    assert res[0] is not None
    assert res[0].id == v_id


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_upsert_data_async(async_embedding_index: AsyncIndex, ns: str):
    v1_id = "vector_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_data = "Hello-World"

    v2_id = "vector_id2"
    v2_data = "Goodbye-World"

    await async_embedding_index.upsert(
        vectors=[
            Data(id=v1_id, data=v1_data, metadata=v1_metadata),
            Data(id=v2_id, data=v2_data),
        ],
        namespace=ns,
    )

    res = await async_embedding_index.fetch(
        ids=[v1_id, v2_id],
        include_vectors=True,
        include_metadata=True,
        include_data=True,
        namespace=ns,
    )

    assert res[0] is not None
    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].data == v1_data

    assert res[1] is not None
    assert res[1].id == v2_id
    assert res[1].metadata is None
    assert res[1].data == v2_data


@pytest.mark.parametrize("ns", NAMESPACES)
def test_upsert_data_with_vectors(embedding_index: Index, ns: str):
    v1_id = "vector_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_data = "Hello-World"

    v2_id = "vector_id2"
    v2_values = [0.1, 0.2]

    with raises(ClientError):
        embedding_index.upsert(
            vectors=[
                Data(id=v1_id, data=v1_data, metadata=v1_metadata),
                Vector(id=v2_id, vector=v2_values),
            ],
            namespace=ns,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_upsert_data_with_vectors_async(async_index: AsyncIndex, ns: str):
    v1_id = "vector_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_data = "Hello-World"

    v2_id = "vector_id2"
    v2_values = [0.1, 0.2]

    with raises(ClientError):
        await async_index.upsert(
            vectors=[
                Data(id=v1_id, data=v1_data, metadata=v1_metadata),
                Vector(id=v2_id, vector=v2_values),
            ],
            namespace=ns,
        )


@pytest.mark.parametrize("ns", NAMESPACES)
def test_invalid_payload(index: Index, ns: str):
    v1_id = "id1"
    v1_data = "cookie"

    v2_id = "id2"
    v2_vector = [2, 2]

    with raises(ClientError):
        index.upsert(
            vectors=[
                Data(v1_id, v1_data),
                (v2_id, v2_vector),
            ],
            namespace=ns,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_invalid_payload_async(async_index: AsyncIndex, ns: str):
    v1_id = "id1"
    v1_data = "cookie"

    v2_id = "id2"
    v2_vector = [2, 2]

    with raises(ClientError):
        await async_index.upsert(
            vectors=[
                Data(v1_id, v1_data),
                (v2_id, v2_vector),
            ],
            namespace=ns,
        )


@pytest.mark.parametrize("ns", NAMESPACES)
def test_upsert_sparse(sparse_index: Index, ns: str):
    sparse_index.upsert(
        vectors=[
            (
                "id0",
                ([0, 1], [0.1, 0.2]),
            ),
            (
                "id1",
                (np.array([1, 2]), [0.2, 0.3]),
                {"key1": "value"},
            ),
            (
                "id2",
                ([2, 3], pd.array([0.3, 0.4])),
                {"key2": "value"},
                "data2",
            ),
            Vector(
                "id3",
                sparse_vector=([10, 20, 30, 40], [1.02, 2.01, 3.3, 4.4]),
            ),
            (
                "id4",
                SparseVector([2, 39, 93], [0.3, 0.5, 0.1]),
            ),
            (
                "id5",
                SparseVector(np.array([11]), pd.array([11.5])),
            ),
            {
                "id": "id6",
                "sparse_vector": ([42, 43, 44, 45, 46, 47], [42, 43, 44, 45, 46, 47]),
                "metadata": {"key6": "value"},
            },
            {
                "id": "id7",
                "sparse_vector": SparseVector([4, 5], [4.5, 5.4]),
                "metadata": {"key7": "value"},
                "data": "data7",
            },
            {
                "id": "id8",
                "sparse_vector": (np.array([0, 3]), np.array([0.1, 3.0])),
            },
            Vector(
                "id9",
                sparse_vector=([1, 2], [1.2, 2.1]),
            ),
            Vector(
                "id10",
                sparse_vector=SparseVector([1], np.array([12.0])),
            ),
        ],
        namespace=ns,
    )

    ids = [f"id{i}" for i in range(11)]
    res = sparse_index.fetch(
        ids,
        include_vectors=True,
        namespace=ns,
    )
    assert len(res) == 11
    for i, r in enumerate(res):
        assert r is not None
        assert r.id == f"id{i}"
        assert r.sparse_vector is not None


@pytest.mark.parametrize("ns", NAMESPACES)
def test_upsert_hybrid(hybrid_index: Index, ns: str):
    hybrid_index.upsert(
        vectors=[
            (
                "id0",
                [0.1, 0.1],
                ([0, 1], [0.1, 0.2]),
            ),
            (
                "id1",
                np.array([0.5, 0.5]),
                (np.array([1, 2]), [0.2, 0.3]),
                {"key1": "value"},
            ),
            (
                "id2",
                pd.array([0.1, 0.2]),
                ([2, 3], pd.array([0.3, 0.4])),
                {"key2": "value"},
                "data2",
            ),
            Vector(
                "id3",
                vector=[3.3, 1.0],
                sparse_vector=([10, 20, 30, 40], [1.02, 2.01, 3.3, 4.4]),
            ),
            (
                "id4",
                [1.1, 2.2],
                SparseVector([2, 39, 93], [0.3, 0.5, 0.1]),
            ),
            (
                "id5",
                [1.0, 5.0],
                SparseVector(np.array([11]), pd.array([11.5])),
            ),
            {
                "id": "id6",
                "vector": [13.0, 0.5],
                "sparse_vector": ([42, 43, 44, 45, 46, 47], [42, 43, 44, 45, 46, 47]),
                "metadata": {"key6": "value"},
            },
            {
                "id": "id7",
                "vector": np.array([4.2, 2.4]),
                "sparse_vector": SparseVector([4, 5], [4.5, 5.4]),
                "metadata": {"key7": "value"},
                "data": "data7",
            },
            {
                "id": "id8",
                "vector": [13, 5.4],
                "sparse_vector": (np.array([0, 3]), np.array([0.1, 3.0])),
            },
            Vector(
                "id9",
                vector=np.array([0.9, 0.7]),
                sparse_vector=([1, 2], [1.2, 2.1]),
            ),
            Vector(
                "id10",
                vector=[0.0, 1.0],
                sparse_vector=SparseVector([1], np.array([12.0])),
            ),
        ],
        namespace=ns,
    )

    ids = [f"id{i}" for i in range(11)]
    res = hybrid_index.fetch(
        ids,
        include_vectors=True,
        namespace=ns,
    )
    assert len(res) == 11
    for i, r in enumerate(res):
        assert r is not None
        assert r.id == f"id{i}"
        assert r.vector is not None
        assert len(r.vector) == 2
        assert r.sparse_vector is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_upsert_sparse_async(async_sparse_index: AsyncIndex, ns: str):
    await async_sparse_index.upsert(
        vectors=[
            (
                "id0",
                ([0, 1], [0.1, 0.2]),
            ),
            (
                "id1",
                (np.array([1, 2]), [0.2, 0.3]),
                {"key1": "value"},
            ),
            (
                "id2",
                ([2, 3], pd.array([0.3, 0.4])),
                {"key2": "value"},
                "data2",
            ),
            Vector(
                "id3",
                sparse_vector=([10, 20, 30, 40], [1.02, 2.01, 3.3, 4.4]),
            ),
            (
                "id4",
                SparseVector([2, 39, 93], [0.3, 0.5, 0.1]),
            ),
            (
                "id5",
                SparseVector(np.array([11]), pd.array([11.5])),
            ),
            {
                "id": "id6",
                "sparse_vector": ([42, 43, 44, 45, 46, 47], [42, 43, 44, 45, 46, 47]),
                "metadata": {"key6": "value"},
            },
            {
                "id": "id7",
                "sparse_vector": SparseVector([4, 5], [4.5, 5.4]),
                "metadata": {"key7": "value"},
                "data": "data7",
            },
            {
                "id": "id8",
                "sparse_vector": (np.array([0, 3]), np.array([0.1, 3.0])),
            },
            Vector(
                "id9",
                sparse_vector=([1, 2], [1.2, 2.1]),
            ),
            Vector(
                "id10",
                sparse_vector=SparseVector([1], np.array([12.0])),
            ),
        ],
        namespace=ns,
    )

    ids = [f"id{i}" for i in range(11)]
    res = await async_sparse_index.fetch(
        ids,
        include_vectors=True,
        namespace=ns,
    )
    assert len(res) == 11
    for i, r in enumerate(res):
        assert r is not None
        assert r.id == f"id{i}"
        assert r.sparse_vector is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_upsert_hybrid_async(async_hybrid_index: AsyncIndex, ns: str):
    await async_hybrid_index.upsert(
        vectors=[
            (
                "id0",
                [0.1, 0.1],
                ([0, 1], [0.1, 0.2]),
            ),
            (
                "id1",
                np.array([0.5, 0.5]),
                (np.array([1, 2]), [0.2, 0.3]),
                {"key1": "value"},
            ),
            (
                "id2",
                pd.array([0.1, 0.2]),
                ([2, 3], pd.array([0.3, 0.4])),
                {"key2": "value"},
                "data2",
            ),
            Vector(
                "id3",
                vector=[3.3, 1.0],
                sparse_vector=([10, 20, 30, 40], [1.02, 2.01, 3.3, 4.4]),
            ),
            (
                "id4",
                [1.1, 2.2],
                SparseVector([2, 39, 93], [0.3, 0.5, 0.1]),
            ),
            (
                "id5",
                [1.0, 5.0],
                SparseVector(np.array([11]), pd.array([11.5])),
            ),
            {
                "id": "id6",
                "vector": [13.0, 0.5],
                "sparse_vector": ([42, 43, 44, 45, 46, 47], [42, 43, 44, 45, 46, 47]),
                "metadata": {"key6": "value"},
            },
            {
                "id": "id7",
                "vector": np.array([4.2, 2.4]),
                "sparse_vector": SparseVector([4, 5], [4.5, 5.4]),
                "metadata": {"key7": "value"},
                "data": "data7",
            },
            {
                "id": "id8",
                "vector": [13, 5.4],
                "sparse_vector": (np.array([0, 3]), np.array([0.1, 3.0])),
            },
            Vector(
                "id9",
                vector=np.array([0.9, 0.7]),
                sparse_vector=([1, 2], [1.2, 2.1]),
            ),
            Vector(
                "id10",
                vector=[0.0, 1.0],
                sparse_vector=SparseVector([1], np.array([12.0])),
            ),
        ],
        namespace=ns,
    )

    ids = [f"id{i}" for i in range(11)]
    res = await async_hybrid_index.fetch(
        ids,
        include_vectors=True,
        namespace=ns,
    )
    assert len(res) == 11
    for i, r in enumerate(res):
        assert r is not None
        assert r.id == f"id{i}"
        assert r.vector is not None
        assert len(r.vector) == 2
        assert r.sparse_vector is not None


@pytest.mark.parametrize("ns", NAMESPACES)
def test_upsert_data_as_vector(embedding_index: Index, ns: str):
    embedding_index.upsert(
        vectors=[
            Vector(id="test", data="data"),
        ],
        namespace=ns,
    )

    res = embedding_index.fetch(
        ids=["test"],
        include_data=True,
        namespace=ns,
    )

    assert res[0] is not None
    assert res[0].id == "test"
    assert res[0].data == "data"


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_upsert_data_as_vector_async(async_embedding_index: AsyncIndex, ns: str):
    await async_embedding_index.upsert(
        vectors=[
            Vector(id="test", data="data"),
        ],
        namespace=ns,
    )

    res = await async_embedding_index.fetch(
        ids=["test"],
        include_data=True,
        namespace=ns,
    )

    assert res[0] is not None
    assert res[0].id == "test"
    assert res[0].data == "data"
