from upstash_vector import Index
from upstash_vector.types import Vector

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
            Vector(id=v1_id, vector=v1_values, metadata=v1_metadata),  # type: ignore
            Vector(id=v2_id, vector=v2_values),  # type: ignore
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
            Vector(id=v1_id, vector=v1_values, metadata=v1_metadata),  # type: ignore
            Vector(id=v2_id, vector=v2_values),  # type: ignore
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
