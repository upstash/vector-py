import os
from upstash_vector import Index
import pytest

url = os.environ["URL"]
token = os.environ["TOKEN"]


@pytest.fixture(autouse=True)
def reset_index():
    Index(url=url, token=token).reset()


def test_upsert_tuple():
    index = Index(url=url, token=token)

    v1_id = "id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "id2"
    v2_values = [0.3, 0.4]

    res = index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
        ]
    )

    res = index.fetch(ids=[v1_id, v2_id], include_vectors=True, include_metadata=True)

    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values

    assert res[1].id == v2_id
    assert res[1].metadata == None
    assert res[1].vector == v2_values


def test_upsert_dict():
    index = Index(url=url, token=token)

    v1_id = "dict_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "dict_id2"
    v2_values = [0.3, 0.4]

    res = index.upsert(
        vectors=[
            {"id": v1_id, "vector": v1_values, "metadata": v1_metadata},
            {"id": v2_id, "vector": v2_values},
        ]
    )

    res = index.fetch(ids=[v1_id, v2_id], include_vectors=True, include_metadata=True)

    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values

    assert res[1].id == v2_id
    assert res[1].metadata == None
    assert res[1].vector == v2_values


def test_upsert_vector():
    from upstash_vector import Vector

    index = Index(url=url, token=token)

    v1_id = "vector_id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "vector_id2"
    v2_values = [0.3, 0.4]

    res = index.upsert(
        vectors=[
            Vector(id=v1_id, vector=v1_values, metadata=v1_metadata),
            Vector(id=v2_id, vector=v2_values),
        ]
    )

    res = index.fetch(ids=[v1_id, v2_id], include_vectors=True, include_metadata=True)

    assert res[0].id == v1_id
    assert res[0].metadata == v1_metadata
    assert res[0].vector == v1_values

    assert res[1].id == v2_id
    assert res[1].metadata == None
    assert res[1].vector == v2_values
