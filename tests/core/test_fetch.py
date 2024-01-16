import os
from upstash_vector import Index
import pytest

url = os.environ["URL"]
token = os.environ["TOKEN"]


@pytest.fixture(autouse=True)
def reset_index():
    Index(url=url, token=token).reset()


def test_fetch_with_vectors_with_metadata():
    index = Index(url=url, token=token)

    v1_id = "v1-id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "v1-id2"
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


def test_fetch_with_vectors_without_metadata():
    index = Index(url=url, token=token)

    v1_id = "v2-id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "v2-id2"
    v2_values = [0.3, 0.4]

    res = index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
        ]
    )

    res = index.fetch(ids=[v1_id, v2_id], include_vectors=True, include_metadata=False)

    assert res[0].id == v1_id
    assert res[0].metadata == None
    assert res[0].vector == v1_values

    assert res[1].id == v2_id
    assert res[1].metadata == None
    assert res[1].vector == v2_values


def test_fetch_without_vectors_without_metadata():
    index = Index(url=url, token=token)

    v1_id = "v3-id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "v3-id2"
    v2_values = [0.3, 0.4]

    res = index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
        ]
    )

    res = index.fetch(ids=[v1_id, v2_id], include_vectors=False, include_metadata=False)

    assert res[0].id == v1_id
    assert res[0].metadata == None
    assert res[0].vector == None

    assert res[1].id == v2_id
    assert res[1].metadata == None
    assert res[1].vector == None
