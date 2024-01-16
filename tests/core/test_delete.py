import os
from upstash_vector import Index
import pytest

url = os.environ["URL"]
token = os.environ["TOKEN"]


@pytest.fixture(autouse=True)
def reset_index():
    Index(url=url, token=token).reset()


def test_delete():
    index = Index(url=url, token=token)

    v1_id = "delete-id1"
    v1_metadata = {"metadata_field": "metadata_value"}
    v1_values = [0.1, 0.2]

    v2_id = "delete-id2"
    v2_values = [0.3, 0.4]

    v3_id = "delete-id3"
    v3_values = [0.5, 0.6]

    res = index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
            (v3_id, v3_values),
        ]
    )

    res = index.fetch(
        ids=[v1_id, v2_id, v3_id], include_vectors=True, include_metadata=True
    )
    assert len(res) == 3
    assert res[0] is not None
    assert res[1] is not None
    assert res[2] is not None

    del_res = index.delete(ids=v1_id)
    assert del_res.deleted_count == 1

    res = index.fetch(ids=[v1_id, v2_id], include_vectors=True, include_metadata=True)
    assert res[0] is None
    assert res[1] is not None

    index.upsert(
        vectors=[
            (v1_id, v1_values, v1_metadata),
            (v2_id, v2_values),
            (v3_id, v3_values),
        ]
    )

    del_res = index.delete(ids=[v1_id, v2_id])
    assert del_res.deleted_count == 2

    res = index.fetch(
        ids=[v1_id, v2_id, v3_id], include_vectors=True, include_metadata=True
    )
    assert res[0] is None
    assert res[1] is None
    assert res[2] is not None
