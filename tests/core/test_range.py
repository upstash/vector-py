import os
from upstash_vector import Index
import pytest
import random
from pytest import raises

from upstash_vector.errors import ClientError, UpstashError

url = os.environ["URL"]
token = os.environ["TOKEN"]


@pytest.fixture(autouse=True)
def reset_index():
    Index(url=url, token=token).reset()


def test_range():
    index = Index(url=url, token=token)

    vectors = [
        {"id": f"id-{i}", "vector": [random.random() for j in range(2)]}
        for i in range(20)
    ]

    res = index.upsert(vectors=vectors)

    res = index.range(cursor="", limit=4, include_vectors=True)
    assert len(res.vectors) == 4
    assert res.next_cursor != ""

    while res.next_cursor != "":
        res = index.range(cursor=res.next_cursor, limit=8, include_vectors=True)
        assert len(res.vectors) == 8

    with raises(ClientError):
        index.range(cursor="0", limit=0, include_vectors=True)
