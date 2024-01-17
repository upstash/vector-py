from upstash_vector import Index
import random
from pytest import raises

from upstash_vector.errors import ClientError


def test_range(index: Index):
    vectors = [
        {"id": f"id-{i}", "vector": [random.random() for _ in range(2)]}
        for i in range(20)
    ]

    index.upsert(vectors=vectors)

    res = index.range(cursor="", limit=4, include_vectors=True)
    assert len(res.vectors) == 4
    assert res.next_cursor != ""

    while res.next_cursor != "":
        res = index.range(cursor=res.next_cursor, limit=8, include_vectors=True)
        assert len(res.vectors) == 8

    with raises(ClientError):
        index.range(cursor="0", limit=0, include_vectors=True)
