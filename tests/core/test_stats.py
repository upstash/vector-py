from tests import assert_eventually
from upstash_vector import Index


def test_stats(index: Index):
    stats = index.stats()

    assert stats.vector_count == 0
    assert stats.pending_vector_count == 0

    index.upsert([{"id": "foo", "vector": [0, 1]}])

    def assertion():
        assert index.stats().vector_count == 1

    assert_eventually(assertion)
