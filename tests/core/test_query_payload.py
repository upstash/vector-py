import pytest

from upstash_vector.errors import ClientError
from upstash_vector.utils import query_requests_to_payload


def test_query_requests_to_payload_rejects_vector_then_data() -> None:
    with pytest.raises(ClientError, match="cannot be mixed"):
        query_requests_to_payload(
            [
                {"vector": [0.1, 0.2]},
                {"data": "hello"},
            ]
        )


def test_query_requests_to_payload_rejects_sparse_vector_then_data() -> None:
    with pytest.raises(ClientError, match="cannot be mixed"):
        query_requests_to_payload(
            [
                {"sparse_vector": ([0, 2], [0.5, 0.8])},
                {"data": "hello"},
            ]
        )


def test_query_requests_to_payload_rejects_data_then_vector() -> None:
    with pytest.raises(ClientError, match="cannot be mixed"):
        query_requests_to_payload(
            [
                {"data": "hello"},
                {"vector": [0.1, 0.2]},
            ]
        )


def test_query_requests_to_payload_allows_homogeneous_batches() -> None:
    is_vector, vector_payloads = query_requests_to_payload(
        [
            {"vector": [0.1, 0.2]},
            {"sparse_vector": ([1], [0.4])},
        ]
    )
    assert is_vector is True
    assert len(vector_payloads) == 2

    is_vector_data, data_payloads = query_requests_to_payload(
        [
            {"data": "first"},
            {"data": "second"},
        ]
    )
    assert is_vector_data is False
    assert data_payloads == [{"data": "first"}, {"data": "second"}]
