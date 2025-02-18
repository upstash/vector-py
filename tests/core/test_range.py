import random

import pytest
from pytest import raises

from tests import NAMESPACES
from upstash_vector import AsyncIndex, Index
from upstash_vector.errors import ClientError


@pytest.mark.parametrize("ns", NAMESPACES)
def test_range(index: Index, ns: str):
    vectors = [
        {
            "id": f"id-{i}",
            "vector": [random.random() for _ in range(2)],
            "metadata": {"meta": i},
            "data": f"data-{i}",
        }
        for i in range(20)
    ]

    index.upsert(vectors=vectors, namespace=ns)

    res = index.range(
        cursor="",
        limit=4,
        include_vectors=True,
        include_metadata=True,
        include_data=True,
        namespace=ns,
    )
    assert len(res.vectors) == 4
    assert res.next_cursor != ""

    for i in range(4):
        assert res.vectors[i].id == f"id-{i}"
        assert res.vectors[i].metadata == {"meta": i}
        assert res.vectors[i].data == f"data-{i}"
        v = res.vectors[i].vector
        assert v is not None
        assert len(v) == 2

    while res.next_cursor != "":
        res = index.range(
            cursor=res.next_cursor,
            limit=8,
            include_vectors=True,
            namespace=ns,
        )
        assert len(res.vectors) == 8

    with raises(ClientError):
        index.range(
            cursor="0",
            limit=0,
            include_vectors=True,
            namespace=ns,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_range_async(async_index: AsyncIndex, ns: str):
    vectors = [
        {
            "id": f"id-{i}",
            "vector": [random.random() for _ in range(2)],
            "metadata": {"meta": i},
            "data": f"data-{i}",
        }
        for i in range(20)
    ]

    await async_index.upsert(vectors=vectors, namespace=ns)

    res = await async_index.range(
        cursor="",
        limit=4,
        include_vectors=True,
        include_metadata=True,
        include_data=True,
        namespace=ns,
    )
    assert len(res.vectors) == 4
    assert res.next_cursor != ""

    for i in range(4):
        assert res.vectors[i].id == f"id-{i}"
        assert res.vectors[i].metadata == {"meta": i}
        assert res.vectors[i].data == f"data-{i}"
        v = res.vectors[i].vector
        assert v is not None
        assert len(v) == 2

    while res.next_cursor != "":
        res = await async_index.range(
            cursor=res.next_cursor,
            limit=8,
            include_vectors=True,
            namespace=ns,
        )
        assert len(res.vectors) == 8

    with raises(ClientError):
        await async_index.range(
            cursor="0",
            limit=0,
            include_vectors=True,
            namespace=ns,
        )


@pytest.mark.parametrize("ns", NAMESPACES)
def test_range_sparse(sparse_index: Index, ns: str):
    vectors = [
        {
            "id": f"id-{i}",
            "sparse_vector": (
                [random.randint(0, 10) for _ in range(2)],
                [random.random() for _ in range(2)],
            ),
            "metadata": {"meta": i},
            "data": f"data-{i}",
        }
        for i in range(20)
    ]

    sparse_index.upsert(vectors=vectors, namespace=ns)

    res = sparse_index.range(
        cursor="",
        limit=4,
        include_vectors=True,
        include_metadata=True,
        include_data=True,
        namespace=ns,
    )
    assert len(res.vectors) == 4
    assert res.next_cursor != ""

    for i in range(4):
        assert res.vectors[i].id == f"id-{i}"
        assert res.vectors[i].metadata == {"meta": i}
        assert res.vectors[i].data == f"data-{i}"
        assert res.vectors[i].sparse_vector is not None

    while res.next_cursor != "":
        res = sparse_index.range(
            cursor=res.next_cursor,
            limit=8,
            include_vectors=True,
            namespace=ns,
        )
        assert len(res.vectors) == 8

    with raises(ClientError):
        sparse_index.range(
            cursor="0",
            limit=0,
            include_vectors=True,
            namespace=ns,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_range_sparse_async(async_sparse_index: AsyncIndex, ns: str):
    vectors = [
        {
            "id": f"id-{i}",
            "sparse_vector": (
                [random.randint(0, 10) for _ in range(2)],
                [random.random() for _ in range(2)],
            ),
            "metadata": {"meta": i},
            "data": f"data-{i}",
        }
        for i in range(20)
    ]

    await async_sparse_index.upsert(vectors=vectors, namespace=ns)

    res = await async_sparse_index.range(
        cursor="",
        limit=4,
        include_vectors=True,
        include_metadata=True,
        include_data=True,
        namespace=ns,
    )
    assert len(res.vectors) == 4
    assert res.next_cursor != ""

    for i in range(4):
        assert res.vectors[i].id == f"id-{i}"
        assert res.vectors[i].metadata == {"meta": i}
        assert res.vectors[i].data == f"data-{i}"
        assert res.vectors[i].sparse_vector is not None

    while res.next_cursor != "":
        res = await async_sparse_index.range(
            cursor=res.next_cursor,
            limit=8,
            include_vectors=True,
            namespace=ns,
        )
        assert len(res.vectors) == 8

    with raises(ClientError):
        await async_sparse_index.range(
            cursor="0",
            limit=0,
            include_vectors=True,
            namespace=ns,
        )


@pytest.mark.parametrize("ns", NAMESPACES)
def test_range_hybrid(hybrid_index: Index, ns: str):
    vectors = [
        {
            "id": f"id-{i}",
            "vector": [random.random() for _ in range(2)],
            "sparse_vector": (
                [random.randint(0, 10) for _ in range(2)],
                [random.random() for _ in range(2)],
            ),
            "metadata": {"meta": i},
            "data": f"data-{i}",
        }
        for i in range(20)
    ]

    hybrid_index.upsert(vectors=vectors, namespace=ns)

    res = hybrid_index.range(
        cursor="",
        limit=4,
        include_vectors=True,
        include_metadata=True,
        include_data=True,
        namespace=ns,
    )
    assert len(res.vectors) == 4
    assert res.next_cursor != ""

    for i in range(4):
        assert res.vectors[i].id == f"id-{i}"
        assert res.vectors[i].metadata == {"meta": i}
        assert res.vectors[i].data == f"data-{i}"
        v = res.vectors[i].vector
        assert v is not None
        assert len(v) == 2
        assert res.vectors[i].sparse_vector is not None

    while res.next_cursor != "":
        res = hybrid_index.range(
            cursor=res.next_cursor,
            limit=8,
            include_vectors=True,
            namespace=ns,
        )
        assert len(res.vectors) == 8

    with raises(ClientError):
        hybrid_index.range(
            cursor="0",
            limit=0,
            include_vectors=True,
            namespace=ns,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_range_hybrid_async(async_hybrid_index: AsyncIndex, ns: str):
    vectors = [
        {
            "id": f"id-{i}",
            "vector": [random.random() for _ in range(2)],
            "sparse_vector": (
                [random.randint(0, 10) for _ in range(2)],
                [random.random() for _ in range(2)],
            ),
            "metadata": {"meta": i},
            "data": f"data-{i}",
        }
        for i in range(20)
    ]

    await async_hybrid_index.upsert(vectors=vectors, namespace=ns)

    res = await async_hybrid_index.range(
        cursor="",
        limit=4,
        include_vectors=True,
        include_metadata=True,
        include_data=True,
        namespace=ns,
    )
    assert len(res.vectors) == 4
    assert res.next_cursor != ""

    for i in range(4):
        assert res.vectors[i].id == f"id-{i}"
        assert res.vectors[i].metadata == {"meta": i}
        assert res.vectors[i].data == f"data-{i}"
        v = res.vectors[i].vector
        assert v is not None
        assert len(v) == 2
        assert res.vectors[i].sparse_vector is not None

    while res.next_cursor != "":
        res = await async_hybrid_index.range(
            cursor=res.next_cursor,
            limit=8,
            include_vectors=True,
            namespace=ns,
        )
        assert len(res.vectors) == 8

    with raises(ClientError):
        await async_hybrid_index.range(
            cursor="0",
            limit=0,
            include_vectors=True,
            namespace=ns,
        )


@pytest.mark.parametrize("ns", NAMESPACES)
def test_range_prefix(index: Index, ns: str):
    index.upsert(
        vectors=[
            ("id-00", [0.1, 0.2]),
            ("id-01", [0.1, 0.3]),
            ("id-10", [0.1, 0.4]),
            ("id-11", [0.1, 0.5]),
            ("id-12", [0.1, 0.6]),
            ("id-13", [0.1, 0.7]),
        ],
        namespace=ns,
    )

    result = index.range(
        limit=2,
        prefix="id-1",
        namespace=ns,
    )

    assert len(result.vectors) == 2
    assert result.vectors[0].id == "id-10"
    assert result.vectors[1].id == "id-11"

    assert result.next_cursor != ""

    result = index.range(
        cursor=result.next_cursor,
        limit=2,
        prefix="id-1",
        namespace=ns,
    )

    assert len(result.vectors) == 2
    assert result.vectors[0].id == "id-12"
    assert result.vectors[1].id == "id-13"

    assert result.next_cursor == ""


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_range_prefix_async(async_index: AsyncIndex, ns: str):
    await async_index.upsert(
        vectors=[
            ("id-00", [0.1, 0.2]),
            ("id-01", [0.1, 0.3]),
            ("id-10", [0.1, 0.4]),
            ("id-11", [0.1, 0.5]),
            ("id-12", [0.1, 0.6]),
            ("id-13", [0.1, 0.7]),
        ],
        namespace=ns,
    )

    result = await async_index.range(
        limit=2,
        prefix="id-1",
        namespace=ns,
    )

    assert len(result.vectors) == 2
    assert result.vectors[0].id == "id-10"
    assert result.vectors[1].id == "id-11"

    assert result.next_cursor != ""

    result = await async_index.range(
        cursor=result.next_cursor,
        limit=2,
        prefix="id-1",
        namespace=ns,
    )

    assert len(result.vectors) == 2
    assert result.vectors[0].id == "id-12"
    assert result.vectors[1].id == "id-13"

    assert result.next_cursor == ""
