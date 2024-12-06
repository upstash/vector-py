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
