import pytest

from tests import NAMESPACES, ensure_ns_exists, ensure_ns_exists_async
from upstash_vector import AsyncIndex, Index
from upstash_vector.core.index_operations import DEFAULT_NAMESPACE


def test_list_namespaces(index: Index):
    for ns in NAMESPACES:
        ensure_ns_exists(index, ns)

    all_ns = index.list_namespaces()

    assert len(all_ns) == len(NAMESPACES)
    assert NAMESPACES[0] in all_ns
    assert NAMESPACES[1] in all_ns


@pytest.mark.asyncio
async def test_list_namespaces_async(async_index: AsyncIndex):
    for ns in NAMESPACES:
        await ensure_ns_exists_async(async_index, ns)

    all_ns = await async_index.list_namespaces()

    assert len(all_ns) == len(NAMESPACES)
    assert NAMESPACES[0] in all_ns
    assert NAMESPACES[1] in all_ns


def test_delete_namespaces(index: Index):
    for ns in NAMESPACES:
        ensure_ns_exists(index, ns)

    for ns in NAMESPACES:
        if ns == DEFAULT_NAMESPACE:
            continue

        # Should not fail
        index.delete_namespace(namespace=ns)

    info = index.info()

    # Only default namespace should exist
    assert len(info.namespaces) == 1


@pytest.mark.asyncio
async def test_delete_namespaces_async(async_index: AsyncIndex):
    for ns in NAMESPACES:
        await ensure_ns_exists_async(async_index, ns)

    for ns in NAMESPACES:
        if ns == DEFAULT_NAMESPACE:
            continue

        # Should not fail
        await async_index.delete_namespace(namespace=ns)

    info = await async_index.info()

    # Only default namespace should exist
    assert len(info.namespaces) == 1
