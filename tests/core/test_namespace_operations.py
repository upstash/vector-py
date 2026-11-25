import pytest

from tests import NAMESPACES, ensure_ns_exists, ensure_ns_exists_async
from upstash_vector import AsyncIndex, Index
from upstash_vector.core.index_operations import DEFAULT_NAMESPACE


def test_list_namespaces(index: Index):
    for ns in NAMESPACES:
        ensure_ns_exists(index, ns)

    all_ns = index.list_namespaces()

    assert len(all_ns) >= len(NAMESPACES)
    assert NAMESPACES[0] in all_ns
    assert NAMESPACES[1] in all_ns


@pytest.mark.asyncio
async def test_list_namespaces_async(async_index: AsyncIndex):
    for ns in NAMESPACES:
        await ensure_ns_exists_async(async_index, ns)

    all_ns = await async_index.list_namespaces()

    assert len(all_ns) >= len(NAMESPACES)
    assert NAMESPACES[0] in all_ns
    assert NAMESPACES[1] in all_ns


def test_delete_namespaces(index: Index):
    for ns in NAMESPACES:
        ensure_ns_exists(index, ns)

    deleted = None
    for ns in NAMESPACES:
        if ns == DEFAULT_NAMESPACE:
            continue

        deleted = ns
        # Should not fail
        index.delete_namespace(namespace=ns)

    info = index.info()

    # default namespace should exist
    assert len(info.namespaces) >= 1
    assert DEFAULT_NAMESPACE in info.namespaces
    assert deleted is not None
    assert deleted not in info.namespaces


@pytest.mark.asyncio
async def test_delete_namespaces_async(async_index: AsyncIndex):
    for ns in NAMESPACES:
        await ensure_ns_exists_async(async_index, ns)

    deleted = None
    for ns in NAMESPACES:
        if ns == DEFAULT_NAMESPACE:
            continue

        deleted = ns
        # Should not fail
        await async_index.delete_namespace(namespace=ns)

    info = await async_index.info()

    # default namespace should exist
    assert len(info.namespaces) >= 1
    assert DEFAULT_NAMESPACE in info.namespaces
    assert deleted is not None
    assert deleted not in info.namespaces


def test_rename_namespace(index: Index):
    try:
        index.delete_namespace("new_name")
    except Exception:
        pass

    ensure_ns_exists(index, "old_name")

    ok = index.rename_namespace("old_name", "new_name")
    assert ok is True

    info = index.info()

    assert "new_name" in info.namespaces
    assert "old_name" not in info.namespaces


def test_rename_namespace_existing(index: Index):
    ensure_ns_exists(index, "old_name")
    ensure_ns_exists(index, "new_name")

    ok = index.rename_namespace("old_name", "new_name", delete_existing=True)
    assert ok is True

    info = index.info()

    assert "new_name" in info.namespaces
    assert "old_name" not in info.namespaces


@pytest.mark.asyncio
async def test_rename_namespace_async(async_index: AsyncIndex):
    try:
        await async_index.delete_namespace("new_name")
    except Exception:
        pass

    await ensure_ns_exists_async(async_index, "old_name")

    ok = await async_index.rename_namespace("old_name", "new_name")
    assert ok is True

    info = await async_index.info()

    assert "new_name" in info.namespaces
    assert "old_name" not in info.namespaces


@pytest.mark.asyncio
async def test_rename_namespace_existing_async(async_index: AsyncIndex):
    await ensure_ns_exists_async(async_index, "old_name")
    await ensure_ns_exists_async(async_index, "new_name")

    ok = await async_index.rename_namespace(
        "old_name", "new_name", delete_existing=True
    )
    assert ok is True

    info = await async_index.info()

    assert "new_name" in info.namespaces
    assert "old_name" not in info.namespaces
