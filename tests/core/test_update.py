import pytest

from tests import NAMESPACES
from upstash_vector import Index, AsyncIndex
from upstash_vector.errors import ClientError


@pytest.mark.parametrize("ns", NAMESPACES)
def test_update_vector(index: Index, ns: str):
    index.upsert(
        [("id-0", [0.1, 0.2])],
        namespace=ns,
    )

    res = index.fetch("id-0", include_vectors=True, namespace=ns)
    assert len(res) == 1
    assert res[0] is not None
    assert res[0].vector == [0.1, 0.2]

    updated = index.update("id-0", vector=[0.2, 0.3], namespace=ns)
    assert updated is True

    res = index.fetch("id-0", include_vectors=True, namespace=ns)
    assert len(res) == 1
    assert res[0] is not None
    assert res[0].vector == [0.2, 0.3]


@pytest.mark.parametrize("ns", NAMESPACES)
def test_update_data(embedding_index: Index, ns: str):
    embedding_index.upsert(
        [("id-0", "hello")],
        namespace=ns,
    )

    res = embedding_index.fetch("id-0", include_vectors=True, namespace=ns)
    assert len(res) == 1
    assert res[0] is not None

    old_vector = res[0].vector

    updated = embedding_index.update("id-0", data="bye", namespace=ns)
    assert updated is True

    res = embedding_index.fetch("id-0", include_vectors=True, namespace=ns)
    assert len(res) == 1
    assert res[0] is not None
    assert res[0].vector != old_vector


@pytest.mark.parametrize("ns", NAMESPACES)
def test_update_metadata(index: Index, ns: str):
    index.upsert(
        [("id-0", [0.1, 0.2], {"field": "value"})],
        namespace=ns,
    )

    res = index.fetch("id-0", include_metadata=True, namespace=ns)
    assert len(res) == 1
    assert res[0] is not None
    assert res[0].metadata == {"field": "value"}

    updated = index.update("id-0", metadata={"new_field": "new_value"}, namespace=ns)
    assert updated is True

    res = index.fetch("id-0", include_metadata=True, namespace=ns)
    assert len(res) == 1
    assert res[0] is not None
    assert res[0].metadata == {"new_field": "new_value"}


@pytest.mark.parametrize("ns", NAMESPACES)
def test_update_non_existing_id(index: Index, ns: str):
    updated = index.update("id-999", vector=[0.4, 0.5], namespace=ns)
    assert updated is False


def test_update_too_many_params(index: Index):
    with pytest.raises(ClientError):
        index.update("id-0", vector=[0.2, 0.3], metadata={"new_field": "new_value"})


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_update_vector_async(async_index: AsyncIndex, ns: str):
    await async_index.upsert(
        [("id-0", [0.1, 0.2])],
        namespace=ns,
    )

    res = await async_index.fetch("id-0", include_vectors=True, namespace=ns)
    assert len(res) == 1
    assert res[0] is not None
    assert res[0].vector == [0.1, 0.2]

    updated = await async_index.update("id-0", vector=[0.2, 0.3], namespace=ns)
    assert updated is True

    res = await async_index.fetch("id-0", include_vectors=True, namespace=ns)
    assert len(res) == 1
    assert res[0] is not None
    assert res[0].vector == [0.2, 0.3]


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_update_data_async(async_embedding_index: AsyncIndex, ns: str):
    await async_embedding_index.upsert(
        [("id-0", "hello")],
        namespace=ns,
    )

    res = await async_embedding_index.fetch("id-0", include_vectors=True, namespace=ns)
    assert len(res) == 1
    assert res[0] is not None

    old_vector = res[0].vector

    updated = await async_embedding_index.update("id-0", data="bye", namespace=ns)
    assert updated is True

    res = await async_embedding_index.fetch("id-0", include_vectors=True, namespace=ns)
    assert len(res) == 1
    assert res[0] is not None
    assert res[0].vector != old_vector


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_update_metadata_async(async_index: AsyncIndex, ns: str):
    await async_index.upsert(
        [("id-0", [0.1, 0.2], {"field": "value"})],
        namespace=ns,
    )

    res = await async_index.fetch("id-0", include_metadata=True, namespace=ns)
    assert len(res) == 1
    assert res[0] is not None
    assert res[0].metadata == {"field": "value"}

    updated = await async_index.update(
        "id-0", metadata={"new_field": "new_value"}, namespace=ns
    )
    assert updated is True

    res = await async_index.fetch("id-0", include_metadata=True, namespace=ns)
    assert len(res) == 1
    assert res[0] is not None
    assert res[0].metadata == {"new_field": "new_value"}


@pytest.mark.asyncio
@pytest.mark.parametrize("ns", NAMESPACES)
async def test_update_non_existing_id_async(async_index: AsyncIndex, ns: str):
    updated = await async_index.update("id-999", vector=[0.4, 0.5], namespace=ns)
    assert updated is False


@pytest.mark.asyncio
async def test_update_too_many_params_async(async_index: AsyncIndex):
    with pytest.raises(ClientError):
        await async_index.update(
            "id-0", vector=[0.2, 0.3], metadata={"new_field": "new_value"}
        )
