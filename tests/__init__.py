import time
import asyncio

from upstash_vector import Index, AsyncIndex
from upstash_vector.core.index_operations import DEFAULT_NAMESPACE

NAMESPACES = [DEFAULT_NAMESPACE, "ns"]


def assert_eventually(assertion, retry_delay=0.5, timeout=5.0):
    deadline = time.time() + timeout
    last_err = None

    while time.time() < deadline:
        try:
            assertion()
            return
        except AssertionError as e:
            last_err = e
            time.sleep(retry_delay)

    if last_err is None:
        raise AssertionError("Couldn't run the assertion")

    raise last_err


async def assert_eventually_async(assertion, retry_delay=0.5, timeout=5.0):
    deadline = time.time() + timeout
    last_err = None

    while time.time() < deadline:
        try:
            await assertion()
            return
        except AssertionError as e:
            last_err = e
            await asyncio.sleep(retry_delay)

    if last_err is None:
        raise AssertionError("Couldn't run the assertion")

    raise last_err


def ensure_ns_exists(index: Index, ns: str):
    """
    Ensures the given namespace exists in the index by upserting some
    random vector into it, and calling reset.

    No need to call this method, if you are upserting some vector/data.
    """
    if ns == DEFAULT_NAMESPACE:
        return

    index.upsert(
        vectors=[("0", [0.1, 0.1])],
        namespace=ns,
    )

    def assertion():
        info = index.info()
        assert info.namespaces[ns].pending_vector_count == 0

    assert_eventually(assertion)

    index.reset(namespace=ns)


async def ensure_ns_exists_async(index: AsyncIndex, ns: str):
    """
    Ensures the given namespace exists in the index by upserting some
    random vector into it, and calling reset.

    No need to call this method, if you are upserting some vector/data.
    """
    if ns == DEFAULT_NAMESPACE:
        return

    await index.upsert(
        vectors=[("0", [0.1, 0.1])],
        namespace=ns,
    )

    async def assertion():
        info = await index.info()
        assert info.namespaces[ns].pending_vector_count == 0

    await assert_eventually_async(assertion)

    await index.reset(namespace=ns)
