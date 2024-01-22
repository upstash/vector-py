import time


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
            time.sleep(retry_delay)

    if last_err is None:
        raise AssertionError("Couldn't run the assertion")

    raise last_err
