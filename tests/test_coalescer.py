import asyncio
from src.async_coalescer import RequestCoelescer

async def test_concurrent_request_share_one_operation():
    coelescer = RequestCoelescer()

    operation_count = 0

    async def fetch_user():
        nonlocal operation_count
        operation_count += 1
        await asyncio.sleep(0.1)
        return {"id": 42, "name": "Alice"}

    results = await asyncio.gather(
        coelescer.get("user:42", fetch_user),
        coelescer.get("user:42", fetch_user),
        coelescer.get("user:42", fetch_user)
    )

    assert operation_count == 1
    assert results == [
        {"id": 42, "name": "Alice"},
        {"id": 42, "name": "Alice"},
        {"id": 42, "name": "Alice"}
    ]

async def test_completed_operation_is_removed():
    coalescer = RequestCoelescer()

    async def operation():
        return 42

    result = await coalescer.get("answer", operation)

    assert result == 42
    assert "answer" not in coalescer._in_flight

async def test_failed_operation_is_cleaned_up():
    coalescer = RequestCoelescer()
    operation_count = 0

    async def operation():
        nonlocal operation_count
        operation_count += 1
        raise RuntimeError("boom")

    results = await asyncio.gather(
    coalescer.get("key", operation),
    coalescer.get("key", operation),
    return_exceptions=True,
    )

    assert operation_count == 1

    assert isinstance(results[0], RuntimeError)
    assert isinstance(results[1], RuntimeError)

    assert str(results[0]) == "boom"
    assert str(results[1]) == "boom"

    assert "key" not in coalescer._in_flight

async def test_new_request_reruns_operation():
    coalescer = RequestCoelescer()

    operation_count = 0
    async def operation():
        nonlocal operation_count
        operation_count += 1
        raise RuntimeError("not working")

    result1 = await asyncio.gather(
        coalescer.get("user-1", operation),
        return_exceptions=True
    )

    result2 = await asyncio.gather(
        coalescer.get("user-1", operation),
        return_exceptions=True
    )

    assert operation_count == 2

    assert isinstance(result1[0], RuntimeError)
    assert isinstance(result2[0], RuntimeError)

    assert "user-1" not in coalescer._in_flight

async def test_cancellation_of_one_waiter():
    coalescer = RequestCoelescer()

    started = asyncio.Event()
    release = asyncio.Event()

    async def operation():
        started.set()
        await release.wait()
        return 42

    task_a = asyncio.create_task(
        coalescer.get("key", operation)
    )

    task_b = asyncio.create_task(
        coalescer.get("key", operation)
    )

    await started.wait()

    shared_task = coalescer._in_flight["key"]

    task_a.cancel()
    release.set()

    resultb = await task_b
    try:
        await task_a
    except asyncio.CancelledError:
        pass

    # What do you expect here?
    print("shared:", shared_task)
    print("B:", task_b)

    assert resultb == 42
    assert shared_task.done()
    assert not shared_task.cancelled()