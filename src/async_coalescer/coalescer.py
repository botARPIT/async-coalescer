import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")

class RequestCoelescer:
    def __init__(self) -> None:
        self._in_flight: dict[str, asyncio.Task[T]] = {}

    async def get(
            self,
            key: str,
            operation: Callable[[], Awaitable[T]],
    ) -> T:
        task = self._in_flight.get(key)

        if task is None:
            task = asyncio.create_task(
                self._run(key, operation),
                  name="worker_task"
                )
            self._in_flight[key] = task

        return await task


    async def _run(
            self,
            key: str,
            operation: Callable[[], Awaitable[T]]
    ) -> T:
        current = asyncio.current_task()
        try:
            return await operation()
            
        finally:
            if self._in_flight.get(key) is current:
                del self._in_flight[key]
