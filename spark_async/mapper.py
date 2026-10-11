from abc import ABC, abstractmethod


class AsyncMapper[T, R](ABC):
    async def open(self) -> None:
        """Acquire per-partition resources before any row is mapped.

        If this raises, close() is not called. An implementation that acquires
        more than one resource must release whatever it already acquired
        before letting the exception propagate.
        """
        pass

    @abstractmethod
    async def map(self, row: T) -> R: ...

    async def close(self) -> None:
        pass
