import asyncio
from collections.abc import Iterable, Iterator

from pyspark import RDD

from spark_async.mapper import AsyncMapper


async def _worker[T, R](mapper: AsyncMapper[T, R], source: Iterator[T], results: asyncio.Queue[R]) -> None:
    for row in source:
        await results.put(await mapper.map(row))


def _ready[R](results: asyncio.Queue[R]) -> Iterator[R]:
    try:
        while True:
            yield results.get_nowait()
    except asyncio.QueueEmpty:
        pass


async def _cancel(tasks: Iterable[asyncio.Future]) -> None:
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def _process_partition[T, R](mapper: AsyncMapper[T, R], rows: Iterable[T], max_in_flight: int) -> Iterator[R]:
    with asyncio.Runner() as runner:
        loop = runner.get_loop()
        runner.run(mapper.open())
        source = iter(rows)
        results: asyncio.Queue[R] = asyncio.Queue(max_in_flight)
        workers = [loop.create_task(_worker(mapper, source, results)) for _ in range(max_in_flight)]
        done = asyncio.gather(*workers)
        done.add_done_callback(lambda _: results.shutdown())
        try:
            while True:
                yield loop.run_until_complete(results.get())
                yield from _ready(results)
        except asyncio.QueueShutDown:
            loop.run_until_complete(done)
        finally:
            runner.run(_cancel([done, *workers]))
            runner.run(mapper.close())


def map_partitions_async[T, R](rdd: RDD[T], mapper: AsyncMapper[T, R], max_in_flight: int = 100) -> RDD[R]:
    return rdd.mapPartitions(lambda rows: _process_partition(mapper, rows, max_in_flight))
