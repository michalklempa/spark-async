# spark-async

Run an `async` function on every row of a PySpark partition, with a bounded number of calls in flight. Spark has no built-in equivalent of Flink's async I/O operator, so this library wraps `mapPartitions` around an asyncio event loop.

You implement `AsyncMapper`:

- `open()` runs once per partition, before any row is mapped. Create clients and connections here.
- `map(row)` runs once per row, concurrently, with at most `max_in_flight` calls running at the same time.
- `close()` runs once per partition after the last row, also when a `map()` call fails. It does nothing by default.

If `open()` raises, `close()` is not called, so an `open()` that acquires several resources must release what it already holds before it raises.

Results are returned in completion order, not input order.

## How it works

For each partition, `map_partitions_async` starts `max_in_flight` worker tasks on a private asyncio event loop. The workers share the partition's input iterator, so a row is read only when a worker is free to process it. Each worker awaits `mapper.map(row)` and puts the result on a bounded queue of size `max_in_flight`.

Spark pulls results from a normal Python generator. Each pull runs the event loop until at least one result exists, hands it over, and then hands over every other result that is already finished, without waiting for more. A full queue blocks the workers, so a slow consumer slows the input reads too.

Nothing is collected per partition. At most `max_in_flight` rows are being mapped, and at most `max_in_flight` finished results are waiting in the queue. The event loop runs only while Spark is waiting for the next result, so requests make no progress while Spark is busy with a row it has just received.

If a `map()` call raises, the remaining calls are cancelled, `close()` runs, and the exception reaches Spark as a task failure. Handle errors you want to keep as data inside `map()`.

## Requirements

- Python 3.13 or newer
- PySpark 4.2 or newer
- [uv](https://docs.astral.sh/uv/)

Install uv:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Use it in your own project

From GitHub:

```bash
uv add "spark-async @ git+https://github.com/michalklempa/spark-async"
```

From a local checkout:

```bash
uv add --editable /path/to/spark-async
```

From a built wheel:

```bash
uv add /path/to/spark_async-0.1.0-py3-none-any.whl
```

## Example

```python
import asyncio

import httpx
from pyspark.sql import SparkSession
from spark_async import AsyncMapper, map_partitions_async


class Lookup(AsyncMapper[dict, dict]):
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    async def open(self) -> None:
        self.client = httpx.AsyncClient(base_url=self.base_url)

    async def map(self, row: dict) -> dict:
        response = await self.client.get("/items", params={"id": row["id"]})
        return {**row, "status": response.status_code}

    async def close(self) -> None:
        await self.client.aclose()


spark = SparkSession.builder.master("local[*]").getOrCreate()
rdd = spark.sparkContext.parallelize([{"id": i} for i in range(1000)], 4)
enriched = map_partitions_async(rdd, Lookup("http://localhost:4040"), max_in_flight=50)
print(enriched.take(5))
```

`map_partitions_async(rdd, mapper, max_in_flight=100)` returns an ordinary RDD. To get a DataFrame, call `spark.createDataFrame(enriched, schema)`.

The mapper object is pickled and sent to the executors, so keep its constructor arguments picklable and create clients, connections and locks in `open()`.

## Run it on a Spark cluster

The executors' Python processes must be able to import `spark_async` and the module that defines your mapper class. The library itself depends only on PySpark.

- Install the package in the Python environment of every worker node, or
- ship a packed virtual environment with `spark-submit --archives` and point `PYSPARK_PYTHON` at it.

In local mode, make sure the workers use the same interpreter as the driver:

```python
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
```

## Develop

```bash
git clone https://github.com/michalklempa/spark-async
cd spark-async
uv sync
```

## Build a package

```bash
uv build
```

This writes a wheel and a source archive to `dist/`.

## Publish to PyPI (optional)

Publishing is not required: users can install straight from GitHub, a local path or a wheel. Publishing to PyPI makes `uv add spark-async` work.

1. Create an account on [pypi.org](https://pypi.org) and an API token, or configure a trusted publisher for your GitHub repository.
2. Try it first on TestPyPI:

   ```bash
   uv build
   uv publish --publish-url https://test.pypi.org/legacy/ --token <testpypi-token>
   ```

3. Publish for real:

   ```bash
   uv publish --token <pypi-token>
   ```

A version number can be published only once. Bump `version` in `pyproject.toml` for every release.

## License

MIT, see [LICENSE](LICENSE).
