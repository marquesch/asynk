# asynk

asynk is a lightweight asyncio task framework for Python 3.11+, built on the
standard library only. Define tasks with a decorator, publish them with
`delay`, and run them in workers, behind any broker you plug in.

**Documentation:** <https://marquesch.github.io/asynk/>

## Install

```bash
pip install asynk
```

## Example

```python
"""The smallest asynk app: one task, published and run in the same process."""

import asyncio

from asynk import App, MemoryStore, Worker
from asynk.transport.memory import memory_transport

producer, consumer = memory_transport()
app = App("first-app", producer=producer, consumer=consumer, store=MemoryStore())


@app.task()
async def add(a: int, b: int) -> int:
    return a + b


async def main() -> None:
    worker = Worker(app)
    run = asyncio.create_task(worker.run())

    handle = await add.delay(2, b=3)
    print("2 + 3 =", await handle.result(timeout=5))

    worker.request_shutdown()
    await run


if __name__ == "__main__":
    asyncio.run(main())
```

With a broker transport, producers and workers run in separate processes; start
a worker with `asynk worker tasks:app`.

## Why asynk

- **Decoupled producers and consumers**: the core only knows an envelope, a
  codec and two small protocols, so any broker can sit behind them.
- **Pluggable transport and store**: bring your own broker and result store; an
  in-memory transport and store ship for development and tests, and a
  [Redis store](https://marquesch.github.io/asynk/guides/redis/) and a
  [Redis transport](https://marquesch.github.io/asynk/guides/redis-transport/)
  come with `asynk[redis]`.
- **Type-checked `delay`**: calls are checked against the task's signature.
- **Cron scheduled tasks** with per-slot dedupe when schedulers share a store.
- **Task results** through a store, with states and awaitable handles.
- **Retries with backoff**: a retry policy per app or per task, with exponential
  backoff and an exception filter.
- **Lifecycle hooks** shared by workers and the scheduler.
- **Middleware** around every task: timing, metrics, vetoes, cleanup.
- **Health probes and graceful shutdown**, ready for Kubernetes.

## Learn more

- [Getting started](https://marquesch.github.io/asynk/getting-started/)
- [Concepts](https://marquesch.github.io/asynk/concepts/)
- [Guides](https://marquesch.github.io/asynk/guides/scheduled-tasks/) and
  [advanced topics](https://marquesch.github.io/asynk/advanced/lifecycle-hooks/)
- [API reference](https://marquesch.github.io/asynk/reference/)
- [Changelog](CHANGELOG.md)

## License

MIT, see [LICENSE](LICENSE).
