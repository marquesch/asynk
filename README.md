# asynk

asynk is a small asyncio task framework (Python 3.10+, standard library only).
Producers and consumers are decoupled: the core only knows an `Envelope`, a
`Codec` and two protocols, so any broker can sit behind them. An in-memory
transport and a unix socket transport ship for demos and tests, neither meant
for production.

## Example

```python
# tasks.py
import asyncio

from asynk import App, Worker
from asynk.transport.memory import memory_transport

producer, consumer = memory_transport()
app = App("demo", producer=producer, consumer=consumer)


@app.task(max_retries=3)
async def add(a: int, b: int) -> int:
    print(f"{a} + {b} = {a + b}")
    return a + b


async def main() -> None:
    worker = Worker(app)
    run = asyncio.create_task(worker.run())
    await add.delay(2, b=3)  # arguments must be JSON-friendly
    await asyncio.sleep(0.1)
    worker.request_shutdown()
    await run


if __name__ == "__main__":
    asyncio.run(main())
```

Sync tasks (`def`) run in a thread. `delay` is type-checked against the task
signature. With a real broker transport, producers call `await add.delay(...)`
in one process and a worker runs elsewhere:

```
asynk worker tasks:app --concurrency 10 --shutdown-timeout 30 --heartbeat-interval 5
```

The in-memory queue is per process, so `asynk worker` only helps with a
transport whose producer and consumer talk through an external system.

## Running a worker in another process

The unix socket transport connects a separate producer and worker on one
machine, with no broker:

```python
# app.py
from asynk import App
from asynk.transport.unix import unix_transport

producer, consumer = unix_transport("/tmp/asynk.sock")
app = App("demo", producer=producer, consumer=consumer)


@app.task()
async def add(a: int, b: int) -> int:
    print(f"{a} + {b} = {a + b}")
    return a + b
```

```
# terminal 1: the worker creates and listens on the socket
python -m asynk.cli worker app:app

# terminal 2: publish a task
python -c "import asyncio; from app import add; asyncio.run(add.delay(2, 3))"
```

`python -m asynk.cli` is used so the current directory is importable; the
`asynk` command works the same when the module is on `sys.path`.

This transport is for learning and testing only. Messages are held in memory,
nothing is retried, and anything unsettled when the worker stops or crashes is
lost. Publishing fails while no worker is listening. Pass a codec as the second
argument to `unix_transport` to change the wire format (it must not emit
newlines, which delimit messages).

Runnable versions, including a custom codec, are in `examples/memory` and
`examples/unix`.

## Producer and consumer protocols

```python
class Producer(Protocol):
    async def publish(self, envelope: Envelope) -> Any: ...


class Consumer(Protocol):
    @property
    def connected(self) -> bool: ...

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def deliveries(self) -> AsyncIterator[Delivery]: ...
    def process(self, delivery: Delivery) -> AbstractAsyncContextManager[None]: ...


class Delivery(Protocol):
    @property
    def envelope(self) -> Envelope: ...
    @property
    def settled(self) -> bool: ...

    async def ack(self) -> None: ...
    async def reject(self, *, requeue: bool = False) -> None: ...
```

`Codec` (`encode(envelope) -> bytes`, `decode(bytes) -> Envelope`) is the only
thing a producer and a consumer must agree on; `JsonCodec` is the default.
`consumer.process(delivery)` wraps the handling: the transport decides what to
do on success (usually ack) and on an exception (usually reject).

### Consumer process contract

When a task fails, is unknown or exceeds its retries, the failure reaches
`consumer.process()` as a `TaskFailedError` (see `.result.status`); other
exceptions are unexpected errors. `process()` must settle a delivery that is
still unsettled when the block exits, and must never suppress exceptions,
especially `asyncio.CancelledError`. On shutdown cancellation the worker
rejects the unsettled delivery with `requeue=True` before the cancellation
reaches the context manager. `deliveries()` may be cancelled at any await
point (an `aclose()` on the iterator is called if present); when it ends, the
worker drains and `run()` returns. `stop()` runs after the iterator is closed,
and the broker should redeliver anything still unsettled. `connected` drives
readiness. See the docstrings in `asynk.transport` for details.

## Probes

A running worker (given `probe_dir`, which the CLI always sets; it defaults to
`<tempfile.gettempdir()>/asynk`) touches
`liveness` and `readiness` files every heartbeat. Readiness is removed while
the consumer is disconnected or the worker is shutting down. Use a Kubernetes
`exec` probe:

```yaml
livenessProbe:
  exec:
    command: ["asynk", "health", "liveness", "--max-age", "15"]
readinessProbe:
  exec:
    command: ["asynk", "health", "readiness", "--max-age", "15"]
```

`--max-age` should exceed the heartbeat interval (default 5s). Both commands
accept `--probe-dir`; exit code 0 means healthy. `asynk health` must use the
same `--probe-dir` as the worker it checks, and two workers in one container
need distinct directories.

## Retries

The core does not retry. A failing task yields a `TaskResult` with status
`FAILED` (or `RETRIES_EXCEEDED` once `envelope.attempt >= max_retries`), the
`after_task` hook runs, and, if the hook left the delivery unsettled, the
failure reaches `consumer.process()` as a `TaskFailedError` and the consumer's
default settles it (the memory transport rejects without requeue). Retry
mechanics belong to the producer, consumer and hook. For example:

```python
from asynk import Delivery, TaskResult, TaskStatus


@app.after_task
async def retry(result: TaskResult, delivery: Delivery) -> None:
    if result.status is TaskStatus.FAILED:
        # Publish first, then ack: if publishing fails the hook raises, the
        # delivery is still unsettled and gets requeued, so nothing is lost.
        await producer.publish(result.envelope.retry())  # attempt + 1
        await delivery.ack()
```

A hook that raises causes the message to be requeued. The hook above only
retries `FAILED`; `RETRIES_EXCEEDED` falls through to the consumer's default
(reject, i.e. dead-letter). If you also retried `RETRIES_EXCEEDED` here, a
task with `max_retries=None` would retry forever.

## Delivery guarantees and shutdown

Delivery is at-least-once, so tasks should be idempotent. `envelope.id` stays
stable across `retry()`, which makes it usable as an idempotency key.

On shutdown the worker stops fetching and waits up to `shutdown_timeout` for
running tasks. Async tasks still running afterwards are cancelled and their
deliveries requeued. Sync tasks cannot be interrupted: after the timeout their
delivery is requeued but the thread keeps running, so the task may run twice,
and process exit blocks until the thread finishes. A failing `after_task` hook
on a `SUCCESS` result also requeues the message, which runs the task again.

On Kubernetes set `terminationGracePeriodSeconds` above `shutdown_timeout`
plus the longest sync task. The default `shutdown_timeout` of 30 seconds equals
the Kubernetes default grace period of 30 seconds, so raise one of them.
