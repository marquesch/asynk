# asynk

asynk is a small asyncio task framework (Python 3.14+, standard library only).
Producers and consumers are decoupled: the core only knows an `Envelope`, a
`Codec` and two protocols, so any broker can sit behind them. An in-memory
transport ships for demos and tests.

## Example

```python
# tasks.py
import asyncio

from asynk import App, Worker
from asynk.transports.memory import memory_transport

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

## Producer and consumer protocols

```python
class Producer(Protocol):
    async def publish(self, envelope: Envelope) -> Any: ...


class Consumer(Protocol):
    connected: bool  # read-only property

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def deliveries(self) -> AsyncIterator[Delivery]: ...
    def process(self, delivery: Delivery) -> AbstractAsyncContextManager[None]: ...


class Delivery(Protocol):
    envelope: Envelope
    settled: bool

    async def ack(self) -> None: ...
    async def reject(self, *, requeue: bool = False) -> None: ...
```

`Codec` (`encode(envelope) -> bytes`, `decode(bytes) -> Envelope`) is the only
thing a producer and a consumer must agree on; `JsonCodec` is the default.
`consumer.process(delivery)` wraps the handling: the transport decides what to
do on success (usually ack) and on an exception (usually reject).

## Probes

A running worker (given `probe_dir`, which the CLI always sets) touches
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
accept `--probe-dir`; exit code 0 means healthy.

## Retries

The core does not retry. A failing task yields a `TaskResult` with status
`FAILED` (or `RETRIES_EXCEEDED` once `envelope.attempt >= max_retries`), the
delivery is rejected without requeue, and the `after_task` hook runs first.
Retry mechanics belong to the producer, consumer and hook. For example:

```python
from asynk import Delivery, TaskResult, TaskStatus


@app.after_task
async def retry(result: TaskResult, delivery: Delivery) -> None:
    if result.status is TaskStatus.FAILED:
        await delivery.ack()
        await app.producer.publish(result.envelope.retry())  # attempt + 1
```

A hook that raises causes the message to be requeued.
