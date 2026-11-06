# asynk

asynk is a small asyncio task framework (Python 3.11+, standard library only).
Producers and consumers are decoupled: the core only knows an `Envelope`, a
`Codec` and two protocols, so any broker can sit behind them. An in-memory
transport ships for development and testing.

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
    await add.delay(2, b=3)  # arguments are encoded by app.codec
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

## Publishing with metadata

Attach metadata, data carried alongside the call, to a published call with
`with_metadata`. It returns an immutable bound task, so calls can be chained and
later keys override earlier ones:

```python
await add.with_metadata(source="api", trace="x").with_metadata(trace="y").delay(1, b=2)
# envelope.metadata == {"source": "api", "trace": "y"}
```

The transport moves bytes: it carries the message bodies that `app.codec`
produces. To change the wire format, pass `App(..., codec=MyCodec())`;
`memory_transport()` takes no codec.

A runnable version is in `examples/memory/` (a worker and a scheduler in one
process, with a fast virtual clock).

## Scheduled tasks (scheduler)

`@app.scheduled_task` registers a task that `asynk scheduler` publishes on a cron schedule.
The scheduler only publishes; a worker runs the tasks, as for any other message:

```python
@app.scheduled_task(cron="30 8 * * mon-fri", tz="America/Sao_Paulo")
async def morning_report() -> None: ...


@app.scheduled_task(cron="*/5 * * * *", args=["eu"], kwargs={"limit": 100})
def sync(region: str, *, limit: int) -> None: ...
```

```
asynk scheduler tasks:app --heartbeat-interval 5
```

A scheduled task is an ordinary task (it also accepts `name` and `max_retries`) with
fixed `args` and `kwargs`. `tz` defaults to the app's `tz` (`App(name, tz="UTC")`).
Expressions have five fields, with a resolution of one minute; month and weekday
names are accepted, and macros such as `@daily` and a seconds field are not.
When both day-of-month and day-of-week are restricted, a slot matches if either
does (the classic Vixie rule); a field starting with `*` counts as unrestricted.
Nothing fires at startup: the first slot is the first one after the scheduler starts.

The scheduler publishes with the app's producer and runs the app's startup and
shutdown hooks (see [Lifecycle hooks](#lifecycle-hooks)); `runner.role` is `"scheduler"` there. Keep
those hooks quick: the scheduler does not refresh its liveness probe while one
runs, so a hook slower than your probe's staleness limit gets the process killed,
and a slow hook delays shutdown by its full duration. On shutdown (SIGTERM or
SIGINT) an in-flight publish is cancelled. It writes the `liveness` probe file (never `readiness`), so use
the liveness probe from [Probes](#probes) with the same `--probe-dir`. The probe
is refreshed by the scheduler's main loop, so a scheduler stuck in a hung publish (for
example on a half-open connection) stops refreshing it and `asynk health
liveness` fails. That is intended: run the scheduler under a supervisor (such as a
Kubernetes liveness probe) that restarts it.

**Run one scheduler.** Without a store there is no locking or coordination, so a
second scheduler publishes every slot twice. Duplicates are also possible around
restarts and failovers. With a store the scheduler dedupes slots itself (see
[Results and the store](#results-and-the-store)). Each publish carries
`metadata["idempotency_key"]`, which is `"<task name>:<slot as UTC ISO instant>"`,
and `metadata["scheduled_at"]`, the slot as a UTC ISO instant. Without a store, a
middleware layer can dedupe, given a store shared by all workers (a set in
memory only works for one):

```python
from asynk import App, Next, TaskCall, TaskResult

app = App("myapp")  # or the app from the snippets above
seen: set[str] = set()  # use a shared store (a database, Redis) in production


@app.middleware
async def dedupe(call: TaskCall, next: Next) -> TaskResult:
    key = call.envelope.metadata.get("idempotency_key")
    if key is not None:
        if key in seen:
            return call.skip()
        seen.add(str(key))
    return await next(call)
```

There is no guard: every slot is published, and a scheduled task decides for
itself whether there is work to do (a synchronous guard inside the scheduler could
stall the other scheduled tasks).

**Daylight saving time.** A local time that does not exist (the spring-forward
gap) is skipped for that day. A local time that occurs twice fires only on its
first occurrence, so an every-minute schedule does not fire during the repeated
hour.

**Missed runs.** The scheduler keeps no state and never replays the past. A due
slot fires if it is at most `grace` seconds late (default 30 s); an older one
(after a stall) is skipped with a warning and the scheduler resumes at the next
slot. Slots that passed while the scheduler was down are not published. Grace is
set with `Scheduler(app, grace=...)`; `asynk scheduler` uses the default.

**Clock changes.** If the wall clock steps backwards, the scheduler never fires a
slot twice in one process: it waits for the next slot after the last one handled.

Only non-UTC timezones need a time zone database. On systems without one (slim
containers, Windows) install the `tzdata` package; UTC works without it.

## Results and the store

Pass a store to the app to record task states and return values:

```python
# `producer` and `consumer` come from a transport (see below); run this inside an
# async function.
from asynk import App, MemoryStore

app = App("demo", producer=producer, consumer=consumer, store=MemoryStore())

handle = await add.delay(1, b=2)  # a TaskHandle
await handle.state()  # TaskState.PENDING, then RUNNING, ...
await handle.result(timeout=10)  # 3, once the task has finished
await app.get_task(handle.task_id).result(timeout=10)  # the same, from just the id
```

`delay` returns a `TaskHandle` with `task_id`, `receipt` (whatever the producer
returned), `state()` and `result(timeout=None)`. `app.get_task(task_id)` builds a
handle from an id, for example in another request or process sharing the store.
`result(timeout=...)` raises `TimeoutError` when the task is not done in time, and
`TaskNotFoundError` for an id the store has no record of.

| State | Meaning |
|---|---|
| `PENDING` | published, not started yet |
| `RUNNING` | a worker is running it |
| `RETRYING` | an attempt failed and a retry was published |
| `SUCCESS` | finished; `result()` returns the value |
| `FAILURE` | failed for good; `result()` raises `TaskError` |
| `SKIPPED` | a middleware layer vetoed it with `call.skip()`; `result()` returns `None` |
| `UNKNOWN` | the store has no record for this id |

A failed task makes `result()` raise `TaskError`, which carries the failure's type
name and message as `.type_name` and `.message`.

**Store lifecycle.** The app starts its store in its `before_startup` hooks and
stops it in its `after_shutdown` hooks, so a worker and a scheduler need nothing
extra. A producer-only script starts and stops the store itself:

```python
await app.store.start()
handle = await add.delay(1, b=2)
print(await handle.result(timeout=10))
await app.store.stop()
```

**Stores.**

- `MemoryStore(ttl=86400.0)` keeps records in one process for `ttl` seconds, so the
  producer, worker and readers share it. It is the development and testing store.

For anything wider, implement the `Store` protocol over Redis or a database:

```python
class Store(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def get(self, key: str) -> bytes | None: ...
    async def set(self, key: str, value: bytes, *, overwrite: bool = False) -> bool: ...
    async def wait(self, key: str, timeout: float | None = None) -> bytes | None: ...
    async def delete(self, key: str) -> bool: ...
```

`set` returns `False` when the key exists and `overwrite` is false; `wait` returns
the value once it is written, or `None` when `timeout` elapses; `delete` returns
whether the key existed and was removed.

**Codec.** Task results and states are encoded with `app.codec`, the same codec
that encodes messages (`JsonCodec()` by default). Pass `App(..., codec=MyCodec())`
to use another format, for example pickled values that keep tuples.

**Scheduler dedupe.** Schedulers that share a store claim each slot before
publishing it, so overlapping schedulers publish each slot once. Give them one
shared store; `MemoryStore` covers one process, and several machines need a network
store. A scheduler releases its claim when its publish fails or is cancelled, so
another scheduler can publish that slot.

See [CHANGELOG.md](CHANGELOG.md) for breaking changes.

## Producer and consumer protocols

```python
class Producer(Protocol):
    async def publish(self, envelope: Envelope, body: bytes) -> Any: ...


class Consumer(Protocol):
    @property
    def connected(self) -> bool: ...

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def deliveries(self) -> AsyncIterator[Delivery]: ...
    def process(self, delivery: Delivery) -> AbstractAsyncContextManager[None]: ...


class Delivery(Protocol):
    @property
    def body(self) -> bytes: ...
    @property
    def settled(self) -> bool: ...

    async def ack(self) -> None: ...
    async def reject(self, *, requeue: bool = False) -> None: ...
```

`Codec` has three methods: `validate(value)`, `encode(value) -> bytes` and
`decode(data) -> object`. `App(codec=...)` is the single codec for messages and
results; `JsonCodec` is the default, and `EncodeError` and `DecodeError` report
values the codec cannot encode or bytes it cannot decode. A producer sends the
`body` it is given, and `publish(envelope, body)` receives the envelope for
routing and its id. The app builds the body with
`app.codec.encode(envelope.to_dict())`, and the worker rebuilds the envelope from
`Delivery.body` with `Envelope.from_dict(app.codec.decode(body))`. The scheduler
calls `app.codec.validate` on each scheduled task's arguments when it is created.
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

## Lifecycle hooks

Hooks extend a worker or scheduler without subclassing it. Each decorator on `App` appends to
an ordered list, and hooks run in registration order. Hooks may be `def` or
`async def`, and sync hooks run on the event loop, so keep them cheap.
Registering the same hook twice is a no-op; hooks that merely share a name, such as
lambdas or functions made by a factory, are all registered.

| Decorator | Runs | Receives |
|---|---|---|
| `before_startup` | before the consumer starts (worker) or the loop starts (scheduler) | `Runner` |
| `after_startup` | once the consumer has started (worker) or the loop is about to start (scheduler) | `Runner` |
| `before_shutdown` | when the worker stops fetching, before draining; when the scheduler stops | `Runner` |
| `after_shutdown` | last: after draining with the consumer stopped (worker); after the loop ended (scheduler) | `Runner` |

The lifecycle hooks run in every asynk process, `Worker` and `Scheduler`, and
receive a `Runner`. `runner.role` is `"worker"` or `"scheduler"`, so a hook can
branch where the processes differ. A hook annotated `Worker` is rejected by mypy,
since it would not accept a scheduler; annotate it `Runner`.

```python
from asynk import Runner


@app.before_startup
async def startup(runner: Runner) -> None:
    sentry_sdk.init()  # telemetry in every process: workers and the scheduler
    if runner.role == "worker":
        await database.connect()  # only workers need the database
```

Hooks fail loudly. A startup or shutdown hook that raises stops the remaining
hooks of that event and makes `Worker.run()` or `Scheduler.run()` raise; the
process still drains tasks (worker) and cleans up first.

## Middleware

Middleware wraps the execution of each task in the worker. A layer is an
`async def` taking a `TaskCall` (its `envelope` and `delivery`) and `next`, the
rest of the chain, and returning a `TaskResult`:

```python
import logging

from asynk import Next, TaskCall, TaskResult

logger = logging.getLogger(__name__)


@app.middleware
async def timing(call: TaskCall, next: Next) -> TaskResult:
    logger.info("starting %s", call.envelope.task)
    try:
        return await next(call)
    finally:
        logger.info("finished %s", call.envelope.task)
```

**The last registered layer is the outermost**: it runs first on the way in and
last on the way out. With layers `a` then `b` registered, `b` runs first, then
`a`, then the task. Registering the same layer twice is a no-op, like hooks.
Layers registered after the worker started apply from the next message.

`await next(call)` returns the `TaskResult` of the task (or of the layers inside),
and a layer may return it unchanged or return another. The worker records and
settles whatever the chain returns. A `try`/`finally` or `async with` in a layer
also runs when the worker cancels a running task on shutdown.

A layer vetoes a task by returning `call.skip()` without awaiting `next`: the
task does not run, the result has status `SKIPPED`, and the delivery is acked.
The chain also runs for unknown tasks, where the layers receive an
`UNKNOWN_TASK` result. A layer may settle `call.delivery` itself, and then the
worker leaves the delivery alone.

If a layer raises before the task ran, the error is logged and the message is
redelivered: the worker publishes `envelope.retry()` through `app.producer` and
acks the original, so the failure counts towards `max_retries`. Once
`envelope.attempt >= max_retries` the message is rejected instead. Without a
producer the worker falls back to `requeue=True`, which redelivers the message
but does not increment `attempt`. If a layer raises after the task ran, the
error is logged and the task's outcome stands: the message is not redelivered.

## Retries

The core does not retry. A failing task yields a `TaskResult` with status
`FAILED` (or `RETRIES_EXCEEDED` once `envelope.attempt >= max_retries`), the
middleware chain returns it, and, if no layer settled the delivery, the
failure reaches `consumer.process()` as a `TaskFailedError` and the consumer's
default settles it (the memory transport rejects without requeue). Retry
mechanics for failing tasks belong to the producer, consumer and your middleware.
(Failures of a layer itself are the exception; see [Middleware](#middleware).) For
example:

```python
from asynk import Next, TaskCall, TaskResult, TaskStatus


@app.middleware
async def retry(call: TaskCall, next: Next) -> TaskResult:
    result = await next(call)
    if result.status is TaskStatus.FAILED:
        # Publish first, then ack, so a crash in between duplicates instead of loses.
        next_envelope = result.envelope.retry()  # attempt + 1
        await producer.publish(next_envelope, app.codec.encode(next_envelope.to_dict()))
        await call.delivery.ack()
    return result
```

The layer above only retries `FAILED`; `RETRIES_EXCEEDED` falls through to the consumer's default
(reject, i.e. dead-letter). If you also retried `RETRIES_EXCEEDED` here, a
task with `max_retries=None` would retry forever.

## Delivery guarantees and shutdown

Delivery is at-least-once, so tasks should be idempotent. `envelope.id` stays
stable across `retry()`, which makes it usable as an idempotency key.

On shutdown the worker stops fetching and waits up to `shutdown_timeout` for
running tasks. Async tasks still running afterwards are cancelled and their
deliveries requeued. Sync tasks cannot be interrupted: after the timeout their
delivery is requeued but the thread keeps running, so the task may run twice,
and process exit blocks until the thread finishes.

On Kubernetes set `terminationGracePeriodSeconds` above `shutdown_timeout`
plus the longest sync task. The default `shutdown_timeout` of 30 seconds equals
the Kubernetes default grace period of 30 seconds, so raise one of them.
