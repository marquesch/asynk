# Middleware

Middleware wraps every task a worker runs. [Lifecycle hooks](lifecycle-hooks.md)
are events: they run once when a worker or scheduler starts or stops. A
middleware layer is a wrapper: it runs around each task, sees the message
before the task runs and the result after it, and can change or veto the
outcome. Use it for timing, metrics, tracing, vetoes and per-task cleanup.

Middleware runs in workers only; a scheduler runs no tasks.

## A minimal layer

A layer is an `async def` that takes the call and `next`, and returns a
`TaskResult`:

```python
async def layer(call: TaskCall, next: Next) -> TaskResult: ...
```

`call.envelope` is the message being handled and `call.delivery` the delivery
it arrived on. `await next(call)` runs the rest of the chain and the task, and
returns its result. Register a layer with `@app.middleware`:

``` { .python }
--8<-- "examples/advanced/middleware_minimal.py"
```

The layer usually returns the result it got from `next`. It may also return
another one, for example a `TaskResult` with status `SUCCESS` and a fallback
value, which the worker then records as the task's result.

## Ordering

The last registered layer is the outermost: it runs first on the way in and
last on the way out. The first registered layer sits closest to the task.

``` { .python }
--8<-- "examples/advanced/middleware_order.py"
```

This prints:

```text
second: in
first: in
task
first: out
second: out
```

The chain is built for each delivery, so a layer registered after the worker
was created still applies. Registering the same layer twice has no effect.

## Vetoing a task

To keep a task from running, return `call.skip()` without calling `next`. The
task is recorded as `SKIPPED`, `result()` returns `None`, and the delivery is
settled like a success, so the message is not redelivered.

``` { .python }
--8<-- "examples/advanced/middleware_veto.py"
```

## Cleaning up

Put cleanup in a `try`/`finally` around `next`, so it runs whatever happens
inside:

```python
@app.middleware
async def with_connection(call: TaskCall, next: Next) -> TaskResult:
    connection = await pool.acquire()
    try:
        return await next(call)
    finally:
        await pool.release(connection)
```

This includes shutdown. When a task is still running at the end of
`shutdown_timeout`, the worker cancels it; the cancellation unwinds every
layer, running their `finally` blocks, and the delivery is rejected with
requeue so the broker redelivers it. Let `asyncio.CancelledError` propagate:
do not swallow it in a layer.

## When a layer raises

What happens depends on whether the task already ran.

- **Before the task ran** (the layer raised before `next`, or a layer further
  in did): the error is logged and the message is retried like a failing task
  would be. The worker publishes `envelope.retry()`, with `attempt`
  incremented, and acks the original delivery; once `attempt` reaches the
  task's `max_retries` the delivery is rejected and the result is
  `RETRIES_EXCEEDED`. An app without a producer rejects the delivery with
  requeue, keeping the same attempt.
- **After the task ran** (the layer raised after `next` returned): the error is
  logged and the task's own outcome stands. It is recorded and settled as if
  the layer had returned it, and the layer's error causes no redelivery.

With `max_retries=None`, a layer that always raises before the task redelivers
the message forever, with no backoff.

## Settling the delivery yourself

A layer may settle the delivery itself with `call.delivery.ack()` or
`call.delivery.reject()`. The worker sees that the delivery is settled and does
not settle it again. For example, a layer can republish failed tasks for a
retry and ack the original:

```python
@app.middleware
async def retry_failures(call: TaskCall, next: Next) -> TaskResult:
    result = await next(call)
    if result.status is TaskStatus.FAILED:
        retried = call.envelope.retry()
        await app.producer.publish(retried, app.codec.encode(retried.to_dict()))
        await call.delivery.ack()
    return result
```

A layer that settles the delivery and then raises is only logged: the message
is not redelivered.

## Pitfalls

- **Forgetting `next` skips the task.** A layer that returns without awaiting
  `next(call)` vetoes the task, whether it meant to or not.
- **Calling `next` twice runs the task twice.** Each call runs the rest of the
  chain and the task again.
- **Sync layers fail type-checking only.** asynk does not check a layer when
  it is registered; mypy reports a plain `def`. At runtime, a plain `def` that
  returns a `TaskResult` raises `TypeError` when the worker awaits it, which is
  handled like a layer that raised before the task ran.
- **Layers registered later wrap earlier ones.** A layer meant to see
  everything, such as metrics, must be registered last, after the layers that
  veto or replace results.
- **Unknown tasks reach the layers.** A message naming a task the app has not
  registered still runs through the chain, and `next` returns a result with
  status `UNKNOWN_TASK`. `call.envelope.task` is then whatever name the
  producer sent.

A message that cannot be decoded into an envelope runs no layer: it is logged
and rejected without requeue.

## Timing and counting tasks (Prometheus)

A metrics layer times each task with `time.perf_counter()` and counts runs per
task and status. Here the numbers go into a small class, and a veto layer
shows that the metrics layer, registered last, also sees skipped tasks:

``` { .python }
--8<-- "examples/advanced/middleware_metrics.py"
```

It prints:

```text
add success: 2
cleanup skipped: 1
divide retries_exceeded: 1
```

followed by the total seconds per task.

The same layer with `prometheus_client` exposes a counter labelled by task and
status and a histogram of durations labelled by task, served on
`http://localhost:8000/metrics`. It needs `pip install prometheus-client`:

```python
import time

from prometheus_client import Counter, Histogram, start_http_server

from asynk import Next, Runner, TaskCall, TaskResult, TaskStatus

TASKS = Counter("asynk_tasks_total", "Tasks handled, by task and status.", ["task", "status"])
DURATION = Histogram("asynk_task_duration_seconds", "Task duration in seconds.", ["task"])


@app.before_startup
def serve_metrics(runner: Runner) -> None:
    if runner.role == "worker":
        start_http_server(8000)


@app.middleware
async def measure(call: TaskCall, next: Next) -> TaskResult:
    started = time.perf_counter()
    result = await next(call)
    task = "unknown" if result.status is TaskStatus.UNKNOWN_TASK else call.envelope.task
    TASKS.labels(task, result.status.value).inc()
    DURATION.labels(task).observe(time.perf_counter() - started)
    return result
```

Keep labels low-cardinality: every distinct label value is a new time series.
Task name and status are bounded; task arguments or ids are not. The name of
an unknown task comes from the message, so the layer above reports all
unknown tasks under one `unknown` label.
