# Retries

A failing task is retried only when it has a retry policy. Without one, a
failure is final. When a failing task has retries left, the worker records the
state `RETRYING`, publishes `envelope.retry()` (a copy with the same `id` and
`attempt` incremented) after the policy's backoff, and acks the original
delivery. If the retry cannot be published, the worker rejects the delivery with
requeue instead; see [Delays and transports](#delays-and-transports).

``` { .python }
--8<-- "examples/guides/retries.py"
```

It prints:

```text
fetch: page after 3 attempts
charge failed: ValueError - card declined
charge state: failure
```

`fetch` fails twice and succeeds on its third attempt, after waiting 0.01 s and
then 0.02 s. `charge` raises an error outside its policy's `retry_on`, so it
fails at once.

## Retry policies

A `RetryPolicy` has three fields:

- `max_retries=1`: how many times a failed task is retried. It counts retries,
  not attempts, so `1` means two attempts in all. `0` never retries, and `None`
  retries without limit; you have to write `None` yourself.
- `backoff=None`: how long to wait before each retry; see [Backoff](#backoff).
- `retry_on=(Exception,)`: the exception types that are retried; see
  [Which errors are retried](#which-errors-are-retried).

Retries are set through `TaskOptions`, a frozen dataclass whose `retry` field is
a `RetryPolicy`, `False` or `None`. Set a default for every task of an app with
`App(options=TaskOptions(retry=...))`, and options for one task with
`@app.task(options=...)` or `@app.scheduled_task(options=...)`:

```python
app = App(
    "shop",
    producer=producer,
    consumer=consumer,
    options=TaskOptions(retry=RetryPolicy(max_retries=3)),
)


@app.task(options=TaskOptions(retry=RetryPolicy(max_retries=5, backoff=10)))
async def sync_stock() -> None: ...


@app.task(options=TaskOptions(retry=False))
async def send_receipt(order_id: str) -> None: ...
```

- A task's policy replaces the app's entirely. Fields it leaves out take the
  `RetryPolicy` defaults, not the app's values: `RetryPolicy(backoff=10)` on a
  task means one retry, whatever the app sets.
- `TaskOptions(retry=False)` turns retries off for one task.
- A task whose options leave `retry` as `None` uses the app's policy, as it is
  when the task is defined. An app without a policy, or with `retry=False`, does
  not retry its tasks.
- A value of the wrong type raises `TypeError` when the options, app or task are
  defined; a `RetryPolicy` field out of range raises `ValueError`.

## Backoff

Without a backoff, a retry is published at once. `backoff` takes:

- a number: the same delay in seconds before every retry;
- `Exponential(base, factor, max, jitter=False)`: retry `n` waits
  `min(max, base * factor ** (n - 1))` seconds, so the first retry waits
  `base`, the second `base * factor`, and so on up to `max`. With
  `jitter=True` it waits a random time between 0 and that value instead, which
  spreads out retries of tasks that failed together;
- your own strategy: any object with a `delay(retry: int) -> float` method
  (the `Backoff` protocol), where `retry` counts from 1.

A delay of zero or less means no delay. The delay is computed once, when the
worker publishes the retry.

## Which errors are retried

`retry_on` is an allow-list of exception types, matched with `isinstance`, so
subclasses count. An error that does not match fails the task at once with
status `FAILED`, even when retries are left. `retry_on` cannot be empty; use
`TaskOptions(retry=False)` to turn retries off.

## Timeouts

`TaskOptions(timeout=...)` limits how long one attempt of a task may run, in
seconds. An attempt that exceeds it is cancelled, its concurrency slot is freed
and the task fails with `TaskTimeoutError`, which goes through the retry policy
like any other error.

``` { .python }
--8<-- "examples/guides/timeouts.py"
```

It prints:

```text
call_partner failed: TaskTimeoutError after 2 attempts
rebuild_index: built
```

- The limit is per attempt: every retry starts a fresh timer. There is no
  total deadline across retries.
- Only the task function is timed, not middleware or hooks.
- Set it for every task with `App(options=TaskOptions(timeout=...))` and for
  one task with `@app.task(options=...)`. A task's value replaces the app's;
  `TaskOptions(timeout=False)` turns the limit off for one task, and `None`
  leaves it unset. The value must be a number greater than zero and finite:
  `0`, a negative number, `nan` or `inf` raise `ValueError`, and a non-number
  raises `TypeError`.
- A timeout is retried exactly when `retry_on` matches `TaskTimeoutError`. The
  default `(Exception,)` matches it; a narrowed `retry_on`, such as
  `(ConnectionError,)`, excludes timeouts unless you list `TaskTimeoutError`.
- A `TimeoutError` the task raises itself, such as an HTTP client timeout, is an
  ordinary failure, not a `TaskTimeoutError`.
- `handle.result()` raises `TaskError` with type name `TaskTimeoutError`.
- A worker shutdown that cancels the task is not a timeout: the delivery is
  requeued at the same attempt.

Limits:

- A task must not swallow `asyncio.CancelledError`. If it does, it keeps its
  slot until it finishes: the timeout cancels the task, it does not kill it.
- A blocking call inside a task blocks the event loop, including the timer, so
  nothing can time it out. The backstop is the liveness probe: the worker's
  heartbeat runs on the same loop, so a blocked loop lets the probe files go
  stale and the pod is restarted, and the broker redelivers the messages. See
  [Probes](probes.md).

## Statuses and states

| Outcome | `TaskStatus` | `TaskState` |
|---|---|---|
| failed, with retries left | `RETRYING` | `RETRYING` |
| failed, not retried (no policy, or an error not in `retry_on`) | `FAILED` | `FAILURE` |
| failed, with no retries left | `RETRIES_EXCEEDED` | `FAILURE` |

The worker records `RETRYING` before it publishes the retry, and the retry
records `RUNNING` when it starts. While the state is `RETRYING`,
`handle.result()` keeps waiting for a later attempt. Once the task fails for
good, its state is `FAILURE` and `result()` raises `TaskError`, with the
error's type name and message.

## Delays and transports

The delay is not part of the envelope. The worker passes it to the producer
when it publishes the retry, as `PublishOptions(delay=...)`, and passes no
options when there is no delay. Options are advisory: a transport that cannot
delay a message delivers the retry at once, so the backoff is lost. See
[Writing a custom transport](../advanced/custom-transport.md#producer).

The memory transport holds a delayed retry in the process and delivers it when
the delay is over. A retry that is not yet due is lost when the process exits.

When the retry cannot be published, because the broker fails or the app has no
producer, the worker logs the error and rejects the delivery with requeue. The
message then runs again at the same attempt, and its state stays `RETRYING`
until then. An app without a producer therefore retries a failing task over and
over, with no backoff and without using up its retries.

## Middleware

Layers see the final status: a task that will be retried returns `RETRYING`
from `next`. A layer that raises before the task ran goes through the task's
retry policy, like a failing task. A layer that raises after the task ran is
logged, and the task's outcome stands, so a `RETRYING` task is still retried.

The worker publishes the retry after the chain returns. A layer that settles
the delivery itself, with `call.delivery.ack()` or `reject()`, takes over the
retry: the worker publishes nothing for it. See
[Middleware](../advanced/middleware.md#settling-the-delivery-yourself).

## Pitfalls

- **Retried tasks must be idempotent.** Delivery is at-least-once. A retry
  runs the whole task again, and a worker cancelled while it publishes a retry
  requeues the original, so the task can run twice even when the retry was
  published.
- **`max_retries=None` retries forever.** A task that always fails is retried
  without end, waiting its backoff each time.
- **A transport that ignores `delay` retries immediately.** The backoff only
  works when the transport honors `PublishOptions.delay`.
- **A failed retry publish does not count as a retry.** The original delivery
  is requeued at the same attempt, so it can loop while the broker is down.
- **A layer that settles a `RETRYING` result stops the retry.** Its state
  stays `RETRYING`, and `result()` keeps waiting, until another attempt records
  a new one.
