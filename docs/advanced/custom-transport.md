# Writing a custom transport

A transport connects asynk to a broker. It is two objects that satisfy the
`Producer` and `Consumer` protocols, plus the `Delivery` objects the consumer
yields. They are protocols, so no base class is needed: any objects with the
right methods work, and mypy checks them where they are passed to `App`.

This one runs over an `asyncio.Queue`. A broker transport has the same shape,
with network calls in place of the queue:

``` { .python }
--8<-- "examples/advanced/custom_transport.py"
```

The signatures and their docstrings are in the API reference:
[`Producer`](../reference.md#asynk.Producer),
[`Consumer`](../reference.md#asynk.Consumer) and
[`Delivery`](../reference.md#asynk.Delivery), with
[`PublishOptions`](../reference.md#asynk.PublishOptions).

## Producer

`publish(envelope, body, options=None)` sends `body`, the bytes the app built with
`app.codec.encode(envelope.to_dict())`. The envelope is there for routing and
broker metadata, such as using `envelope.id` as the broker's message id. The
transport never encodes anything itself. Whatever `publish` returns becomes
`handle.receipt`.

`options` is a `PublishOptions` or `None`. Its `delay` asks the broker to hold
the message for that many seconds before delivering it; the worker passes it
when it publishes a retry with backoff, and only then, so `publish(envelope,
body)` keeps working. Options are advisory: honor what your broker supports and
ignore the rest. A transport that ignores `delay` delivers retries immediately,
so they get no backoff. Raising from `publish` instead is worse: the worker
then rejects the original delivery with requeue, so it runs again at the same
attempt and never uses up its retries. Broker limits, such as a maximum delay, are the
transport's to handle. The memory transport holds a delayed message in a timer,
so one not yet due is lost when the process exits.

## Delivery

A delivery carries the received `body` and is settled exactly once, with
`ack()` or `reject(requeue=...)`. `settled` reports whether that happened. The
worker settles some deliveries itself (see below), so `process` checks
`settled` before settling.

## The consumer contract

- `start()` is called once before `deliveries()`, and `stop()` once after the
  iterator is closed. Deliveries still unsettled when `stop()` runs should be
  redelivered by the broker.
- `connected` drives the readiness probe: report false while the broker
  connection is down.
- `deliveries()` returns the stream of incoming deliveries. It may be cancelled
  at any await point during shutdown; if the iterator has an `aclose()`
  method the worker calls it. When the iterator ends, the worker drains its
  running tasks and `Worker.run()` returns.
- `process(delivery)` is an async context manager entered once per delivery,
  around its handling. On a clean exit it must settle an unsettled delivery
  (usually `ack`). On an exception it must settle an unsettled delivery
  (usually `reject`) and must never suppress the exception, especially
  `asyncio.CancelledError`.

What reaches `process`:

- When a task fails for good, is unknown or exceeds its retries, the worker
  raises `DeliveryFailedError` inside the block; `.result.status` says which. A
  task that is retried is settled by the worker: it publishes the retry and
  acks the delivery, or rejects it with requeue when the retry cannot be
  published. Any other exception is an unexpected error.
- The worker decodes each body inside `process`. A body it cannot decode is
  rejected without requeue there, and the block exits cleanly.
- When a task is cancelled at the shutdown timeout, the worker rejects its
  unsettled delivery with `requeue=True` before the cancellation reaches
  `process`, so the message is redelivered rather than dropped.
