# Concepts

asynk has a small core. Producers and consumers are decoupled: the core only
knows an envelope, a codec and two transport protocols, so any broker can sit
behind them.

## Envelope

An `Envelope` is the message exchanged between producers and consumers. It
carries the task name, its `args` and `kwargs`, a unique `id`, a zero-based
`attempt` counter and free-form `metadata`. `envelope.retry()` returns a copy
with `attempt` incremented and the same `id`; the worker publishes it to retry
a task.

## Codec

A `Codec` turns values into bytes and back, with three methods:
`validate(value)`, `encode(value) -> bytes` and `decode(data) -> object`. The
app owns a single codec, `App(codec=...)`, used for messages, task states and
results. `JsonCodec` is the default. `EncodeError` and `DecodeError` report
values the codec cannot encode and bytes it cannot decode.

The app builds a message body with `app.codec.encode(envelope.to_dict())`, and
the worker rebuilds the envelope with
`Envelope.from_dict(app.codec.decode(delivery.body))`. `delay` encodes its
arguments right away, so a value the codec cannot encode fails at the call
site. The scheduler validates each scheduled task's fixed arguments when it is
created. To change the wire format, pass another codec, for example one that
pickles values so tuples survive the round trip.

## Transport

A transport is a `Producer` and a `Consumer`. The transport moves bytes: the
producer publishes the body the app encoded, and the consumer yields
`Delivery` objects that carry those bytes and are settled with `ack` or
`reject`. The consumer also decides how a delivery is settled once its task has
run. Transports never encode anything themselves, so they take no codec. See
[Writing a custom transport](advanced/custom-transport.md).

asynk ships `memory_transport()`, an in-process pair for development and
testing. Its queue lives in one process and vanishes when that process exits,
so whoever publishes and whoever runs the worker must be the same process: the
memory transport is not for production, and `asynk worker` only helps with a
transport whose producer and consumer talk through an external system.

## Store

A `Store` is a small async key-value interface. The app uses it for task states
and results, and the scheduler uses it to claim cron slots. Without a store the
app uses a `NullStore`, which records nothing. `MemoryStore` keeps records in
one process. See [Results and the store](guides/results.md) and
[Writing a custom store](advanced/custom-store.md).

## Worker and scheduler

A `Worker` consumes deliveries and runs their tasks with bounded concurrency. A
`Scheduler` publishes scheduled tasks on their cron schedules and runs nothing
itself. Both are runners: they run the app's
[lifecycle hooks](advanced/lifecycle-hooks.md), write
[probe files](guides/probes.md) and shut down gracefully on SIGTERM or SIGINT
when started from the `asynk` command line.

## Errors

Every exception asynk defines derives from `AsynkError` and can be imported
from `asynk`: `NotConfiguredError`, `EncodeError`, `DecodeError`,
`InvalidEnvelopeError`, `CronError`, `TaskError`, `TaskNotFoundError`,
`TaskTimeoutError` and `DeliveryFailedError`. `except AsynkError` catches all of them. Invalid
arguments, such as `Worker(concurrency=0)` or an unknown time zone, raise the
built-in `ValueError`, and `TaskHandle.result(timeout=...)` raises the built-in
`TimeoutError`.
