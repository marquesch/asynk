# OpenTelemetry

asynk records traces and metrics with [OpenTelemetry](https://opentelemetry.io/)
and needs nothing beyond installing the API:

```
pip install 'asynk[otel]'
```

## What turns on, and when

The extra installs `opentelemetry-api` only. When an `App` is built, it checks
whether that package is importable; if it is, telemetry is on for that app.
There is no asynk setting for it. Without the package the app uses a
[`NullTelemetry`][asynk.telemetry.NullTelemetry] that does nothing, and `import asynk`
never imports OpenTelemetry.

The API alone records nothing: spans and metrics go to whatever SDK your process
configures (and to the no-op default if it configures none). With no SDK, no
trace context is written to messages, and the only cost is a few no-op calls.
To record anything, set up the OpenTelemetry SDK and an exporter in your
process, for example in a `before_startup` hook, the way you would for any
other library.

To turn it off, set the standard `OTEL_SDK_DISABLED=true` before the app is
built. asynk has no variable of its own.

Telemetry fails open: an error in any of it is logged once, telemetry is
switched off for the rest of the process, and publishing and tasks carry on.

## Traces

- **Publishing** (`delay`, `App.publish`) writes the active span's W3C
  `traceparent` (and `tracestate`, when there is one) into the envelope
  metadata through the global propagator. Metadata keys you set yourself win.
  There is no producer span.
- **Running a task** happens under a `CONSUMER` span named `process <task name>`
  whose parent is the context found in the metadata. The span has
  `messaging.system.name` (`asynk`), `messaging.operation.name` and
  `messaging.operation.type` (`process`), `messaging.message.id` (the envelope
  id), `asynk.task.name`, `asynk.task.attempt` and `asynk.task.status` (the
  final [`TaskStatus`][asynk.TaskStatus] value).
- A task that ends `failed`, `retrying` or `retries_exceeded` gets the
  exception recorded on its span, status `ERROR` and `error.type`. A `skipped`
  or `unknown_task` outcome is not an error.
- The span is the outermost layer of the [middleware](../advanced/middleware.md)
  chain whatever you register, so your own layers run inside it and see it as
  the current span. An exception raised by one of your layers, or a
  cancellation, ends the span with `ERROR`.
- A **retry** is published again with the original metadata, so every attempt
  continues the first trace.
- Tasks published by the [scheduler](scheduled-tasks.md) have no active span,
  so their worker spans start new traces.

## Metrics

The instrumentation scope is `asynk`, versioned with `asynk.__version__`.

| Metric | Type | Unit | Meaning |
| --- | --- | --- | --- |
| `messaging.process.duration` | histogram | `s` | Time to run a message through the chain, with `asynk.task.name`, `asynk.task.status` and `error.type` on errors |
| `messaging.client.sent.messages` | counter | `{message}` | Messages published by `App.publish`; retries are not counted |
| `messaging.client.consumed.messages` | counter | `{message}` | Messages a worker took for processing |
| `asynk.worker.active_tasks` | up-down counter | `{task}` | Deliveries being handled, to compare with `concurrency` |
| `asynk.task.retries` | counter | `{retry}` | Retries a worker published |

## Not provided

Queue depth (there is no transport statistics API), producer spans, log
correlation, broker-level spans from the transports, and a queue-time
histogram.

## Semantic conventions

The names follow the [messaging span][spans] and [messaging metric][metrics]
conventions, which are still in development, so attribute and metric names may
change in OpenTelemetry and in asynk with them. Choices that go beyond them:

- asynk has no queue or topic name in its core, so the task name stands in
  where the convention puts the destination: in the span name
  (`process <task name>`) and in `asynk.task.name`. The messaging
  destination attributes are not set.
- `error.type` is the exception's fully qualified class name (just the class
  name for built-ins).
- `messaging.client.consumed.messages` is counted when processing starts, as
  asynk has no separate receive step.

[spans]: https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/
[metrics]: https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/
