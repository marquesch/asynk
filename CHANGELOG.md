# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `PublishOptions(delay=...)`, an optional third argument of `Producer.publish`;
  the memory transport delivers a message after its `delay`.
- `AsynkError`, the base class of every exception asynk defines.
- `CronError` is exported from `asynk`.
- `asynk.__version__`, read from the installed distribution's metadata.
- Package classifiers, keywords and project URLs.
- Documentation site built with Zensical (strict build in CI, `make docs` and
  `make docs-serve`, `docs` dependency group), deployed to GitHub Pages from
  master.
- Task middleware: `@app.middleware` registers
  `async def layer(call: TaskCall, next: Next) -> TaskResult` around each task; the
  last registered layer is the outermost. `TaskCall.skip()` builds the `SKIPPED`
  result that vetoes a task.
- Documentation pages: concepts, guides (scheduled tasks, results and the store,
  publishing with metadata, probes, delivery guarantees and shutdown), advanced
  topics (lifecycle hooks, writing a custom transport, writing a custom store)
  and the changelog, with tested examples.
- Documentation page on middleware (advanced topics): ordering, vetoes, cleanup,
  failing layers and pitfalls, with a tested timing and counting example and its
  Prometheus version.
- The documentation site follows the system color scheme and has a light/dark toggle.

### Changed

- **Breaking:** `Producer.publish(envelope, body, options=None)` takes publish
  options; custom producers must accept the parameter and may ignore it.
- **Breaking:** asynk's exceptions are defined in `asynk.errors` and exported from
  `asynk`; `asynk.app`, `asynk.codec` and `asynk.hooks` no longer provide them.
- **Breaking:** they derive from `AsynkError` only: `except ValueError` no longer
  catches `CronError`, `DecodeError`, `EncodeError` or `InvalidEnvelopeError`,
  `except LookupError` no longer catches `TaskNotFoundError`, and
  `except RuntimeError` no longer catches `NotConfiguredError`. `DecodeError` for
  a NaN or Infinity constant now carries its message without the
  `invalid JSON: ` prefix.
- **Breaking:** `TaskFailedError` is renamed `DeliveryFailedError`.
- **Breaking:** a middleware layer that raises after the task ran is logged and the
  task's outcome stands; a failing `after_task` hook used to redeliver the message.
- The README is reduced to a pitch and a quick start; the details moved to the
  documentation site.
- **Breaking:** Python 3.11 is now the minimum supported version.
- **Breaking:** `delay` returns a `TaskHandle`; the producer's receipt is at
  `handle.receipt`.
- **Breaking:** `App.get_task(name)` was renamed `App.registered_task(name)`, and
  `get_task` takes a task id.
- **Breaking:** `Codec` is now a generic value serializer with `validate(value)`,
  `encode(value)` and `decode(data)`, owned by the app through `App(codec=...)`.
- **Breaking:** `Producer.publish(envelope, body)` takes the encoded body and
  `Delivery.body` carries it; transports no longer take a codec argument.
- The worker decodes each message and rejects, without requeue, one it cannot decode.
- **Breaking:** hooks are deduped by object identity and equality instead of
  `module.qualname`, so partials, lambdas, factory-made hooks and callable instances
  register; registering the same hook twice is still a no-op.
- **Breaking:** the `Store` protocol gains `delete(key) -> bool`; third-party stores must
  implement it.
- Docstrings reviewed for consistency: `max_retries` is described as the attempt count
  after which a failure is reported as `RETRIES_EXCEEDED`, and every module has a docstring, and the status and state enum members are documented.
- **Breaking:** the store is no longer registered in `app.hooks`; the worker and the
  scheduler start it before the `before_startup` hooks and stop it after the
  `after_shutdown` hooks, so `after_shutdown` hooks can use it. `before_startup` and
  `after_shutdown` start empty, and `after_shutdown` hooks run only after a completed
  startup.
- **Breaking:** `Runner` implements `run()`, which runs the lifecycle; subclasses
  must implement `_serve()`.
- **Breaking:** when several `after_shutdown` hooks raise, `run()` raises an
  `ExceptionGroup`; it used to surface only the first error, so `except RuntimeError`
  no longer catches that case.

### Removed

- **Breaking:** `App.before_task`, `App.after_task`, `BeforeTaskHook`, `AfterTaskHook`
  and `SkipTask`; middleware replaces them.
- **Breaking:** `Producer.validate` and `Delivery.envelope`.
- **Breaking:** `MemoryProducer.publish_raw`.
- **Breaking:** the unix socket transport (`asynk.transport.unix`, `unix_transport`) and
  `SqliteStore`, with their examples.

### Fixed

- The scheduler releases its dedupe claim when the publish fails or is cancelled.
- `MemoryStore` purges expired keys and frees wait events instead of keeping them
  for the life of the store.
- Every `after_shutdown` hook runs even when one raises; one failure is re-raised.
- The store is stopped when a worker's or scheduler's startup fails (a `before_startup`
  hook raises or the consumer fails to start) or `run()` is cancelled; an error from its
  `stop()` is logged and does not hide the startup error.
