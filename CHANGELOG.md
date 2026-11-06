# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

### Changed

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
