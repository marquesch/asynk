# Release notes

The release notes explain what matters in each release: what breaks, how to migrate
and what is new. They are written by hand, newest release first, when a release is
prepared, not in the pull requests of individual features.

The [changelog](changelog.md) is generated from the commit history and is the complete
list of changes in each release.

## 0.1.0

The first release. These notes group the changes made before it by kind, with the
breaking ones first.

### Breaking changes

#### Changed

- `TaskStatus.FAILED` is final: the task is not retried and is recorded
  as `FAILURE`. It used to be recorded as `RETRYING` and `result()` never completed.
- A middleware layer that raises before the task ran is retried through
  the task's retry policy; without a policy the failure is final. It used to be
  republished with no limit when `max_retries` was not set.
- The worker no longer rejects a delivery itself when retries are used
  up after a failing layer; the consumer settles it, as for any failed task.
- When a layer raised before the task ran and the retry cannot be
  published, the delivery is rejected with requeue at the same attempt; it used to
  be left to the consumer's default settlement.
- `Producer.publish(envelope, body, options=None)` takes publish
  options; custom producers must accept the parameter and may ignore it.
- asynk's exceptions are defined in `asynk.errors` and exported from
  `asynk`; `asynk.app`, `asynk.codec` and `asynk.hooks` no longer provide them.
- They derive from `AsynkError` only: `except ValueError` no longer
  catches `CronError`, `DecodeError`, `EncodeError` or `InvalidEnvelopeError`,
  `except LookupError` no longer catches `TaskNotFoundError`, and
  `except RuntimeError` no longer catches `NotConfiguredError`. `DecodeError` for
  a NaN or Infinity constant now carries its message without the
  `invalid JSON: ` prefix.
- `TaskFailedError` is renamed `DeliveryFailedError`.
- A middleware layer that raises after the task ran is logged and the
  task's outcome stands; a failing `after_task` hook used to redeliver the message.
- Python 3.11 is now the minimum supported version.
- `delay` returns a `TaskHandle`; the producer's receipt is at
  `handle.receipt`.
- `App.get_task(name)` was renamed `App.registered_task(name)`, and
  `get_task` takes a task id.
- `Codec` is now a generic value serializer with `validate(value)`,
  `encode(value)` and `decode(data)`, owned by the app through `App(codec=...)`.
- `Producer.publish(envelope, body)` takes the encoded body and
  `Delivery.body` carries it; transports no longer take a codec argument.
- Hooks are deduped by object identity and equality instead of
  `module.qualname`, so partials, lambdas, factory-made hooks and callable instances
  register; registering the same hook twice is still a no-op.
- The `Store` protocol gains `delete(key) -> bool`; third-party stores must
  implement it.
- The store is no longer registered in `app.hooks`; the worker and the
  scheduler start it before the `before_startup` hooks and stop it after the
  `after_shutdown` hooks, so `after_shutdown` hooks can use it. `before_startup` and
  `after_shutdown` start empty, and `after_shutdown` hooks run only after a completed
  startup.
- `Runner` implements `run()`, which runs the lifecycle; subclasses
  must implement `_serve()`.
- When several `after_shutdown` hooks raise, `run()` raises an
  `ExceptionGroup`; it used to surface only the first error, so `except RuntimeError`
  no longer catches that case.

#### Removed

- `max_retries=` on `@app.task` and `@app.scheduled_task`, and
  `Task.max_retries`; write `options=TaskOptions(retry=RetryPolicy(max_retries=3))` instead of
  `max_retries=3`.
- The extra keyword arguments of `@app.task` and `@app.scheduled_task`
  (`**options`) and `Task.options`; an unknown keyword argument raises `TypeError`.
- `App.before_task`, `App.after_task`, `BeforeTaskHook`, `AfterTaskHook`
  and `SkipTask`; middleware replaces them.
- `Producer.validate` and `Delivery.envelope`.
- `MemoryProducer.publish_raw`.
- The unix socket transport (`asynk.transport.unix`, `unix_transport`) and
  `SqliteStore`, with their examples.

### Added

- MIT license: a `LICENSE` file and the `license` metadata of the package.
- `RedisStore` in `asynk.store.redis`: a store on Redis that several processes
  share, for task results and scheduler claims, installed with the `asynk[redis]`
  extra; documentation page "Redis store".
- `asynk.store.polling.poll`, a helper for custom stores that can only read a key to
  implement `wait` by polling with a growing pause.
- `TaskOptions(retry=None)`, per-task options, accepted as `options=` by `App` (the
  default of every task), `@app.task` and `@app.scheduled_task`.
- Built-in retries, configured with `TaskOptions(retry=...)`, which takes a
  `RetryPolicy`, `False` or `None`; a task's policy replaces the app's default, and
  `False` disables retries. A wrong type raises `TypeError` at definition. A failing task with
  retries left is reported as `TaskStatus.RETRYING`, republished as
  `envelope.retry()` after its backoff, and the original delivery is acked; if the
  retry cannot be published, the delivery is rejected with requeue.
- `RetryPolicy(max_retries=1, backoff=None, retry_on=(Exception,))`, the
  `Exponential(base, factor, max, jitter=False)` backoff and the `Backoff`
  protocol for custom strategies.
- `PublishOptions(delay=...)`, an optional third argument of `Producer.publish`;
  the memory transport delivers a message after its `delay`.
- Task middleware: `@app.middleware` registers
  `async def layer(call: TaskCall, next: Next) -> TaskResult` around each task; the
  last registered layer is the outermost. `TaskCall.skip()` builds the `SKIPPED`
  result that vetoes a task.
- Transport contract test suite (`tests/transport/contract/`) that custom
  transports can copy to check their delivery, settle and `process` behavior.
- `AsynkError`, the base class of every exception asynk defines.
- `CronError` is exported from `asynk`.
- `asynk.__version__`, read from the installed distribution's metadata.
- Package classifiers, keywords and project URLs.
- Documentation site built with Zensical (strict build in CI, `make docs` and
  `make docs-serve`, `docs` dependency group), deployed to GitHub Pages from
  master.
- Documentation pages: concepts, guides (scheduled tasks, results and the store,
  publishing with metadata, probes, delivery guarantees and shutdown), advanced
  topics (lifecycle hooks, writing a custom transport, writing a custom store)
  and the changelog, with tested examples.
- Documentation page on middleware (advanced topics): ordering, vetoes, cleanup,
  failing layers and pitfalls, with a tested timing and counting example and its
  Prometheus version.
- Documentation page on retries (guides), with a tested example.
- The documentation site follows the system color scheme and has a light/dark toggle.
- `RedisProducer` and `RedisConsumer` in `asynk.transport.redis`: a transport on
  Redis Streams (consumer groups, `XAUTOCLAIM` takeover of expired leases, a
  dead-letter stream, delayed publish), installed with the `asynk[redis]` extra;
  documentation page "Redis transport".
- Transport contract suite: `ContractSettings.supports_delay` and a check that a
  delayed publish is not delivered early; a `redis` registry entry that runs on a
  real server when `ASYNK_TEST_REDIS_URL` is set.

### Changed

- Development: `make test` reports coverage through `pytest-cov` (branch coverage of
  `src/asynk`, no minimum), and CI reports the coverage of each test run.
- `Delivery` documents that a second `ack` or `reject` raises `RuntimeError`.
- The worker decodes each message and rejects, without requeue, one it cannot decode.
- The README is reduced to a pitch and a quick start; the details moved to the
  documentation site.
- Docstrings reviewed for consistency: every module has a docstring, and the status and state enum members are documented.

### Fixed

- The scheduler releases its dedupe claim when the publish fails or is cancelled.
- `MemoryStore` purges expired keys and frees wait events instead of keeping them
  for the life of the store.
- Every `after_shutdown` hook runs even when one raises; one failure is re-raised.
- The store is stopped when a worker's or scheduler's startup fails (a `before_startup`
  hook raises or the consumer fails to start) or `run()` is cancelled; an error from its
  `stop()` is logged and does not hide the startup error.
