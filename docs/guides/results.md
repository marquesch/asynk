# Results and the store

Pass a store to the app to record task states and return values. `delay` then
returns a handle that reads them:

``` { .python }
--8<-- "examples/guides/results_store.py"
```

## Handles

`delay` returns a `TaskHandle` with:

- `task_id`, the envelope id;
- `receipt`, whatever the producer's `publish` returned;
- `state()`, the current `TaskState`;
- `result(timeout=None)`, which waits for the task to finish and returns its
  value.

`app.get_task(task_id)` builds a handle from an id alone, for example in
another request or process that shares the store. `result(timeout=...)` raises
`TimeoutError` when the task is not done in time, and `TaskNotFoundError` for an
id the store has no record of. A failed task makes `result()` raise
`TaskError`, which carries the failure's type name and message as `.type_name`
and `.message`.

## States

| State | Meaning |
|---|---|
| `PENDING` | published, not started yet |
| `RUNNING` | a worker is running it |
| `RETRYING` | an attempt failed and a retry is published |
| `SUCCESS` | finished; `result()` returns the value |
| `FAILURE` | failed for good; `result()` raises `TaskError` |
| `SKIPPED` | a middleware vetoed the task, so it did not run; `result()` returns `None` |
| `UNKNOWN` | the store has no record for this id |

A failed attempt is final, `FAILURE`, when the task has no retry policy, when
the error is not in the policy's `retry_on`, or when its retries are used up.
Otherwise the state is `RETRYING` until the retry runs, and `result()` keeps
waiting for a later attempt (see [Retries](retries.md)). A message for a task name the worker does not know
is recorded as `FAILURE`. A return value the codec cannot encode is recorded as
a `FAILURE` with the type name `ResultNotSerializable`.

Store errors never affect a task or its delivery: the worker logs them and
carries on, so a record can be missing after a store outage.

## Store lifecycle

A worker and a scheduler start the app's store before their `before_startup`
hooks and stop it after their `after_shutdown` hooks, also when startup fails,
so they need nothing extra. A
producer-only script, which runs neither, starts and stops the store itself
(a fragment: it needs a worker running elsewhere):

```python
await app.store.start()
handle = await add.delay(1, b=2)
print(await handle.result(timeout=10))
await app.store.stop()
```

## Stores

`MemoryStore(ttl=86400.0)` keeps records in one process for `ttl` seconds, so
the producer, the worker and readers must share that process. It is the
development and testing store. For anything wider, implement the `Store`
protocol over Redis or a database; see
[Writing a custom store](../advanced/custom-store.md).

Without a store the app uses a `NullStore`: it records nothing, and `state()`
and `result()` raise `NotConfiguredError`.

## Codec

Task results and states are encoded with `app.codec`, the same codec that
encodes messages (`JsonCodec()` by default). Pass `App(..., codec=MyCodec())`
to use another format, for example pickled values that keep tuples. With
`JsonCodec`, a tuple comes back as a list.
