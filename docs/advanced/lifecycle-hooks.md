# Lifecycle hooks

Lifecycle hooks extend a worker or a scheduler without subclassing it, for
example to open a connection pool at startup and close it at shutdown. To run
code around each task instead, see [Middleware](middleware.md).

``` { .python }
--8<-- "examples/advanced/lifecycle_hooks.py"
```

| Decorator | Runs |
|---|---|
| `before_startup` | after the store starts, before the consumer starts (worker) or the loop starts (scheduler) |
| `after_startup` | once the consumer has started (worker) or the loop is about to start (scheduler) |
| `before_shutdown` | when the worker stops fetching, before draining; when the scheduler stops |
| `after_shutdown` | after draining with the consumer stopped (worker); after the loop ended (scheduler); before the store stops, and only after a completed startup |

## One hook for every process

Lifecycle hooks run in every asynk process, `Worker` and `Scheduler` alike, and
receive the `Runner` they run in. `runner.role` is `"worker"` or
`"scheduler"`, so a hook can branch where the processes differ. Annotate the
parameter `Runner`: a hook annotated `Worker` is rejected by mypy, since it
would not accept a scheduler.

## Startup and shutdown order

1. The store starts.
2. The `before_startup` hooks run.
3. The consumer starts (worker).
4. The `after_startup` hooks run.
5. The process serves.
6. The `before_shutdown` hooks run.
7. The worker drains and stops its consumer.
8. The `after_shutdown` hooks run.
9. The store stops.

Every hook can use the store.

## Registration

Each decorator appends to an ordered list, and hooks run in registration
order. Hooks may be `def` or `async def`; sync hooks run on the event loop, so
keep them cheap. Registering the same hook object twice is a no-op; hooks that
merely share a name, such as lambdas or functions made by a factory, are all
registered. A module that is imported again (for example reloaded) registers
its hooks again, since its functions are new objects.

## Errors

Hooks fail loudly. A lifecycle hook that raises makes `Worker.run()` or
`Scheduler.run()` raise, and the remaining hooks of that event do not run, with
one exception: every `after_shutdown` hook runs even when one raises. Each
failure is logged. One failure is re-raised as is; several are raised together
as an `ExceptionGroup`, which you catch with `except*`. The process still
cleans up first, and a worker still drains its running tasks.

If a `before_startup` hook raises, the consumer fails to start, or `run()` is
cancelled before startup completes, no further hook runs: not `after_startup`,
`before_shutdown` or `after_shutdown`. Only the store is stopped, and the error
propagates. If the store's `stop()` raises while this unwinds, that error is
logged and never hides the original one.

So a resource opened by an earlier `before_startup` hook is not closed by your
`after_shutdown` hook when a later step fails. Close it yourself: open and
close it in one hook, with `try`/`except` around what can fail. A failing
`after_startup` hook still runs the shutdown path, including `after_shutdown`.

In a scheduler, keep hooks quick: the liveness probe is not refreshed while a
hook runs, and a slow shutdown hook delays exit by its full duration. See
[Scheduled tasks](../guides/scheduled-tasks.md#hooks-and-probes).
