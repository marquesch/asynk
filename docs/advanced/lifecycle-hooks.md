# Lifecycle hooks

Lifecycle hooks extend a worker or a scheduler without subclassing it, for
example to open a connection pool at startup and close it at shutdown.

``` { .python }
--8<-- "examples/advanced/lifecycle_hooks.py"
```

| Decorator | Runs |
|---|---|
| `before_startup` | before the consumer starts (worker) or the loop starts (scheduler) |
| `after_startup` | once the consumer has started (worker) or the loop is about to start (scheduler) |
| `before_shutdown` | when the worker stops fetching, before draining; when the scheduler stops |
| `after_shutdown` | last: after draining with the consumer stopped (worker); after the loop ended (scheduler) |

## One hook for every process

Lifecycle hooks run in every asynk process, `Worker` and `Scheduler` alike, and
receive the `Runner` they run in. `runner.role` is `"worker"` or
`"scheduler"`, so a hook can branch where the processes differ. Annotate the
parameter `Runner`: a hook annotated `Worker` is rejected by mypy, since it
would not accept a scheduler.

The app registers its own hooks first: it starts its store in `before_startup`
and stops it in `after_shutdown`, so your startup hooks can already use the
store.

## Registration

Each decorator appends to an ordered list, and hooks run in registration
order. Hooks may be `def` or `async def`; sync hooks run on the event loop, so
keep them cheap. Registering the same hook object twice is a no-op; hooks that
merely share a name, such as lambdas or functions made by a factory, are all
registered. A module that is imported again (for example reloaded) registers
its hooks again, since its functions are new objects.

## Errors

Hooks fail loudly. A lifecycle hook that raises stops the remaining hooks of
that event and makes `Worker.run()` or `Scheduler.run()` raise. The process
still cleans up first, and a worker still drains its running tasks.

In a scheduler, keep hooks quick: the liveness probe is not refreshed while a
hook runs, and a slow shutdown hook delays exit by its full duration. See
[Scheduled tasks](../guides/scheduled-tasks.md#hooks-and-probes).
