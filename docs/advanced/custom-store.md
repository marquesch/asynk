# Writing a custom store

`MemoryStore` covers one process. To share task results and scheduler claims
between processes or machines, use the [Redis store](../guides/redis.md), or
implement the `Store` protocol for another backend. Like the transport
protocols it needs no base class.

``` { .python }
--8<-- "examples/advanced/custom_store.py"
```

The signatures are in the API reference, under
[`Store`](../reference.md#asynk.Store).

## Methods

- `start()` and `stop()` open and close resources such as a connection pool.
  A worker and a scheduler call them around their lifecycle hooks, so they
  need nothing extra; `stop()` is called only after `start()` returned, also
  when startup fails later. A producer-only script calls them itself (see
  [Store lifecycle](../guides/results.md#store-lifecycle)).
- `get(key)` returns the value, or `None` when the key is missing.
- `set(key, value, *, overwrite=False)` writes a value. It returns `False`, and
  writes nothing, when the key exists and `overwrite` is false. The scheduler
  relies on this to claim each slot once, so it must be atomic across every
  process sharing the store (for example `SET NX` in Redis).
- `wait(key, timeout=None)` returns the value once it is written, or `None`
  when `timeout` elapses. `TaskHandle.result` uses it.
- `delete(key)` removes a key and returns whether it existed.

If the backend can only read a key, not wait for it to appear, build `wait` on
`asynk.store.polling.poll(fetch, *, timeout, max_interval)`. It calls the
coroutine function `fetch` until it returns a value, pausing longer after each
miss up to `max_interval` seconds, and returns `None` once `timeout` elapses. The
Redis store does this: `poll(lambda: self.get(key), timeout=timeout, max_interval=1.0)`.

Keys and values are opaque: keys are strings asynk builds, and values are bytes
encoded with the app's codec. Records are not deleted by asynk once a task has
finished, so a store should expire them, as `MemoryStore` does with its `ttl`.
