# Redis store

`RedisStore` keeps task states, results and scheduler claims in Redis, so
producers, workers and schedulers in different processes or on different
machines share them.

## Install

```sh
pip install 'asynk[redis]'
# or
uv add 'asynk[redis]'
```

Import it from its own module. Without the extra, this import raises an
`ImportError` that tells you to install it:

```python
from asynk.store.redis import RedisStore
```

The example needs a Redis server at `redis://localhost:6379/0`, so the
documentation tests do not run it:

``` { .python }
--8<-- "examples/guides/redis_store.py"
```

## Configuration

`RedisStore(url=None, *, client=None, ttl=86400.0, prefix="asynk:", max_poll_interval=1.0)`

- **`url` or `client`**: give exactly one. With `url`, the store builds the
  client and closes it on `stop()`. With `client`, a
  `redis.asyncio.Redis` you built, you own it and close it yourself.
- **`ttl`**: seconds a key lives. Every write resets it.
- **`prefix`**: text before every key. Give each app that shares a Redis its
  own, for example `"asynk:billing:"`.
- **`max_poll_interval`**: the longest pause, in seconds, between the checks
  `result()` makes while it waits.

A client built from `url` uses a 5 second socket timeout and a 5 second
connect timeout. Query parameters in the URL override them
(`redis://redis:6379/0?socket_timeout=10`). For anything else, such as TLS
settings or retries, pass your own `client`.

## How it maps to Redis

| Store method | Redis command |
|---|---|
| `set(key, value)` | `SET prefix+key value PX ttl NX` |
| `set(key, value, overwrite=True)` | `SET prefix+key value PX ttl` |
| `get(key)` | `GET prefix+key` |
| `delete(key)` | `DEL prefix+key` |
| `wait(key, timeout)` | `GET` repeated with a growing pause |

The keys are `asynk:state:<id>`, `asynk:result:<id>` and
`asynk:claim:<task>:<time>`, with your prefix in place of `asynk:`. Scheduler
claims use `SET NX`, which Redis runs atomically, so several schedulers can
share one store and exactly one publishes each slot.

## Pitfalls

- `wait` polls. `result()` can lag by up to `max_poll_interval` after the
  worker records the result.
- `start()` does not contact Redis. A wrong URL or a Redis that is down shows up
  as logged store errors, not as a failed startup. The worker and scheduler
  carry on without the store, as described in
  [Results and the store](results.md).
- A client you pass in without socket timeouts lets a hung Redis stall a claim
  or a record.
- A client you pass in must use `decode_responses=False`: values are bytes. With
  `True`, `get` raises a `TypeError`.
- Redis Cluster clients (`RedisCluster`) are not supported, and `wait` does not
  use pub/sub wake-ups.
- A producer-only process runs no worker or scheduler, so call
  `app.store.stop()` when it is done to close the connection pool; see
  [Store lifecycle](results.md#store-lifecycle).
- Records last only as long as Redis keeps them: its persistence settings and
  eviction policy apply.
