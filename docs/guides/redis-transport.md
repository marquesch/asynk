# Redis transport

`RedisProducer` and `RedisConsumer` move messages over Redis Streams, so
producers and workers on different machines share a queue. Redis 6.2 or newer is
required.

## Install

```sh
pip install 'asynk[redis]'
```

Import them from their own module. Without the extra, the import raises an
`ImportError` that tells you to install it:

```python
from asynk.transport.redis import RedisConsumer, RedisProducer
```

The example needs a Redis server at `redis://localhost:6379/0`, so the
documentation tests do not run it:

``` { .python }
--8<-- "examples/guides/redis_transport.py"
```

A process that only publishes calls `await producer.close()` when it is done, to
close the connection pool the producer created from a URL.

## Configuration

`RedisProducer(url=None, *, client=None, queue="default")`

`RedisConsumer(url=None, *, client=None, queue="default", group="asynk", lease=60.0, block=1.0, max_deliveries=None, dead_maxlen=10000)`

- **`url` or `client`**: give exactly one. With `url` the object builds its
  client (5 second socket and connect timeouts) and closes it; with `client`, a
  `redis.asyncio.Redis` created with `decode_responses=False`, you own it.
- **`queue`**: the queue name. There is one queue per producer or consumer.
  Producer and consumer must use the same name.
- **`group`**: the consumer group. Workers in one group share the queue's
  messages; workers in different groups each receive every message.
- **`lease`**: seconds a delivered message may stay unsettled before another
  consumer of the group takes it over.
- **`block`**: seconds each read waits for a message. It is also how often the
  consumer moves due delayed messages onto the stream and takes over expired
  leases, and the longest `deliveries()` takes to notice a shutdown.
- **`max_deliveries`**: see [Messages that crash the worker](#messages-that-crash-the-worker).
- **`dead_maxlen`**: how many entries the dead-letter stream keeps, approximately.

Set `lease` above the longest task, or a long task is delivered a second time to
another worker while it still runs.

## How it maps to Redis

The keys are `asynk:{<queue>}:stream`, `asynk:{<queue>}:delayed` and
`asynk:{<queue>}:dead`. The `{}` hash tag puts them in one cluster slot.

| Transport | Redis |
|---|---|
| `publish` | `XADD stream * body <bytes>` |
| `publish` with a delay | `ZADD delayed <server time + delay> <body>`; the server's clock is used, so hosts do not need synchronized clocks |
| a delay becomes due | each consumer, once per `block`, runs `WATCH`, `ZRANGEBYSCORE`, then `MULTI` with `ZREM` and `XADD`; concurrent consumers never move a message twice |
| `start` | `XGROUP CREATE ... 0 MKSTREAM`, so messages published before the group existed are consumed |
| `deliveries` | `XREADGROUP ... COUNT 1 BLOCK`, and `XAUTOCLAIM` for messages idle longer than `lease` |
| `ack` | `XACK` and `XDEL` in one transaction |
| `reject()` | `XADD` to the dead stream, `XACK` and `XDEL` in one transaction |
| `reject(requeue=True)` | `XADD` a fresh copy, `XACK` and `XDEL` in one transaction |
| `stop` | `XGROUP DELCONSUMER` for this consumer if it has no pending messages |

## Dead letters

A message rejected without requeue, which includes one whose retries ran out, is
moved to the `:dead` stream in the transaction that settles it. The entry has the
fields `body`, `reason` (`rejected` or `max_deliveries`), `entry_id` (the
original stream id) and `at` (milliseconds since the epoch). The stream is
trimmed to roughly `dead_maxlen` entries. To replay a message, read it with
`XRANGE` and publish `body` again.

## Messages that crash the worker

A message whose task kills the worker before it is settled is delivered again
after the lease, and again after that. asynk's `attempt` counter does not grow,
because no retry was published. `max_deliveries` caps this: a message taken over
more than that many times is dead-lettered with the reason `max_deliveries`. It
defaults to `None` (no cap); set it in production.

## Pitfalls

- Delivery is at-least-once. A message whose lease expires, or whose worker
  stopped without settling it, runs again on another worker.
- Takeover waits for the lease. After a worker dies its messages sit unsettled
  until `lease` has passed and a consumer of the group does its next check.
- `deliveries()` reads with short blocking commands so that cancelling it lands
  between commands. Shutdown takes up to `block` seconds. A message that Redis
  hands over just as the read is cancelled stays pending and is taken over after
  the lease.
- Delayed messages are moved once per `block`, so a delay can be late by up to
  that long, and nothing moves them while no consumer runs.
- Message order is not guaranteed: a delayed or requeued message goes to the back
  of the stream when it is due.
- `connected`, which drives the readiness probe, is false after a command failed
  with a connection or timeout error and true again after the next success.
  `deliveries()` keeps retrying every `block` seconds while Redis is down.
- Consumers that crash leave their names in the group. A consumer that stops
  cleanly with nothing pending removes its own name. To clean up the rest, use
  `XINFO CONSUMERS` and `XGROUP DELCONSUMER`, and only for consumers with
  `pending` equal to 0: deleting one that still has pending messages loses them.
- Run Redis with `maxmemory-policy noeviction`; evicting keys corrupts the
  queue. Enable AOF persistence if messages must survive a Redis restart.
- Redis Cluster works at the key level through the hash tags, but the clients
  here are the single-node `redis.asyncio.Redis`; `RedisCluster` is not
  supported.
- There is no lease extension for long tasks, and one queue per producer and
  consumer; queues and priorities are not supported.

## Testing

The contract suite runs against `fakeredis` by default, which covers the
protocol and the transaction logic. It cannot prove how a real server behaves:
blocking reads, lease expiry timing, `XAUTOCLAIM` under load and connection
loss. Set `ASYNK_TEST_REDIS_URL` to run the same suite on a real server; CI does. The
[custom transport guide](../advanced/custom-transport.md#testing-your-transport) has a
`docker run` recipe.
