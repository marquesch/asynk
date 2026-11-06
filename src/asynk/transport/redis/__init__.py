"""Redis Streams transport for asynk, installed with the `asynk[redis]` extra."""

from asynk._extras import require_extra

require_extra("redis", "redis")

from asynk.transport.redis.transport import RedisConsumer, RedisProducer  # noqa: E402

__all__ = ["RedisConsumer", "RedisProducer"]
