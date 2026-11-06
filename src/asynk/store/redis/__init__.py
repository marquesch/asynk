"""Redis integration for asynk, installed with the `asynk[redis]` extra."""

from asynk._extras import require_extra

require_extra("redis", "redis")

from asynk.store.redis.store import RedisStore  # noqa: E402

__all__ = ["RedisStore"]
