"""Redis integration for asynk, installed with the `asynk[redis]` extra."""

try:
    import redis  # noqa: F401
except ImportError as exc:
    if exc.name != "redis":
        raise
    raise ImportError("RedisStore needs the redis extra: pip install 'asynk[redis]'") from exc

from asynk.store.redis.store import RedisStore

__all__ = ["RedisStore"]
