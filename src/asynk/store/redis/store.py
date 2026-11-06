"""`RedisStore`, a store on Redis shared by several processes."""

from __future__ import annotations

from redis.asyncio import Redis

from asynk.store.polling import poll


class RedisStore:
    """Store on Redis that several processes and machines share."""

    def __init__(
        self,
        url: str | None = None,
        *,
        client: Redis | None = None,
        ttl: float = 86400.0,
        prefix: str = "asynk:",
        max_poll_interval: float = 1.0,
    ) -> None:
        """Initializes the store.

        Args:
            url: Redis URL, such as `redis://localhost:6379/0`. The store creates
                and closes its own client. Give either `url` or `client`.
            client: Redis asyncio client, created with `decode_responses=False`.
                The caller owns it and closes it.
            ttl: Seconds a key lives after it is written.
            prefix: Text put before every key, so several apps can share one Redis.
            max_poll_interval: Longest pause in seconds between the checks `wait`
                makes for a value.

        Raises:
            ValueError: If not exactly one of `url` and `client` is given, or
                `ttl` or `max_poll_interval` is not positive.
        """
        if (url is None) == (client is None):
            raise ValueError("RedisStore needs exactly one of url and client")
        if client is None:
            assert url is not None
            client = Redis.from_url(url, socket_timeout=5.0, socket_connect_timeout=5.0)
            self._owns_client = True
        else:
            self._owns_client = False
        self._client = client
        if ttl <= 0:
            raise ValueError(f"ttl must be > 0, got {ttl}")
        if max_poll_interval <= 0:
            raise ValueError(f"max_poll_interval must be > 0, got {max_poll_interval}")
        self._ttl_ms = max(1, round(ttl * 1000))
        self._prefix = prefix
        self._max_poll_interval = max_poll_interval

    async def start(self) -> None:
        """Does nothing: the client connects on first use."""

    async def stop(self) -> None:
        """Closes the client if the store created it."""
        if self._owns_client:
            await self._client.aclose()

    def _key(self, key: str) -> str:
        return self._prefix + key

    async def get(self, key: str) -> bytes | None:
        """Reads a value, or returns None if the key is missing or expired."""
        value = await self._client.get(self._key(key))
        if isinstance(value, str):
            raise TypeError("RedisStore needs a client with decode_responses=False")
        return value

    async def set(self, key: str, value: bytes, *, overwrite: bool = False) -> bool:
        """Writes a value; returns False if the key exists and `overwrite` is false."""
        return bool(
            await self._client.set(self._key(key), value, px=self._ttl_ms, nx=not overwrite)
        )

    async def delete(self, key: str) -> bool:
        """Removes a key; returns False if it is missing or expired."""
        return await self._client.delete(self._key(key)) == 1

    async def wait(self, key: str, timeout: float | None = None) -> bytes | None:
        """Polls until the key has a value; returns None if `timeout` elapses first.

        Args:
            key: Key to wait for.
            timeout: Seconds to wait, or None to wait until the key has a value.
        """
        return await poll(
            lambda: self.get(key), timeout=timeout, max_interval=self._max_poll_interval
        )
