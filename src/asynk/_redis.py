"""The Redis client a Redis store or transport uses, and who closes it."""

from __future__ import annotations

from asynk._extras import require_extra

require_extra("redis", "redis")

from redis.asyncio import Redis  # noqa: E402


class RedisConnection:
    """A Redis client, either given by the caller or created from a URL."""

    def __init__(self, url: str | None, client: Redis | None, *, owner: str) -> None:
        """Initializes the connection.

        Args:
            url: Redis URL; the connection creates its own client and closes it.
            client: Redis client the caller owns and closes.
            owner: Name of the class using the connection, for error messages.

        Raises:
            ValueError: If not exactly one of `url` and `client` is given.
        """
        if (url is None) == (client is None):
            raise ValueError(f"{owner} needs exactly one of url and client")
        if client is None:
            assert url is not None
            client = Redis.from_url(url, socket_timeout=5.0, socket_connect_timeout=5.0)
            self._owns_client = True
        else:
            self._owns_client = False
        self.client = client

    async def close(self) -> None:
        """Closes the client if the connection created it."""
        if self._owns_client:
            await self.client.aclose()
