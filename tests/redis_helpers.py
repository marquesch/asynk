"""fakeredis, made to wait the way a real server does."""

from __future__ import annotations

import asyncio
from typing import Any

import fakeredis


class BlockingFakeRedis(fakeredis.FakeAsyncRedis):
    """A fake client whose blocking `xreadgroup` waits `block` ms when nothing is ready.

    fakeredis returns at once from a blocking read; a real server holds the call.
    """

    async def xreadgroup(self, *args: Any, **kwargs: Any) -> Any:
        entries = await super().xreadgroup(*args, **kwargs)
        block = kwargs.get("block")
        if not entries and block:
            await asyncio.sleep(block / 1000)
        return entries
