from __future__ import annotations

from asynk.store.polling import poll_wait


async def test_poll_wait_returns_value_appearing_later() -> None:
    calls = 0

    async def get(key: str) -> bytes | None:
        nonlocal calls
        calls += 1
        return b"v" if calls >= 3 else None

    assert await poll_wait(get, "k", 1.0, 0.01) == b"v"
    assert calls == 3


async def test_poll_wait_returns_none_on_timeout() -> None:
    async def get(key: str) -> bytes | None:
        return None

    assert await poll_wait(get, "k", 0.05, 0.01) is None
