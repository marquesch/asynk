from __future__ import annotations

import asyncio

import pytest

from asynk.store.polling import poll


class Source:
    """Fetch that misses `misses` times, then returns a value."""

    def __init__(self, misses: int = 0) -> None:
        self.misses = misses
        self.calls = 0

    async def __call__(self) -> bytes | None:
        self.calls += 1
        return b"v" if self.calls > self.misses else None


async def test_returns_the_first_value() -> None:
    source = Source()
    assert await poll(source, timeout=1, max_interval=0.01) == b"v"
    assert source.calls == 1


async def test_keeps_fetching_until_there_is_a_value() -> None:
    source = Source(misses=3)
    assert await poll(source, timeout=None, max_interval=0.01) == b"v"
    assert source.calls == 4


async def test_returns_none_when_the_timeout_elapses() -> None:
    assert await poll(Source(misses=10**9), timeout=0.05, max_interval=0.01) is None


async def test_zero_timeout_fetches_once() -> None:
    source = Source(misses=1)
    assert await poll(source, timeout=0, max_interval=0.01) is None
    assert source.calls == 1


async def test_never_sleeps_past_the_timeout() -> None:
    loop = asyncio.get_running_loop()
    start = loop.time()
    assert await poll(Source(misses=10**9), timeout=0.05, max_interval=10.0) is None
    assert loop.time() - start < 0.5


async def test_cancellation_propagates() -> None:
    task = asyncio.create_task(poll(Source(misses=10**9), timeout=None, max_interval=0.01))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_backs_off_up_to_the_max_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    pauses: list[float] = []
    real_sleep = asyncio.sleep

    async def fake_sleep(delay: float) -> None:
        pauses.append(delay)
        await real_sleep(0)

    monkeypatch.setattr("asynk.store.polling.asyncio.sleep", fake_sleep)
    task = asyncio.create_task(poll(Source(misses=10**9), timeout=None, max_interval=0.3))
    while len(pauses) < 6:
        await real_sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert pauses[:6] == [0.05, 0.1, 0.2, 0.3, 0.3, 0.3]


async def test_last_pause_is_the_time_remaining(monkeypatch: pytest.MonkeyPatch) -> None:
    now = 100.0
    pauses: list[float] = []

    class FakeLoop:
        def time(self) -> float:
            return now

    async def fake_sleep(delay: float) -> None:
        nonlocal now
        pauses.append(delay)
        now += delay

    monkeypatch.setattr("asynk.store.polling.asyncio.sleep", fake_sleep)
    monkeypatch.setattr("asynk.store.polling.asyncio.get_running_loop", lambda: FakeLoop())
    assert await poll(Source(misses=10**9), timeout=0.2, max_interval=1.0) is None
    assert pauses == pytest.approx([0.05, 0.1, 0.05])
