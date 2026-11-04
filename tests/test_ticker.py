from __future__ import annotations

import asyncio
import logging
import os
import signal
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from asynk.app import App, NotConfiguredError
from asynk.envelope import Envelope
from asynk.ticker import SystemClock, Ticker
from asynk.transport.memory import MemoryConsumer, memory_transport

UTC = timezone.utc


def _at(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


class FakeClock:
    """Clock whose `sleep` advances `now` instead of waiting.

    Once `now` reaches `until`, the next `sleep` calls `on_limit` and blocks
    forever, so the ticker under test stops through its normal shutdown path.
    `warps[n]` is added to `now` after the n-th sleep (0-based), simulating a
    stall (positive) or the wall clock stepping back (negative).
    """

    def __init__(self, start: datetime, until: datetime) -> None:
        self.now_value = start
        self.until = until
        self.sleeps: list[float] = []
        self.warps: dict[int, timedelta] = {}
        self.on_limit: Callable[[], None] = lambda: None

    def now(self) -> datetime:
        return self.now_value

    async def sleep(self, seconds: float) -> None:
        if self.now_value >= self.until:
            self.on_limit()
            await asyncio.Event().wait()
        index = len(self.sleeps)
        self.sleeps.append(seconds)
        self.now_value += timedelta(seconds=seconds) + self.warps.get(index, timedelta())
        await asyncio.sleep(0)


class BlockingClock:
    """Clock whose `sleep` never returns on its own."""

    def __init__(self, start: datetime) -> None:
        self.start = start
        self.sleeping = asyncio.Event()

    def now(self) -> datetime:
        return self.start

    async def sleep(self, seconds: float) -> None:
        self.sleeping.set()
        await asyncio.Event().wait()


def _app() -> tuple[App, MemoryConsumer]:
    producer, consumer = memory_transport()
    return App("t", producer=producer), consumer


async def _published(consumer: MemoryConsumer) -> list[Envelope]:
    await consumer.stop()
    return [d.envelope async for d in consumer.deliveries()]


async def _run(app: App, clock: FakeClock, **kwargs: Any) -> Ticker:
    ticker = Ticker(app, clock=clock, **kwargs)
    clock.on_limit = ticker.request_shutdown
    await asyncio.wait_for(ticker.run(), 1)
    return ticker


def _slots(envelopes: list[Envelope]) -> list[tuple[str, Any]]:
    return [(e.task, e.metadata["scheduled_at"]) for e in envelopes]


async def test_fires_on_slot_with_fixed_args_and_kwargs() -> None:
    app, consumer = _app()

    @app.tick(cron="30 * * * *", tz="UTC", args=[1, "a"], kwargs={"b": [2]}, name="job")
    def job(n: int, s: str, *, b: list[int]) -> None: ...

    clock = FakeClock(_at("2026-11-04T11:29:00"), until=_at("2026-11-04T11:31:00"))
    await _run(app, clock)
    [env] = await _published(consumer)
    assert env.task == "job"
    assert env.args == (1, "a")
    assert env.kwargs == {"b": [2]}


async def test_metadata_has_idempotency_key_and_scheduled_at() -> None:
    app, consumer = _app()

    @app.tick(cron="30 8 * * *", tz="America/Sao_Paulo", name="job")
    def job() -> None: ...

    clock = FakeClock(_at("2026-11-04T11:00:00"), until=_at("2026-11-04T12:00:00"))
    await _run(app, clock)
    [env] = await _published(consumer)
    assert env.metadata == {
        "idempotency_key": "job:2026-11-04T11:30:00+00:00",
        "scheduled_at": "2026-11-04T11:30:00+00:00",
    }


async def test_two_tickers_same_slot_same_key() -> None:
    keys = []
    for _ in range(2):
        app, consumer = _app()

        @app.tick(cron="*/5 * * * *", tz="Europe/Berlin", name="job")
        def job() -> None: ...

        clock = FakeClock(_at("2026-11-04T11:02:17"), until=_at("2026-11-04T11:06:00"))
        await _run(app, clock)
        [env] = await _published(consumer)
        keys.append(env.metadata["idempotency_key"])
    assert keys == ["job:2026-11-04T11:05:00+00:00"] * 2


async def test_nothing_fires_at_startup() -> None:
    app, consumer = _app()

    @app.tick(cron="30 11 * * *", tz="UTC", name="job")
    def job() -> None: ...

    # Starting exactly on a slot does not fire it.
    clock = FakeClock(_at("2026-11-04T11:30:00"), until=_at("2026-11-04T11:40:00"))
    await _run(app, clock)
    assert await _published(consumer) == []


async def test_only_due_ticks_fire() -> None:
    app, consumer = _app()

    @app.tick(cron="*/10 * * * *", tz="UTC", name="often")
    def often() -> None: ...

    @app.tick(cron="0 * * * *", tz="UTC", name="hourly")
    def hourly() -> None: ...

    clock = FakeClock(_at("2026-11-04T11:41:00"), until=_at("2026-11-04T12:15:00"))
    await _run(app, clock)
    assert _slots(await _published(consumer)) == [
        ("often", "2026-11-04T11:50:00+00:00"),
        ("often", "2026-11-04T12:00:00+00:00"),
        ("hourly", "2026-11-04T12:00:00+00:00"),
        ("often", "2026-11-04T12:10:00+00:00"),
    ]


async def test_late_within_grace_fires() -> None:
    app, consumer = _app()

    @app.tick(cron="30 * * * *", tz="UTC", name="job")
    def job() -> None: ...

    clock = FakeClock(_at("2026-11-04T11:29:30"), until=_at("2026-11-04T11:31:00"))
    clock.warps[0] = timedelta(seconds=20)  # wakes 20 s after the slot
    await _run(app, clock, grace=30)
    assert _slots(await _published(consumer)) == [("job", "2026-11-04T11:30:00+00:00")]


async def test_stall_beyond_grace_skips_missed_slots(caplog: pytest.LogCaptureFixture) -> None:
    app, consumer = _app()

    @app.tick(cron="0 * * * *", tz="UTC", name="job")
    def job() -> None: ...

    clock = FakeClock(_at("2026-11-04T10:59:00"), until=_at("2026-11-04T14:01:00"))
    clock.warps[0] = timedelta(hours=3)  # first wake lands at 13:59:30
    with caplog.at_level(logging.WARNING, logger="asynk"):
        await _run(app, clock)
    assert clock.now_value >= _at("2026-11-04T14:01:00")
    # 11:00, 12:00 and 13:00 were missed; 14:00 is fired normally.
    assert _slots(await _published(consumer)) == [("job", "2026-11-04T14:00:00+00:00")]
    assert any("skipping" in r.getMessage() for r in caplog.records)


async def test_stall_without_slot_in_grace_resumes_on_next_future_slot() -> None:
    app, consumer = _app()

    @app.tick(cron="0 * * * *", tz="UTC", name="job")
    def job() -> None: ...

    clock = FakeClock(_at("2026-11-04T10:59:00"), until=_at("2026-11-04T15:01:00"))
    clock.warps[0] = timedelta(hours=3, minutes=30)  # first wake lands at 14:29:30
    await _run(app, clock)
    assert _slots(await _published(consumer)) == [("job", "2026-11-04T15:00:00+00:00")]


async def test_catch_up_fires_all_missed_in_order_with_own_keys() -> None:
    app, consumer = _app()

    @app.tick(cron="0 * * * *", tz="UTC", catch_up=True, name="job")
    def job() -> None: ...

    clock = FakeClock(_at("2026-11-04T10:59:00"), until=_at("2026-11-04T14:31:00"))
    clock.warps[0] = timedelta(hours=3, minutes=30)  # first wake lands at 14:29:30
    await _run(app, clock)
    envs = await _published(consumer)
    assert [e.metadata["idempotency_key"] for e in envs] == [
        "job:2026-11-04T11:00:00+00:00",
        "job:2026-11-04T12:00:00+00:00",
        "job:2026-11-04T13:00:00+00:00",
        "job:2026-11-04T14:00:00+00:00",
    ]


async def test_clock_step_back_does_not_refire() -> None:
    app, consumer = _app()

    @app.tick(cron="* * * * *", tz="UTC", name="job")
    def job() -> None: ...

    clock = FakeClock(_at("2026-11-04T11:29:30"), until=_at("2026-11-04T11:31:30"))
    clock.warps[1] = timedelta(minutes=-2)  # fires 11:30, then steps back to 11:29
    await _run(app, clock)
    assert _slots(await _published(consumer)) == [
        ("job", "2026-11-04T11:30:00+00:00"),
        ("job", "2026-11-04T11:31:00+00:00"),
    ]


async def test_publish_failure_is_logged_and_loop_continues(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app, consumer = _app()
    assert app.producer is not None
    real = app.producer
    calls = 0

    class Flaky:
        async def publish(self, envelope: Envelope) -> Any:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ConnectionError("broker down")
            return await real.publish(envelope)

    app.producer = Flaky()

    @app.tick(cron="* * * * *", tz="UTC", name="job")
    def job() -> None: ...

    clock = FakeClock(_at("2026-11-04T11:29:30"), until=_at("2026-11-04T11:31:00"))
    with caplog.at_level(logging.ERROR, logger="asynk"):
        await _run(app, clock)
    assert _slots(await _published(consumer)) == [("job", "2026-11-04T11:31:00+00:00")]
    [record] = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert record.exc_info is not None
    assert isinstance(record.exc_info[1], ConnectionError)


async def test_shutdown_interrupts_sleep() -> None:
    app, consumer = _app()

    @app.tick(cron="0 0 1 1 *", tz="UTC", name="job")
    def job() -> None: ...

    clock = BlockingClock(_at("2026-11-04T11:29:30"))
    ticker = Ticker(app, clock=clock)
    run = asyncio.create_task(ticker.run())
    await asyncio.wait_for(clock.sleeping.wait(), 1)
    ticker.request_shutdown()
    await asyncio.wait_for(run, 1)
    assert await _published(consumer) == []


async def test_sleep_is_capped_by_max_sleep() -> None:
    app, consumer = _app()

    @app.tick(cron="0 12 * * *", tz="UTC", name="job")
    def job() -> None: ...

    clock = FakeClock(_at("2026-11-04T11:58:00"), until=_at("2026-11-04T12:00:30"))
    await _run(app, clock, max_sleep=50)
    assert clock.sleeps[:3] == [50, 50, 20]
    assert max(clock.sleeps) == 50
    assert _slots(await _published(consumer)) == [("job", "2026-11-04T12:00:00+00:00")]


async def test_probe_file_written_and_cleared(tmp_path: Path) -> None:
    app, _ = _app()

    @app.tick(cron="0 0 1 1 *", tz="UTC", name="job")
    def job() -> None: ...

    clock = BlockingClock(_at("2026-11-04T11:29:30"))
    ticker = Ticker(app, clock=clock, probe_dir=tmp_path)
    run = asyncio.create_task(ticker.run())
    await asyncio.wait_for(clock.sleeping.wait(), 1)
    assert (tmp_path / "liveness").exists()
    assert not (tmp_path / "readiness").exists()
    ticker.request_shutdown()
    await asyncio.wait_for(run, 1)
    assert not (tmp_path / "liveness").exists()


async def test_probe_write_failure_is_logged_not_raised(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    app, _ = _app()

    @app.tick(cron="0 0 1 1 *", tz="UTC", name="job")
    def job() -> None: ...

    blocker = tmp_path / "file"
    blocker.write_text("")
    clock = BlockingClock(_at("2026-11-04T11:29:30"))
    ticker = Ticker(app, clock=clock, probe_dir=blocker / "probes")
    run = asyncio.create_task(ticker.run())
    with caplog.at_level(logging.ERROR, logger="asynk"):
        await asyncio.wait_for(clock.sleeping.wait(), 1)
        ticker.request_shutdown()
        await asyncio.wait_for(run, 1)
    assert any("probe" in r.getMessage() for r in caplog.records)


def test_requires_producer() -> None:
    app = App("t")

    @app.tick(cron="* * * * *", tz="UTC", name="job")
    def job() -> None: ...

    with pytest.raises(NotConfiguredError):
        Ticker(app)


def test_requires_ticks() -> None:
    app, _ = _app()
    with pytest.raises(ValueError, match="no ticks"):
        Ticker(app)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"heartbeat_interval": 0}, "heartbeat_interval"),
        ({"max_sleep": 0}, "max_sleep"),
        ({"max_sleep": -1}, "max_sleep"),
        ({"grace": -1}, "grace"),
        ({"grace": 60}, "grace"),
    ],
)
def test_rejects_bad_numbers(kwargs: dict[str, Any], match: str) -> None:
    app, _ = _app()

    @app.tick(cron="* * * * *", tz="UTC", name="job")
    def job() -> None: ...

    with pytest.raises(ValueError, match=match):
        Ticker(app, **kwargs)


def test_system_clock_now_is_aware_utc() -> None:
    now = SystemClock().now()
    assert now.utcoffset() == timedelta(0)


async def test_system_clock_sleep() -> None:
    await SystemClock().sleep(0)


async def test_run_with_signals_sigterm() -> None:
    app, _ = _app()

    @app.tick(cron="0 0 1 1 *", tz="UTC", name="job")
    def job() -> None: ...

    clock = BlockingClock(_at("2026-11-04T11:29:30"))
    ticker = Ticker(app, clock=clock)
    before_term = signal.getsignal(signal.SIGTERM)
    before_int = signal.getsignal(signal.SIGINT)
    run = asyncio.create_task(ticker.run_with_signals())
    await asyncio.wait_for(clock.sleeping.wait(), 1)
    os.kill(os.getpid(), signal.SIGTERM)
    await asyncio.wait_for(run, 1)
    assert signal.getsignal(signal.SIGTERM) == before_term
    assert signal.getsignal(signal.SIGINT) == before_int
