from __future__ import annotations

import asyncio
import logging
import os
import signal
from collections.abc import Callable
from datetime import UTC, datetime, timedelta, tzinfo
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from envelope_helpers import decode_delivery

from asynk import EncodeError, NotConfiguredError
from asynk.app import App
from asynk.envelope import Envelope
from asynk.probes import ProbeFiles
from asynk.runner import Runner
from asynk.scheduler import Scheduler, SystemClock
from asynk.transport.memory import MemoryConsumer, memory_transport


def _at(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


class FakeClock:
    """Clock whose `sleep` advances `now` instead of waiting.

    Once `now` reaches `until`, the next `sleep` calls `on_limit` and blocks
    forever, so the scheduler under test stops through its normal shutdown path.
    `warps[n]` is added to `now` after the n-th sleep (0-based), simulating a
    stall (positive) or the wall clock stepping back (negative). With `zone`,
    `now()` returns the time in that zone instead of UTC.
    """

    def __init__(self, start: datetime, until: datetime, zone: tzinfo | None = None) -> None:
        self.now_value = start
        self.zone = zone
        self.until = until
        self.sleeps: list[float] = []
        self.warps: dict[int, timedelta] = {}
        self.on_limit: Callable[[], None] = lambda: None

    def now(self) -> datetime:
        return self.now_value if self.zone is None else self.now_value.astimezone(self.zone)

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
    return [decode_delivery(d) async for d in consumer.deliveries()]


async def _run(app: App, clock: FakeClock, **kwargs: Any) -> Scheduler:
    scheduler = Scheduler(app, clock=clock, **kwargs)
    clock.on_limit = scheduler.request_shutdown
    await asyncio.wait_for(scheduler.run(), 1)
    return scheduler


def _slots(envelopes: list[Envelope]) -> list[tuple[str, Any]]:
    return [(e.task, e.metadata["scheduled_at"]) for e in envelopes]


async def test_fires_on_slot_with_fixed_args_and_kwargs() -> None:
    app, consumer = _app()

    @app.scheduled_task(cron="30 * * * *", tz="UTC", args=[1, "a"], kwargs={"b": [2]}, name="job")
    def job(n: int, s: str, *, b: list[int]) -> None: ...

    clock = FakeClock(_at("2026-11-04T11:29:00"), until=_at("2026-11-04T11:31:00"))
    await _run(app, clock)
    [env] = await _published(consumer)
    assert env.task == "job"
    assert env.args == (1, "a")
    assert env.kwargs == {"b": [2]}


async def test_metadata_has_idempotency_key_and_scheduled_at() -> None:
    app, consumer = _app()

    @app.scheduled_task(cron="30 8 * * *", tz="America/Sao_Paulo", name="job")
    def job() -> None: ...

    clock = FakeClock(_at("2026-11-04T11:00:00"), until=_at("2026-11-04T12:00:00"))
    await _run(app, clock)
    [env] = await _published(consumer)
    assert env.metadata == {
        "idempotency_key": "job:2026-11-04T11:30:00+00:00",
        "scheduled_at": "2026-11-04T11:30:00+00:00",
    }


async def test_two_schedulers_same_slot_same_key() -> None:
    keys = []
    for _ in range(2):
        app, consumer = _app()

        @app.scheduled_task(cron="*/5 * * * *", tz="Europe/Berlin", name="job")
        def job() -> None: ...

        clock = FakeClock(_at("2026-11-04T11:02:17"), until=_at("2026-11-04T11:06:00"))
        await _run(app, clock)
        [env] = await _published(consumer)
        keys.append(env.metadata["idempotency_key"])
    assert keys == ["job:2026-11-04T11:05:00+00:00"] * 2


async def test_nothing_fires_at_startup() -> None:
    app, consumer = _app()

    @app.scheduled_task(cron="30 11 * * *", tz="UTC", name="job")
    def job() -> None: ...

    # Starting exactly on a slot does not fire it.
    clock = FakeClock(_at("2026-11-04T11:30:00"), until=_at("2026-11-04T11:40:00"))
    await _run(app, clock)
    assert await _published(consumer) == []


async def test_only_due_scheduled_tasks_fire() -> None:
    app, consumer = _app()

    @app.scheduled_task(cron="*/10 * * * *", tz="UTC", name="often")
    def often() -> None: ...

    @app.scheduled_task(cron="0 * * * *", tz="UTC", name="hourly")
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

    @app.scheduled_task(cron="30 * * * *", tz="UTC", name="job")
    def job() -> None: ...

    clock = FakeClock(_at("2026-11-04T11:29:30"), until=_at("2026-11-04T11:31:00"))
    clock.warps[0] = timedelta(seconds=20)  # wakes 20 s after the slot
    await _run(app, clock, grace=30, heartbeat_interval=30)
    assert _slots(await _published(consumer)) == [("job", "2026-11-04T11:30:00+00:00")]


async def test_stall_beyond_grace_skips_missed_slots(caplog: pytest.LogCaptureFixture) -> None:
    app, consumer = _app()

    @app.scheduled_task(cron="0 * * * *", tz="UTC", name="job")
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

    @app.scheduled_task(cron="0 * * * *", tz="UTC", name="job")
    def job() -> None: ...

    clock = FakeClock(_at("2026-11-04T10:59:00"), until=_at("2026-11-04T15:01:00"))
    clock.warps[0] = timedelta(hours=3, minutes=30)  # first wake lands at 14:29:30
    await _run(app, clock)
    assert _slots(await _published(consumer)) == [("job", "2026-11-04T15:00:00+00:00")]


async def test_catch_up_option_does_not_fire_missed_slots() -> None:
    app, consumer = _app()

    # `catch_up` is no longer a scheduled-task feature: it is just an opaque task option.
    @app.scheduled_task(cron="0 * * * *", tz="UTC", catch_up=True, name="job")
    def job() -> None: ...

    clock = FakeClock(_at("2026-11-04T10:59:30"), until=_at("2026-11-04T15:01:00"))
    clock.warps[0] = timedelta(hours=3, minutes=30)  # first wake lands at 14:29:35
    await _run(app, clock)
    assert _slots(await _published(consumer)) == [("job", "2026-11-04T15:00:00+00:00")]


async def test_clock_step_back_does_not_refire() -> None:
    app, consumer = _app()

    @app.scheduled_task(cron="* * * * *", tz="UTC", name="job")
    def job() -> None: ...

    clock = FakeClock(_at("2026-11-04T11:29:30"), until=_at("2026-11-04T11:31:30"))
    clock.warps[1] = timedelta(minutes=-2)  # fires 11:30, then steps back to 11:29
    await _run(app, clock, heartbeat_interval=30)
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
        async def publish(self, envelope: Envelope, body: bytes) -> Any:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ConnectionError("broker down")
            return await real.publish(envelope, body)

    app.producer = Flaky()

    @app.scheduled_task(cron="* * * * *", tz="UTC", name="job")
    def job() -> None: ...

    clock = FakeClock(_at("2026-11-04T11:29:30"), until=_at("2026-11-04T11:31:00"))
    with caplog.at_level(logging.ERROR, logger="asynk"):
        await _run(app, clock)
    assert _slots(await _published(consumer)) == [("job", "2026-11-04T11:31:00+00:00")]
    [record] = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert record.exc_info is not None
    assert isinstance(record.exc_info[1], ConnectionError)


class HangingProducer:
    """Publishes normally `ok` times, then every publish hangs until cancelled."""

    def __init__(self, inner: Any, ok: int) -> None:
        self.inner = inner
        self.ok = ok
        self.calls = 0
        self.hanging = asyncio.Event()
        self.cancelled = 0

    async def publish(self, envelope: Envelope, body: bytes) -> Any:
        self.calls += 1
        if self.calls > self.ok:
            self.hanging.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled += 1
                raise
        return await self.inner.publish(envelope, body)


def _leaked_tasks() -> list[asyncio.Task[Any]]:
    return [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]


async def _shutdown_while_hanging(
    app: App, producer: HangingProducer, clock: FakeClock, probe_dir: Path
) -> None:
    scheduler = Scheduler(app, clock=clock, probe_dir=probe_dir)
    run = asyncio.create_task(scheduler.run())
    await asyncio.wait_for(producer.hanging.wait(), 1)
    scheduler.request_shutdown()
    await asyncio.wait_for(run, 1)
    assert producer.cancelled == 1
    assert _leaked_tasks() == []
    assert not (probe_dir / "liveness").exists()


async def test_publish_cancelled_without_shutdown_is_a_failed_publish(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app, consumer = _app()
    real = app.producer
    calls: list[str] = []

    class SelfCancelling:
        async def publish(self, envelope: Envelope, body: bytes) -> Any:
            calls.append(str(envelope.metadata["scheduled_at"]))
            if len(calls) == 1:
                raise asyncio.CancelledError
            assert real is not None
            return await real.publish(envelope, body)

    app.producer = SelfCancelling()

    @app.scheduled_task(cron="* * * * *", tz="UTC", name="job")
    def job() -> None: ...

    clock = FakeClock(_at("2026-11-04T11:29:30"), until=_at("2026-11-04T11:31:00"))
    with caplog.at_level(logging.WARNING, logger="asynk"):
        await _run(app, clock)
    assert calls == ["2026-11-04T11:30:00+00:00", "2026-11-04T11:31:00+00:00"]
    assert _slots(await _published(consumer)) == [("job", "2026-11-04T11:31:00+00:00")]
    [record] = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert record.levelno == logging.ERROR
    assert "'job'" in record.getMessage()
    assert "2026-11-04T11:30:00+00:00" in record.getMessage()
    assert "cancelled" in record.getMessage()
    assert "shutdown" not in record.getMessage()
    assert _leaked_tasks() == []


async def test_shutdown_interrupts_hung_publish(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    app, consumer = _app()
    producer = HangingProducer(app.producer, ok=0)
    app.producer = producer

    @app.scheduled_task(cron="30 * * * *", tz="UTC", name="job")
    def job() -> None: ...

    clock = FakeClock(_at("2026-11-04T11:29:30"), until=_at("2026-11-04T12:00:00"))
    with caplog.at_level(logging.WARNING, logger="asynk"):
        await _shutdown_while_hanging(app, producer, clock, tmp_path)
    assert await _published(consumer) == []
    [record] = [r for r in caplog.records if "cancel" in r.getMessage()]
    assert record.levelno == logging.WARNING
    assert "'job'" in record.getMessage()
    assert "2026-11-04T11:30:00+00:00" in record.getMessage()


async def test_shutdown_interrupts_sleep() -> None:
    app, consumer = _app()

    @app.scheduled_task(cron="0 0 1 1 *", tz="UTC", name="job")
    def job() -> None: ...

    clock = BlockingClock(_at("2026-11-04T11:29:30"))
    scheduler = Scheduler(app, clock=clock)
    run = asyncio.create_task(scheduler.run())
    await asyncio.wait_for(clock.sleeping.wait(), 1)
    scheduler.request_shutdown()
    await asyncio.wait_for(run, 1)
    assert await _published(consumer) == []


async def test_probe_file_written_and_cleared(tmp_path: Path) -> None:
    app, _ = _app()

    @app.scheduled_task(cron="0 0 1 1 *", tz="UTC", name="job")
    def job() -> None: ...

    clock = BlockingClock(_at("2026-11-04T11:29:30"))
    scheduler = Scheduler(app, clock=clock, probe_dir=tmp_path)
    run = asyncio.create_task(scheduler.run())
    await asyncio.wait_for(clock.sleeping.wait(), 1)
    assert (tmp_path / "liveness").exists()
    assert not (tmp_path / "readiness").exists()
    scheduler.request_shutdown()
    await asyncio.wait_for(run, 1)
    assert not (tmp_path / "liveness").exists()


async def test_probe_write_failure_is_logged_not_raised(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    app, _ = _app()

    @app.scheduled_task(cron="0 0 1 1 *", tz="UTC", name="job")
    def job() -> None: ...

    blocker = tmp_path / "file"
    blocker.write_text("")
    clock = BlockingClock(_at("2026-11-04T11:29:30"))
    scheduler = Scheduler(app, clock=clock, probe_dir=blocker / "probes")
    run = asyncio.create_task(scheduler.run())
    with caplog.at_level(logging.ERROR, logger="asynk"):
        await asyncio.wait_for(clock.sleeping.wait(), 1)
        scheduler.request_shutdown()
        await asyncio.wait_for(run, 1)
    assert any("probe" in r.getMessage() for r in caplog.records)


def test_requires_producer() -> None:
    app = App("t")

    @app.scheduled_task(cron="* * * * *", tz="UTC", name="job")
    def job() -> None: ...

    with pytest.raises(NotConfiguredError):
        Scheduler(app)


def test_requires_scheduled_tasks() -> None:
    app, _ = _app()
    with pytest.raises(ValueError, match="no scheduled tasks"):
        Scheduler(app)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"heartbeat_interval": 0}, "heartbeat_interval"),
        ({"grace": -1}, "grace"),
        ({"grace": 60}, "grace"),
    ],
)
def test_rejects_bad_numbers(kwargs: dict[str, Any], match: str) -> None:
    app, _ = _app()

    @app.scheduled_task(cron="* * * * *", tz="UTC", name="job")
    def job() -> None: ...

    with pytest.raises(ValueError, match=match):
        Scheduler(app, **kwargs)


def test_max_sleep_is_not_a_parameter() -> None:
    app, _ = _app()

    @app.scheduled_task(cron="* * * * *", tz="UTC", name="job")
    def job() -> None: ...

    with pytest.raises(TypeError):
        Scheduler(app, max_sleep=1)  # type: ignore[call-arg]


def test_system_clock_now_is_aware_utc() -> None:
    now = SystemClock().now()
    assert now.utcoffset() == timedelta(0)


async def test_system_clock_sleep() -> None:
    await SystemClock().sleep(0)


async def test_run_with_signals_sigterm() -> None:
    app, _ = _app()

    @app.scheduled_task(cron="0 0 1 1 *", tz="UTC", name="job")
    def job() -> None: ...

    clock = BlockingClock(_at("2026-11-04T11:29:30"))
    scheduler = Scheduler(app, clock=clock)
    before_term = signal.getsignal(signal.SIGTERM)
    before_int = signal.getsignal(signal.SIGINT)
    run = asyncio.create_task(scheduler.run_with_signals())
    await asyncio.wait_for(clock.sleeping.wait(), 1)
    os.kill(os.getpid(), signal.SIGTERM)
    await asyncio.wait_for(run, 1)
    assert signal.getsignal(signal.SIGTERM) == before_term
    assert signal.getsignal(signal.SIGINT) == before_int


async def test_sleep_is_capped_by_heartbeat_interval() -> None:
    app, _ = _app()

    @app.scheduled_task(cron="0 12 * * *", tz="UTC", name="job")
    def job() -> None: ...

    clock = FakeClock(_at("2026-11-04T11:58:00"), until=_at("2026-11-04T11:59:00"))
    await _run(app, clock, heartbeat_interval=7)
    assert clock.sleeps[:3] == [7, 7, 7]
    assert max(clock.sleeps) == 7


def _count_beats(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    beats: list[float] = []
    real = ProbeFiles.beat

    def counting(self: ProbeFiles, *, ready: bool) -> None:
        assert ready is False
        beats.append(len(beats))
        real(self, ready=ready)

    monkeypatch.setattr(ProbeFiles, "beat", counting)
    return beats


async def test_beats_repeatedly_while_idle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    beats = _count_beats(monkeypatch)
    app, _ = _app()

    @app.scheduled_task(cron="0 0 1 1 *", tz="UTC", name="job")
    def job() -> None: ...

    clock = FakeClock(_at("2026-11-04T11:00:00"), until=_at("2026-11-04T12:00:00"))
    await _run(app, clock, probe_dir=tmp_path, heartbeat_interval=5)
    # One hour idle at one beat per 5 s, plus the beat at start.
    assert len(beats) >= 720
    assert max(clock.sleeps) == 5
    assert not (tmp_path / "liveness").exists()


async def test_hung_publish_stops_beating(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    beats = _count_beats(monkeypatch)
    app, _ = _app()
    producer = HangingProducer(app.producer, ok=0)
    app.producer = producer

    @app.scheduled_task(cron="30 * * * *", tz="UTC", name="job")
    def job() -> None: ...

    # A 1 ms heartbeat: a beat from anywhere but the stuck loop would show up.
    clock = FakeClock(_at("2026-11-04T11:29:59.990"), until=_at("2026-11-04T12:00:00"))
    scheduler = Scheduler(app, clock=clock, probe_dir=tmp_path, heartbeat_interval=0.001)
    run = asyncio.create_task(scheduler.run())
    await asyncio.wait_for(producer.hanging.wait(), 1)
    stuck_at = len(beats)
    # Absence of beats is the behavior: it can only be observed over real time.
    await asyncio.sleep(0.02)  # 20 real heartbeat intervals
    assert len(beats) == stuck_at
    scheduler.request_shutdown()
    await asyncio.wait_for(run, 1)
    assert _leaked_tasks() == []
    assert not (tmp_path / "liveness").exists()


async def test_non_utc_clock_judges_lateness_across_fall_back() -> None:
    app, consumer = _app()

    @app.scheduled_task(cron="0 * * * *", tz="America/New_York", name="job")
    def job() -> None: ...

    # 00:59:30 EDT. A stall wakes the scheduler at 06:00:05Z, the second 01:00
    # (EST), so the 01:00 EDT slot (05:00Z) is an hour late and is skipped.
    clock = FakeClock(
        _at("2026-11-01T04:59:30"),
        until=_at("2026-11-01T08:01:00"),
        zone=ZoneInfo("America/New_York"),
    )
    clock.warps[0] = timedelta(hours=1, seconds=30)  # 04:59:35Z -> 06:00:05Z
    await _run(app, clock)
    assert _slots(await _published(consumer)) == [
        ("job", "2026-11-01T07:00:00+00:00"),
        ("job", "2026-11-01T08:00:00+00:00"),
    ]


def _hooked_app() -> tuple[App, list[str]]:
    app, _ = _app()
    events: list[str] = []

    @app.scheduled_task(cron="0 0 1 1 *", tz="UTC", name="job")
    def job() -> None: ...

    return app, events


async def _run_blocking(app: App, **kwargs: Any) -> Scheduler:
    clock = BlockingClock(_at("2026-11-04T11:29:30"))
    scheduler = Scheduler(app, clock=clock, **kwargs)
    run = asyncio.create_task(scheduler.run())
    await asyncio.wait_for(clock.sleeping.wait(), 1)
    scheduler.request_shutdown()
    await asyncio.wait_for(run, 1)
    return scheduler


async def test_lifecycle_hooks_run_in_order_and_receive_the_scheduler() -> None:
    app, events = _hooked_app()
    received: list[Runner] = []

    @app.before_startup
    async def before_startup(runner: Runner) -> None:
        received.append(runner)
        events.append("before_startup")

    @app.after_startup
    def after_startup(runner: Runner) -> None:
        received.append(runner)
        events.append("after_startup")

    @app.before_shutdown
    async def before_shutdown(runner: Runner) -> None:
        received.append(runner)
        events.append("before_shutdown")

    @app.after_shutdown
    def after_shutdown(runner: Runner) -> None:
        received.append(runner)
        events.append("after_shutdown")

    scheduler = await _run_blocking(app)
    assert events == ["before_startup", "after_startup", "before_shutdown", "after_shutdown"]
    assert received == [scheduler] * 4
    assert all(r.role == "scheduler" for r in received)
    assert scheduler.role == "scheduler"


async def test_hooks_run_around_the_loop_and_probes(tmp_path: Path) -> None:
    app, events = _hooked_app()
    live = tmp_path / "liveness"

    @app.before_startup
    def before_startup(runner: Runner) -> None:
        events.append(f"before_startup live={live.exists()}")

    @app.after_startup
    def after_startup(runner: Runner) -> None:
        events.append(f"after_startup live={live.exists()}")

    @app.before_shutdown
    def before_shutdown(runner: Runner) -> None:
        events.append(f"before_shutdown live={live.exists()}")

    @app.after_shutdown
    def after_shutdown(runner: Runner) -> None:
        events.append(f"after_shutdown live={live.exists()}")

    await _run_blocking(app, probe_dir=tmp_path)
    assert events == [
        "before_startup live=False",
        "after_startup live=True",
        "before_shutdown live=True",
        "after_shutdown live=False",
    ]


async def test_nothing_fires_before_after_startup() -> None:
    app, _ = _app()
    fired: list[int] = []

    @app.scheduled_task(cron="* * * * *", tz="UTC", name="job")
    def job() -> None: ...

    @app.after_startup
    def after_startup(runner: Runner) -> None:
        fired.append(len(runner.app.scheduled_tasks))

    clock = FakeClock(_at("2026-11-04T11:29:30"), _at("2026-11-04T11:31:00"))
    await _run(app, clock)
    assert fired == [1]


async def test_failing_before_startup_raises_and_skips_the_loop() -> None:
    app, events = _hooked_app()

    @app.before_startup
    async def broken(runner: Runner) -> None:
        raise RuntimeError("no telemetry")

    @app.after_startup
    def after_startup(runner: Runner) -> None:
        events.append("after_startup")

    @app.after_shutdown
    def after_shutdown(runner: Runner) -> None:
        events.append("after_shutdown")

    clock = BlockingClock(_at("2026-11-04T11:29:30"))
    scheduler = Scheduler(app, clock=clock)
    with pytest.raises(RuntimeError, match="no telemetry"):
        await asyncio.wait_for(scheduler.run(), 1)
    assert events == []
    assert not clock.sleeping.is_set()


async def test_failing_after_startup_raises_after_cleaning_up(tmp_path: Path) -> None:
    app, events = _hooked_app()

    @app.after_startup
    async def broken(runner: Runner) -> None:
        raise RuntimeError("warmup failed")

    @app.after_shutdown
    def after_shutdown(runner: Runner) -> None:
        events.append(f"after_shutdown live={(tmp_path / 'liveness').exists()}")

    clock = BlockingClock(_at("2026-11-04T11:29:30"))
    scheduler = Scheduler(app, clock=clock, probe_dir=tmp_path)
    with pytest.raises(RuntimeError, match="warmup failed"):
        await asyncio.wait_for(scheduler.run(), 1)
    assert events == ["after_shutdown live=False"]
    assert not clock.sleeping.is_set()


async def test_failing_before_shutdown_cleans_up_then_raises(tmp_path: Path) -> None:
    app, events = _hooked_app()

    @app.before_shutdown
    async def broken(runner: Runner) -> None:
        raise RuntimeError("flush failed")

    @app.before_shutdown
    def skipped(runner: Runner) -> None:
        events.append("skipped")

    @app.after_shutdown
    def after_shutdown(runner: Runner) -> None:
        events.append(f"after_shutdown live={(tmp_path / 'liveness').exists()}")

    clock = BlockingClock(_at("2026-11-04T11:29:30"))
    scheduler = Scheduler(app, clock=clock, probe_dir=tmp_path)
    run = asyncio.create_task(scheduler.run())
    await asyncio.wait_for(clock.sleeping.wait(), 1)
    scheduler.request_shutdown()
    with pytest.raises(RuntimeError, match="flush failed"):
        await asyncio.wait_for(run, 1)
    assert events == ["after_shutdown live=False"]
    assert not _leaked_tasks()


async def test_failing_after_shutdown_raises() -> None:
    app, _ = _hooked_app()

    @app.after_shutdown
    async def broken(runner: Runner) -> None:
        raise RuntimeError("close failed")

    with pytest.raises(RuntimeError, match="close failed"):
        await _run_blocking(app)


async def test_every_after_shutdown_hook_runs_when_several_fail() -> None:
    app, _ = _hooked_app()
    ran: list[str] = []

    @app.after_shutdown
    def one(runner: Runner) -> None:
        raise RuntimeError("one")

    @app.after_shutdown
    def middle(runner: Runner) -> None:
        ran.append("ran")

    @app.after_shutdown
    def two(runner: Runner) -> None:
        raise RuntimeError("two")

    with pytest.raises(ExceptionGroup) as info:
        await _run_blocking(app)
    assert [str(e) for e in info.value.exceptions] == ["one", "two"]
    assert ran == ["ran"]


async def test_scheduler_rejects_unencodable_schedule_args_at_startup() -> None:
    app, _ = _app()

    @app.scheduled_task(cron="* * * * *", args=[object()])
    def bad() -> None: ...

    with pytest.raises(EncodeError):
        Scheduler(app)
