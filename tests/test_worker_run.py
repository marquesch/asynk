from __future__ import annotations

import asyncio
import os
import signal
import tempfile
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from envelope_helpers import decode_delivery, publish_envelope

from asynk.app import App
from asynk.envelope import Envelope
from asynk.middleware import Next, TaskCall, TaskResult, TaskStatus
from asynk.runner import Runner
from asynk.transport import Delivery
from asynk.transport.memory import MemoryConsumer, MemoryProducer, memory_transport
from asynk.worker import Worker


def _setup(**kwargs: Any) -> tuple[App, MemoryProducer, MemoryConsumer, Worker]:
    producer, consumer = memory_transport()
    app = App("t", producer=producer, consumer=consumer)
    return app, producer, consumer, Worker(app, **kwargs)


def _collect(app: App) -> list[tuple[TaskResult, Delivery]]:
    seen: list[tuple[TaskResult, Delivery]] = []

    @app.middleware
    async def hook(call: TaskCall, next: Next) -> TaskResult:
        result = await next(call)
        seen.append((result, call.delivery))
        return result

    return seen


async def _drain(consumer: MemoryConsumer) -> list[Delivery]:
    return [d async for d in consumer.deliveries()]


async def _stop(worker: Worker, run: asyncio.Task[None]) -> None:
    worker.request_shutdown()
    await asyncio.wait_for(run, 1)


async def test_run_processes_published_tasks() -> None:
    app, producer, consumer, worker = _setup()
    seen = _collect(app)
    done = asyncio.Event()
    values: list[int] = []

    @app.task(name="rec")
    async def rec(n: int) -> int:
        values.append(n)
        if len(values) == 3:
            done.set()
        return n

    for n in range(3):
        await publish_envelope(producer, Envelope.create("rec", (n,), {}))
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(done.wait(), 1)
    await _stop(worker, run)
    assert sorted(values) == [0, 1, 2]
    assert len(seen) == 3
    assert all(r.status is TaskStatus.SUCCESS and d.settled for r, d in seen)
    assert not consumer.connected
    assert await _drain(consumer) == []


async def test_concurrency_limit_respected() -> None:
    app, producer, consumer, worker = _setup(concurrency=2)
    running = 0
    max_seen = 0
    finished = 0
    two_running = asyncio.Event()
    gate = asyncio.Event()
    all_done = asyncio.Event()

    @app.task(name="t")
    async def t() -> None:
        nonlocal running, max_seen, finished
        running += 1
        max_seen = max(max_seen, running)
        if running == 2:
            two_running.set()
        await asyncio.wait_for(gate.wait(), 1)
        running -= 1
        finished += 1
        if finished == 5:
            all_done.set()

    for _ in range(5):
        await publish_envelope(producer, Envelope.create("t", (), {}))
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(two_running.wait(), 1)
    for _ in range(100):
        await asyncio.sleep(0)
    assert running == 2
    gate.set()
    await asyncio.wait_for(all_done.wait(), 1)
    await _stop(worker, run)
    assert max_seen == 2


async def test_tasks_run_concurrently() -> None:
    app, producer, consumer, worker = _setup()
    seen = _collect(app)
    both = asyncio.Event()
    count = 0

    @app.task(name="meet")
    async def meet() -> None:
        nonlocal count
        count += 1
        if count == 2:
            both.set()
        await asyncio.wait_for(both.wait(), 1)

    await publish_envelope(producer, Envelope.create("meet", (), {}))
    await publish_envelope(producer, Envelope.create("meet", (), {}))
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(both.wait(), 1)
    await _stop(worker, run)
    assert [r.status for r, _ in seen] == [TaskStatus.SUCCESS, TaskStatus.SUCCESS]


async def test_idle_worker_shuts_down_promptly() -> None:
    _, _, consumer, worker = _setup()
    run = asyncio.create_task(worker.run())
    for _ in range(5):
        await asyncio.sleep(0)
    assert consumer.connected
    await _stop(worker, run)
    assert not consumer.connected


async def test_shutdown_waits_for_in_flight_task() -> None:
    app, producer, consumer, worker = _setup()
    seen = _collect(app)
    started = asyncio.Event()
    release = asyncio.Event()

    @app.task(name="slow")
    async def slow() -> str:
        started.set()
        await release.wait()
        return "done"

    await publish_envelope(producer, Envelope.create("slow", (), {}))
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), 1)
    worker.request_shutdown()
    for _ in range(10):
        await asyncio.sleep(0)
    assert not run.done()
    release.set()
    await asyncio.wait_for(run, 1)
    assert len(seen) == 1
    result, delivery = seen[0]
    assert result.status is TaskStatus.SUCCESS
    assert result.value == "done"
    assert delivery.settled
    assert await _drain(consumer) == []


async def test_shutdown_timeout_requeues_unfinished() -> None:
    app, producer, consumer, worker = _setup(shutdown_timeout=0.02)
    started = asyncio.Event()

    @app.task(name="sleepy")
    async def sleepy() -> None:
        started.set()
        await asyncio.Event().wait()  # never finishes: only the shutdown timeout ends it

    env = Envelope.create("sleepy", (), {})
    await publish_envelope(producer, env)
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), 1)
    t0 = time.monotonic()
    await _stop(worker, run)
    assert time.monotonic() - t0 < 1
    remaining = await _drain(consumer)
    assert [decode_delivery(d) for d in remaining] == [env]


async def test_failed_task_does_not_stop_loop() -> None:
    app, producer, consumer, worker = _setup()
    seen = _collect(app)
    ok_done = asyncio.Event()

    @app.task(name="boom")
    async def boom() -> None:
        raise ValueError("x")

    @app.task(name="ok")
    async def ok() -> None:
        ok_done.set()

    await publish_envelope(producer, Envelope.create("boom", (), {}))
    await publish_envelope(producer, Envelope.create("ok", (), {}))
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(ok_done.wait(), 1)
    await _stop(worker, run)
    assert {r.envelope.task: r.status for r, _ in seen} == {
        "boom": TaskStatus.FAILED,
        "ok": TaskStatus.SUCCESS,
    }


async def test_request_shutdown_is_idempotent() -> None:
    _, _, consumer, worker = _setup()
    worker.request_shutdown()
    worker.request_shutdown()
    run = asyncio.create_task(worker.run())
    worker.request_shutdown()
    await asyncio.wait_for(run, 1)
    worker.request_shutdown()
    assert not consumer.connected


async def test_run_with_signals_handles_sigterm() -> None:
    _, _, consumer, worker = _setup()
    before_term = signal.getsignal(signal.SIGTERM)
    before_int = signal.getsignal(signal.SIGINT)
    run = asyncio.create_task(worker.run_with_signals())
    for _ in range(5):
        await asyncio.sleep(0)
    assert consumer.connected
    os.kill(os.getpid(), signal.SIGTERM)
    await asyncio.wait_for(run, 1)
    assert not consumer.connected
    assert signal.getsignal(signal.SIGTERM) == before_term
    assert signal.getsignal(signal.SIGINT) == before_int


class _FailingConsumer:
    def __init__(self, inner: MemoryConsumer, events: list[str]) -> None:
        self.inner = inner
        self.events = events
        self.fail = asyncio.Event()

    @property
    def connected(self) -> bool:
        return self.inner.connected

    async def start(self) -> None:
        await self.inner.start()

    async def stop(self) -> None:
        self.events.append("stop")
        await self.inner.stop()

    async def deliveries(self) -> AsyncIterator[Delivery]:
        yield await anext(self.inner.deliveries())
        await self.fail.wait()
        raise ConnectionError("broker gone")


async def test_consumer_error_cancels_and_requeues_in_flight() -> None:
    producer, inner = memory_transport()
    events: list[str] = []
    consumer = _FailingConsumer(inner, events)
    app = App("t", producer=producer, consumer=consumer)
    worker = Worker(app)
    started = asyncio.Event()

    @app.task(name="slow")
    async def slow() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            events.append("task_exit")

    env = Envelope.create("slow", (), {})
    await publish_envelope(producer, env)
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), 1)
    consumer.fail.set()
    with pytest.raises(ConnectionError, match="broker gone"):
        await asyncio.wait_for(run, 1)
    assert events == ["task_exit", "stop"]
    remaining = await _drain(inner)
    assert [decode_delivery(d) for d in remaining] == [env]


def _listing(path: Path) -> list[Path]:
    return list(path.iterdir())


async def _until(predicate: Any, limit: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + limit
    while not predicate():
        if loop.time() > deadline:
            raise TimeoutError
        await asyncio.sleep(0.005)


async def test_worker_writes_probe_files_while_running(tmp_path: Path) -> None:
    _, _, _, worker = _setup(probe_dir=tmp_path / "p", heartbeat_interval=0.01)
    run = asyncio.create_task(worker.run())
    liveness, readiness = tmp_path / "p" / "liveness", tmp_path / "p" / "readiness"
    await _until(lambda: liveness.exists() and readiness.exists())
    first = liveness.stat().st_mtime_ns
    await _until(lambda: liveness.stat().st_mtime_ns > first)
    second = liveness.stat().st_mtime_ns
    await _until(lambda: liveness.stat().st_mtime_ns > second)
    await _stop(worker, run)


async def test_readiness_removed_when_shutdown_starts(tmp_path: Path) -> None:
    app, producer, _, worker = _setup(probe_dir=tmp_path, heartbeat_interval=0.01)
    started = asyncio.Event()
    release = asyncio.Event()

    @app.task(name="long")
    async def long() -> None:
        started.set()
        await release.wait()

    await publish_envelope(producer, Envelope.create("long", (), {}))
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), 1)
    await _until((tmp_path / "readiness").exists)
    worker.request_shutdown()
    await _until(lambda: not (tmp_path / "readiness").exists())
    assert not run.done()
    assert (tmp_path / "liveness").exists()
    first = (tmp_path / "liveness").stat().st_mtime_ns
    await _until(lambda: (tmp_path / "liveness").stat().st_mtime_ns > first)
    assert not (tmp_path / "readiness").exists()
    release.set()
    await asyncio.wait_for(run, 1)


async def test_readiness_removal_failure_is_logged_not_raised(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    _, _, _, worker = _setup(probe_dir=tmp_path, heartbeat_interval=0.01)
    run = asyncio.create_task(worker.run())
    await _until((tmp_path / "readiness").exists)
    assert worker._probes is not None

    def broken() -> None:
        raise OSError("boom")

    worker._probes.remove_readiness = broken  # type: ignore[method-assign]
    worker.request_shutdown()
    await asyncio.wait_for(run, 1)
    assert "failed to remove readiness probe" in caplog.text


async def test_probe_files_cleared_after_exit(tmp_path: Path) -> None:
    _, _, _, worker = _setup(probe_dir=tmp_path, heartbeat_interval=0.02)
    run = asyncio.create_task(worker.run())
    await _until((tmp_path / "liveness").exists)
    await _stop(worker, run)
    assert not (tmp_path / "liveness").exists()
    assert not (tmp_path / "readiness").exists()
    assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]


async def test_probe_files_cleared_after_error_exit(tmp_path: Path) -> None:
    _, _, consumer, worker = _setup(probe_dir=tmp_path, heartbeat_interval=0.02)
    run = asyncio.create_task(worker.run())
    await _until((tmp_path / "liveness").exists)
    run.cancel()
    await asyncio.gather(run, return_exceptions=True)
    assert not (tmp_path / "liveness").exists()
    assert not (tmp_path / "readiness").exists()
    assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]


async def test_no_files_when_probe_dir_is_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    constructed: list[Path] = []

    class RecordingProbeFiles:
        def __init__(self, directory: Path) -> None:
            constructed.append(directory)

        def __getattr__(self, name: str) -> Any:
            return lambda *args, **kwargs: None

    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    monkeypatch.setattr("asynk.runner.ProbeFiles", RecordingProbeFiles)
    _, _, _, worker = _setup(heartbeat_interval=0.02)
    run = asyncio.create_task(worker.run())
    # Absence of probe files is the behavior: only observable over several real heartbeats.
    await asyncio.sleep(0.05)
    await _stop(worker, run)
    assert constructed == []
    assert _listing(tmp_path) == []


@pytest.mark.parametrize(
    "kwargs",
    [
        {"concurrency": 0},
        {"heartbeat_interval": 0},
        {"heartbeat_interval": -1},
        {"shutdown_timeout": -1},
    ],
)
def test_worker_rejects_invalid_arguments(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        _setup(**kwargs)


def test_default_shutdown_timeout_is_ten_seconds() -> None:
    assert _setup()[3].shutdown_timeout == 10.0


def test_worker_accepts_zero_shutdown_timeout() -> None:
    _setup(shutdown_timeout=0)


def _lifecycle(app: App, consumer: MemoryConsumer, events: list[str]) -> None:
    @app.before_startup
    async def before_startup(runner: Runner) -> None:
        events.append(f"before_startup connected={consumer.connected}")

    @app.after_startup
    def after_startup(runner: Runner) -> None:
        events.append(f"after_startup connected={consumer.connected}")

    @app.before_shutdown
    async def before_shutdown(runner: Runner) -> None:
        events.append(f"before_shutdown connected={consumer.connected}")

    @app.after_shutdown
    async def after_shutdown(runner: Runner) -> None:
        events.append(f"after_shutdown connected={consumer.connected}")


async def test_lifecycle_hooks_run_in_order_around_the_consumer() -> None:
    app, _, consumer, worker = _setup()
    events: list[str] = []
    _lifecycle(app, consumer, events)
    started = asyncio.Event()

    @app.after_startup
    def mark_started(runner: Runner) -> None:
        started.set()

    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), 1)
    assert events == ["before_startup connected=False", "after_startup connected=True"]
    await _stop(worker, run)
    assert events[2:] == ["before_shutdown connected=True", "after_shutdown connected=False"]


async def test_lifecycle_hooks_receive_the_worker() -> None:
    app, _, _, worker = _setup()
    received: list[Runner] = []

    @app.before_startup
    async def hook(w: Runner) -> None:
        received.append(w)

    started = asyncio.Event()

    @app.after_startup
    def mark_started(runner: Runner) -> None:
        started.set()

    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), 1)
    await _stop(worker, run)
    assert received == [worker]


async def test_lifecycle_hooks_see_the_worker_role() -> None:
    app, _, _, worker = _setup()
    roles: list[str] = []

    @app.before_startup
    def hook(runner: Runner) -> None:
        roles.append(runner.role)

    started = asyncio.Event()

    @app.after_startup
    def mark_started(runner: Runner) -> None:
        started.set()

    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), 1)
    await _stop(worker, run)
    assert roles == ["worker"]
    assert worker.role == "worker"
    assert isinstance(worker, Runner)


async def test_before_shutdown_runs_before_in_flight_tasks_are_drained() -> None:
    app, producer, _, worker = _setup()
    order: list[str] = []
    started, release = asyncio.Event(), asyncio.Event()

    @app.task(name="slow")
    async def slow() -> None:
        started.set()
        await release.wait()  # in flight until the hook has run
        order.append("task done")

    @app.before_shutdown
    async def hook(w: Runner) -> None:
        order.append("before_shutdown")
        release.set()

    await publish_envelope(producer, Envelope.create("slow", (), {}))
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), 1)
    await _stop(worker, run)
    assert order == ["before_shutdown", "task done"]


async def test_failing_before_startup_raises_and_never_starts_the_consumer() -> None:
    app, _, consumer, worker = _setup()
    events: list[str] = []
    _lifecycle(app, consumer, events)

    @app.before_startup
    async def broken(w: Runner) -> None:
        raise RuntimeError("no db")

    with pytest.raises(RuntimeError, match="no db"):
        await asyncio.wait_for(worker.run(), 1)
    assert events == ["before_startup connected=False"]
    assert not consumer.connected


async def test_failing_after_startup_raises_after_cleaning_up() -> None:
    app, _, consumer, worker = _setup()
    events: list[str] = []

    @app.after_startup
    async def broken(w: Runner) -> None:
        raise RuntimeError("warmup failed")

    @app.after_shutdown
    async def after(w: Runner) -> None:
        events.append("after_shutdown")

    with pytest.raises(RuntimeError, match="warmup failed"):
        await asyncio.wait_for(worker.run(), 1)
    assert not consumer.connected
    assert events == ["after_shutdown"]


async def test_failing_before_shutdown_still_drains_and_cleans_up_then_raises() -> None:
    app, producer, consumer, worker = _setup()
    done: list[str] = []
    started, release = asyncio.Event(), asyncio.Event()

    @app.task(name="slow")
    async def slow() -> None:
        started.set()
        await release.wait()  # in flight until the hook has run
        done.append("task")

    @app.before_shutdown
    async def broken(w: Runner) -> None:
        release.set()
        raise RuntimeError("flush failed")

    await publish_envelope(producer, Envelope.create("slow", (), {}))
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), 1)
    worker.request_shutdown()
    with pytest.raises(RuntimeError, match="flush failed"):
        await asyncio.wait_for(run, 2)
    assert done == ["task"]
    assert not consumer.connected


async def test_failing_after_shutdown_raises() -> None:
    app, _, _, worker = _setup()

    @app.after_shutdown
    async def broken(w: Runner) -> None:
        raise RuntimeError("close failed")

    started = asyncio.Event()

    @app.after_startup
    def mark_started(runner: Runner) -> None:
        started.set()

    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), 1)
    worker.request_shutdown()
    with pytest.raises(RuntimeError, match="close failed"):
        await asyncio.wait_for(run, 1)


async def test_every_after_shutdown_hook_runs_when_several_fail() -> None:
    app, _, _, worker = _setup()
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

    started = asyncio.Event()

    @app.after_startup
    def mark_started(runner: Runner) -> None:
        started.set()

    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), 1)
    worker.request_shutdown()
    with pytest.raises(ExceptionGroup) as info:
        await asyncio.wait_for(run, 1)
    assert [str(e) for e in info.value.exceptions] == ["one", "two"]
    assert ran == ["ran"]
