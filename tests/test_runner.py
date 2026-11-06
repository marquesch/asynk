import asyncio
import logging
import os
import signal
from pathlib import Path
from typing import Any, ClassVar

import pytest

from asynk.app import App
from asynk.runner import Role, Runner


class _FakeRunner(Runner):
    role: ClassVar[Role] = "worker"

    def __init__(self, app: App, **kwargs: Any) -> None:
        super().__init__(app, **kwargs)
        self.shutdown_requests = 0
        self.serve_error: BaseException | None = None
        self.started = asyncio.Event()

    def _on_shutdown_requested(self) -> None:
        self.shutdown_requests += 1

    async def _serve(self) -> None:
        self._beat(ready=True)
        self.started.set()
        try:
            if self.serve_error is not None:
                raise self.serve_error
            await self._race(asyncio.Event().wait())
        finally:
            self._clear_probes()


def _listing(path: Path) -> list[Path]:
    return list(path.iterdir())


def _runner(**kwargs: Any) -> _FakeRunner:
    return _FakeRunner(App("t"), **kwargs)


@pytest.mark.parametrize("interval", [0, -1])
def test_rejects_non_positive_heartbeat_interval(interval: float) -> None:
    with pytest.raises(ValueError, match=f"heartbeat_interval must be > 0, got {interval}"):
        _runner(heartbeat_interval=interval)


def test_keeps_its_constructor_arguments(tmp_path: Path) -> None:
    runner = _runner(probe_dir=tmp_path, heartbeat_interval=2.5)
    assert runner.app.name == "t"
    assert runner.probe_dir == tmp_path
    assert runner.heartbeat_interval == 2.5


def test_request_shutdown_is_idempotent(caplog: pytest.LogCaptureFixture) -> None:
    runner = _runner()
    with caplog.at_level(logging.INFO, logger="asynk"):
        runner.request_shutdown()
        runner.request_shutdown()
    assert runner.shutdown_requests == 1
    assert [r.getMessage() for r in caplog.records].count("worker shutdown requested") == 1


async def test_request_shutdown_ends_run() -> None:
    runner = _runner()
    run = asyncio.create_task(runner.run())
    await runner.started.wait()
    runner.request_shutdown()
    await asyncio.wait_for(run, 1)


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT])
async def test_run_with_signals_requests_shutdown_and_restores_handlers(
    sig: signal.Signals,
) -> None:
    runner = _runner()
    before = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
    run = asyncio.create_task(runner.run_with_signals())
    await runner.started.wait()
    os.kill(os.getpid(), sig)
    await asyncio.wait_for(run, 1)
    assert runner.shutdown_requests == 1
    assert {s: signal.getsignal(s) for s in before} == before


def test_beat_and_clear_write_and_remove_probe_files(tmp_path: Path) -> None:
    runner = _runner(probe_dir=tmp_path)
    runner._beat(ready=True)
    assert (tmp_path / "liveness").exists()
    assert (tmp_path / "readiness").exists()
    runner._beat(ready=False)
    assert not (tmp_path / "readiness").exists()
    runner._clear_probes()
    assert not (tmp_path / "liveness").exists()


def test_probe_helpers_do_nothing_without_a_probe_dir() -> None:
    runner = _runner()
    runner._beat(ready=True)
    runner._clear_probes()


def test_probe_helpers_log_and_swallow_os_errors(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    blocker = tmp_path / "blocker"
    blocker.write_text("")
    runner = _runner(probe_dir=blocker / "p")
    runner._beat(ready=True)
    assert "failed to write probe files" in caplog.text

    def broken_clear() -> None:
        raise OSError("boom")

    assert runner._probes is not None
    runner._probes.clear = broken_clear  # type: ignore[method-assign]
    runner._clear_probes()
    assert "failed to clear probe files" in caplog.text


async def test_probe_files_cleared_when_serve_raises(tmp_path: Path) -> None:
    runner = _runner(probe_dir=tmp_path)
    runner.serve_error = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="boom"):
        await runner.run()
    assert _listing(tmp_path) == []


async def test_probe_files_cleared_when_run_is_cancelled(tmp_path: Path) -> None:
    runner = _runner(probe_dir=tmp_path)
    run = asyncio.create_task(runner.run())
    await runner.started.wait()
    assert (tmp_path / "liveness").exists()
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    assert _listing(tmp_path) == []


async def test_race_returns_the_awaitables_value() -> None:
    async def value() -> int:
        return 7

    assert await _runner()._race(value()) == (True, 7)


async def test_race_reports_shutdown_and_cancels_the_awaitable() -> None:
    runner = _runner()
    cancelled = asyncio.Event()

    async def forever() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    race = asyncio.create_task(runner._race(forever()))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    runner.request_shutdown()
    assert await asyncio.wait_for(race, 1) == (False, None)
    assert cancelled.is_set()


async def test_race_returns_unfinished_when_shutdown_was_already_requested() -> None:
    runner = _runner()
    runner.request_shutdown()
    finished, _ = await runner._race(asyncio.Event().wait())
    assert not finished


@pytest.mark.parametrize("error", [ValueError("boom"), StopAsyncIteration()])
async def test_race_propagates_the_awaitables_exception(error: Exception) -> None:
    async def fail() -> None:
        raise error

    with pytest.raises(type(error)):
        await _runner()._race(fail())


async def test_race_cancels_the_awaitable_when_the_caller_is_cancelled() -> None:
    runner = _runner()
    inner: list[asyncio.Task[Any]] = []

    async def forever() -> None:
        inner.append(asyncio.current_task())  # type: ignore[arg-type]
        await asyncio.Event().wait()

    race = asyncio.create_task(runner._race(forever()))
    await asyncio.sleep(0.01)
    race.cancel()
    with pytest.raises(asyncio.CancelledError):
        await race
    assert inner[0].cancelled()
    assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
