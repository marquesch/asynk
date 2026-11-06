from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from asynk.app import App
from asynk.cli import CliError, load_app, main
from asynk.probes import ProbeFiles

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_load_app_ok() -> None:
    assert isinstance(load_app("tests.fixture_app:app"), App)


@pytest.mark.parametrize(
    "path",
    ["nocolon", "no_such_module:app", "tests.fixture_app:missing", "tests.fixture_app:__name__"],
)
def test_load_app_errors(path: str) -> None:
    with pytest.raises(CliError):
        load_app(path)


def test_health_exit_codes(tmp_path: Path) -> None:
    args = ["--max-age", "5", "--probe-dir", str(tmp_path)]
    assert main(["health", "liveness", *args]) == 1
    ProbeFiles(tmp_path).beat(ready=True)
    assert main(["health", "liveness", *args]) == 0
    assert main(["health", "readiness", *args]) == 0
    ProbeFiles(tmp_path).beat(ready=False)
    assert main(["health", "readiness", *args]) == 1


def test_health_missing_dir_is_unhealthy(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(["health", "readiness", "--max-age", "5", "--probe-dir", str(tmp_path / "nope")])
    assert code == 1
    assert "Traceback" not in capsys.readouterr().err


def test_worker_bad_app_path_returns_2_without_traceback(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["worker", "nocolon"]) == 2
    err = capsys.readouterr().err
    assert err.startswith("asynk:")
    assert "Traceback" not in err


def test_worker_subprocess_lifecycle(tmp_path: Path) -> None:
    env = {**os.environ, "PYTHONPATH": str(REPO_ROOT)}
    base = ["uv", "run", "asynk"]
    probe = ["--probe-dir", str(tmp_path)]
    proc = subprocess.Popen(
        [*base, "worker", "tests.fixture_app:app", *probe, "--heartbeat-interval", "0.05"],
        cwd=REPO_ROOT,
        env=env,
    )
    try:
        readiness = tmp_path / "readiness"
        deadline = time.monotonic() + 10
        while not readiness.exists():
            assert proc.poll() is None, "worker exited early"
            assert time.monotonic() < deadline, "readiness file never appeared"
            time.sleep(0.05)  # the probe file is written by another process: poll
        health = subprocess.run(
            [*base, "health", "readiness", "--max-age", "5", *probe],
            cwd=REPO_ROOT,
            env=env,
            timeout=10,
        )
        assert health.returncode == 0
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=10) == 0
        assert not (tmp_path / "readiness").exists()
        assert not (tmp_path / "liveness").exists()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def test_worker_configures_logging_and_logs_startup(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import logging

    import asynk.cli
    from asynk.worker import Worker

    async def fake_run(self: Worker) -> None:
        return None

    monkeypatch.setattr(Worker, "run_with_signals", fake_run)
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(logging, "basicConfig", lambda **kw: calls.append(kw))
    records: list[logging.LogRecord] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = Capture()
    logging.getLogger("asynk").addHandler(handler)
    logging.getLogger("asynk").setLevel(logging.INFO)
    try:
        assert asynk.cli.main(["worker", "tests.fixture_app:app", "--concurrency", "3"]) == 0
    finally:
        logging.getLogger("asynk").removeHandler(handler)
        logging.getLogger("asynk").setLevel(logging.NOTSET)
    assert len(calls) == 1
    assert calls[0]["level"] == logging.INFO
    assert any("concurrency=3" in r.getMessage() for r in records)


def test_worker_invalid_argument_returns_2_without_traceback(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["worker", "tests.fixture_app:app", "--concurrency", "0"]) == 2
    err = capsys.readouterr().err
    assert "asynk:" in err
    assert "Traceback" not in err


def test_scheduler_requires_scheduled_tasks(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["scheduler", "tests.fixture_app:app"]) == 2
    err = capsys.readouterr().err
    assert err.startswith("asynk:")
    assert "no scheduled tasks" in err
    assert "Traceback" not in err


def test_scheduler_unencodable_args_returns_2(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["scheduler", "tests.fixture_scheduler_app:unencodable"]) == 2
    err = capsys.readouterr().err
    assert err.startswith("asynk:")
    assert "Traceback" not in err


def test_scheduler_requires_producer(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["scheduler", "tests.fixture_scheduler_app:no_producer"]) == 2
    err = capsys.readouterr().err
    assert "no producer" in err
    assert "Traceback" not in err


def test_scheduler_parses_options(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from asynk.scheduler import Scheduler

    built: list[Scheduler] = []

    async def fake_run(self: Scheduler) -> None:
        built.append(self)

    monkeypatch.setattr(Scheduler, "run_with_signals", fake_run)
    code = main(
        [
            "scheduler",
            "tests.fixture_scheduler_app:app",
            "--heartbeat-interval",
            "0.5",
            "--probe-dir",
            str(tmp_path),
            "--log-level",
            "warning",
        ]
    )
    assert code == 0
    [scheduler] = built
    assert scheduler.heartbeat_interval == 0.5
    assert scheduler.probe_dir == tmp_path


def test_scheduler_logs_default_tz_scheduled_task(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from asynk.scheduler import Scheduler

    async def fake_run(self: Scheduler) -> None:
        return None

    monkeypatch.setattr(Scheduler, "run_with_signals", fake_run)
    with caplog.at_level(logging.INFO, logger="asynk"):
        assert main(["scheduler", "tests.fixture_scheduler_app:app"]) == 0
    assert "(0 0 1 1 *, UTC)" in caplog.text


def test_scheduler_bad_heartbeat_returns_2(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["scheduler", "tests.fixture_scheduler_app:app", "--heartbeat-interval", "0"]) == 2
    assert "heartbeat_interval" in capsys.readouterr().err


def test_scheduler_subprocess_lifecycle(tmp_path: Path) -> None:
    env = {**os.environ, "PYTHONPATH": str(REPO_ROOT)}
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "asynk.cli",
            "scheduler",
            "tests.fixture_scheduler_app:app",
            "--probe-dir",
            str(tmp_path),
            "--heartbeat-interval",
            "0.05",
        ],
        cwd=REPO_ROOT,
        env=env,
    )
    try:
        liveness = tmp_path / "liveness"
        deadline = time.monotonic() + 10
        while not liveness.exists():
            assert proc.poll() is None, "scheduler exited early"
            assert time.monotonic() < deadline, "liveness file never appeared"
            time.sleep(0.05)  # the probe file is written by another process: poll
        assert not (tmp_path / "readiness").exists()
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=10) == 0
        assert not liveness.exists()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
