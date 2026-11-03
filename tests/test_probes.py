from __future__ import annotations

import os
import time
from pathlib import Path

from asynk.probes import ProbeFiles, check_probe, default_probe_dir


def test_beat_creates_dir_and_files(tmp_path: Path) -> None:
    probes = ProbeFiles(tmp_path / "nested" / "probes")
    probes.beat(ready=True)
    assert probes.liveness == tmp_path / "nested" / "probes" / "liveness"
    assert probes.readiness == tmp_path / "nested" / "probes" / "readiness"
    assert probes.liveness.exists()
    assert probes.readiness.exists()
    assert float(probes.liveness.read_text()) > 0
    assert sorted(p.name for p in probes.liveness.parent.iterdir()) == ["liveness", "readiness"]


def test_beat_not_ready_removes_readiness(tmp_path: Path) -> None:
    probes = ProbeFiles(tmp_path)
    probes.beat(ready=True)
    probes.beat(ready=False)
    assert probes.liveness.exists()
    assert not probes.readiness.exists()
    probes.beat(ready=False)  # absent readiness is fine


def test_clear_is_idempotent(tmp_path: Path) -> None:
    probes = ProbeFiles(tmp_path)
    probes.clear()  # nothing there yet
    probes.beat(ready=True)
    probes.clear()
    probes.clear()
    assert not probes.liveness.exists()
    assert not probes.readiness.exists()


def test_clear_tolerates_missing_directory(tmp_path: Path) -> None:
    ProbeFiles(tmp_path / "absent").clear()


def test_check_probe_healthy_fresh_file(tmp_path: Path) -> None:
    probes = ProbeFiles(tmp_path)
    probes.beat(ready=True)
    healthy, message = check_probe(probes.liveness, max_age=30)
    assert healthy
    assert message


def test_check_probe_stale_file(tmp_path: Path) -> None:
    probes = ProbeFiles(tmp_path)
    probes.beat(ready=True)
    old = time.time() - 100
    os.utime(probes.liveness, (old, old))
    healthy, message = check_probe(probes.liveness, max_age=30)
    assert not healthy
    assert "stale" in message


def test_check_probe_uses_given_now(tmp_path: Path) -> None:
    probes = ProbeFiles(tmp_path)
    probes.beat(ready=True)
    mtime = probes.liveness.stat().st_mtime
    assert check_probe(probes.liveness, max_age=10, now=mtime + 10)[0]
    assert not check_probe(probes.liveness, max_age=10, now=mtime + 11)[0]


def test_check_probe_missing_file_and_missing_directory_are_unhealthy_with_message(
    tmp_path: Path,
) -> None:
    missing_file = tmp_path / "liveness"
    healthy, message = check_probe(missing_file, max_age=30)
    assert not healthy
    assert "missing" in message
    assert len(message) < 200

    healthy, message = check_probe(tmp_path / "no-such-dir" / "liveness", max_age=30)
    assert not healthy
    assert "missing" in message
    assert len(message) < 200


def test_default_probe_dir() -> None:
    assert default_probe_dir().name == "asynk"
