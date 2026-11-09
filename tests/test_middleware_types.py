from __future__ import annotations

import ast
from pathlib import Path

import pytest

import asynk
import asynk.middleware
from asynk.middleware import TaskStatus
from asynk.results import TaskState

SRC = Path(asynk.__file__).parent


@pytest.mark.parametrize(
    ("status", "state"),
    [
        (TaskStatus.SUCCESS, TaskState.SUCCESS),
        (TaskStatus.SKIPPED, TaskState.SKIPPED),
        (TaskStatus.FAILED, TaskState.FAILURE),
        (TaskStatus.RETRIES_EXCEEDED, TaskState.FAILURE),
        (TaskStatus.UNKNOWN_TASK, TaskState.FAILURE),
        (TaskStatus.RETRYING, TaskState.RETRYING),
    ],
)
def test_every_status_maps_to_the_state_it_is_recorded_as(
    status: TaskStatus, state: TaskState
) -> None:
    assert status.state is state


def test_every_status_has_a_state() -> None:
    assert all(isinstance(status.state, TaskState) for status in TaskStatus)


def test_the_top_level_names_are_the_middleware_module_ones() -> None:
    for name in ("TaskStatus", "TaskResult", "TaskCall", "Next"):
        assert getattr(asynk, name) is getattr(asynk.middleware, name)


@pytest.mark.parametrize(
    "module", ["retry/__init__.py", "recorder.py", "telemetry.py", "otel.py", "errors.py"]
)
def test_outcome_consumers_do_not_import_the_lifecycle_hooks_module(module: str) -> None:
    tree = ast.parse((SRC / module).read_text())
    imported = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    assert "asynk.hooks" not in imported
