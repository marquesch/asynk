"""Test helper to spy on the timers the memory transport schedules for delayed publishes."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest

Timers = list[tuple[float, Callable[..., object], tuple[object, ...]]]


def spy_on_delivery_timers(monkeypatch: pytest.MonkeyPatch, *, run_now: bool = False) -> Timers:
    """Record the memory transport's delayed-publish timers on the running loop.

    Only the timers whose callback is a queue's `put_nowait` are intercepted. Every
    other `call_later` (for example the ones `asyncio.wait_for` makes on Python 3.11)
    goes to the real loop and keeps its real handle.

    Args:
        monkeypatch: Used to patch the running loop's `call_later`.
        run_now: Run the intercepted callback on the next loop iteration instead of
            holding it back for the test to invoke.

    Returns:
        The intercepted `(delay, callback, args)` timers, filled in as they are made.
    """
    loop = asyncio.get_running_loop()
    real_call_later = loop.call_later
    timers: Timers = []

    def call_later(delay: float, callback: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        if getattr(callback, "__name__", "") != "put_nowait":
            return real_call_later(delay, callback, *args, **kwargs)
        timers.append((delay, callback, args))
        if run_now:
            return loop.call_soon(callback, *args)
        return real_call_later(3600, lambda: None)

    monkeypatch.setattr(loop, "call_later", call_later)
    return timers
