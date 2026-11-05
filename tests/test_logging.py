from __future__ import annotations

import logging

import pytest

from asynk.logging import LOG_FORMAT, configure_logging


def test_configure_logging_accepts_level_name(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(logging, "basicConfig", lambda **kw: calls.append(kw))
    configure_logging("DEBUG")
    assert calls[0]["level"] == logging.DEBUG
    assert calls[0]["format"] == LOG_FORMAT


def test_configure_logging_accepts_numeric_level(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(logging, "basicConfig", lambda **kw: calls.append(kw))
    configure_logging(logging.WARNING)
    assert calls[0]["level"] == logging.WARNING


def test_configure_logging_rejects_unknown_level() -> None:
    with pytest.raises(ValueError):
        configure_logging("LOUD")
