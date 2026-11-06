from __future__ import annotations

import dataclasses

import pytest

from asynk.transport import PublishOptions


def test_publish_options_default_has_no_delay() -> None:
    assert PublishOptions().delay is None


@pytest.mark.parametrize("bad", [-1.0, float("nan"), float("inf")])
def test_publish_options_rejects_bad_delays(bad: float) -> None:
    with pytest.raises(ValueError):
        PublishOptions(delay=bad)


def test_publish_options_rejects_bool() -> None:
    with pytest.raises(TypeError):
        PublishOptions(delay=True)


def test_publish_options_rejects_non_numbers() -> None:
    with pytest.raises(TypeError):
        PublishOptions(delay="1")  # type: ignore[arg-type]


def test_publish_options_is_frozen() -> None:
    with pytest.raises(dataclasses.FrozenInstanceError):
        PublishOptions(delay=1.0).delay = 2.0  # type: ignore[misc]
