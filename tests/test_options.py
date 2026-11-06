from typing import Any

import pytest

from asynk import RetryPolicy, TaskOptions
from asynk.options import resolve_retry


def test_defaults_to_no_retry_setting() -> None:
    assert TaskOptions().retry is None


@pytest.mark.parametrize("retry", [None, False, RetryPolicy(max_retries=2)])
def test_accepts_a_policy_false_or_none(retry: RetryPolicy | bool | None) -> None:
    assert TaskOptions(retry=retry).retry is retry  # type: ignore[arg-type]


@pytest.mark.parametrize("retry", [True, 3, {"max_retries": 3}, "x"])
def test_rejects_other_values(retry: Any) -> None:
    with pytest.raises(TypeError, match="retry"):
        TaskOptions(retry=retry)


def test_is_frozen() -> None:
    with pytest.raises(AttributeError):
        TaskOptions().retry = False  # type: ignore[misc]


def test_resolve_retry_takes_the_most_specific_layer_that_sets_it() -> None:
    task, app = RetryPolicy(max_retries=5), RetryPolicy(max_retries=2)
    assert resolve_retry(TaskOptions(retry=task), TaskOptions(retry=app)) is task
    assert resolve_retry(TaskOptions(), TaskOptions(retry=app)) is app
    assert resolve_retry(None, TaskOptions(retry=app)) is app


def test_resolve_retry_false_wins_and_means_none() -> None:
    assert resolve_retry(TaskOptions(retry=False), TaskOptions(retry=RetryPolicy())) is None
    assert resolve_retry(TaskOptions(), TaskOptions()) is None
