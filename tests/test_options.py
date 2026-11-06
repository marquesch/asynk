from typing import Any

import pytest

from asynk import RetryPolicy, TaskOptions


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


def test_with_defaults_keeps_the_options_that_are_set_and_fills_the_rest() -> None:
    task, app = RetryPolicy(max_retries=5), RetryPolicy(max_retries=2)
    assert TaskOptions(retry=task).with_defaults(TaskOptions(retry=app)).retry is task
    assert TaskOptions().with_defaults(TaskOptions(retry=app)).retry is app
    assert TaskOptions(retry=False).with_defaults(TaskOptions(retry=app)).retry is False


def test_with_defaults_returns_a_new_instance_and_leaves_both_untouched() -> None:
    options, defaults = TaskOptions(), TaskOptions(retry=RetryPolicy())
    merged = options.with_defaults(defaults)
    assert merged is not options
    assert options.retry is None
    assert defaults.retry is not None


def test_policy_is_the_retry_policy_or_none() -> None:
    policy = RetryPolicy()
    assert TaskOptions(retry=policy).policy is policy
    assert TaskOptions(retry=False).policy is None
    assert TaskOptions().policy is None
