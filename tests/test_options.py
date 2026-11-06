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


def test_timeout_defaults_to_unset() -> None:
    assert TaskOptions().timeout is None


@pytest.mark.parametrize("timeout", [None, False, 0.5, 3])
def test_accepts_a_positive_number_false_or_none(timeout: float | bool | None) -> None:
    assert TaskOptions(timeout=timeout).timeout is timeout


@pytest.mark.parametrize("timeout", [0, -1, 0.0, float("nan"), float("inf"), float("-inf")])
def test_rejects_a_timeout_that_is_not_finite_and_positive(timeout: float) -> None:
    with pytest.raises(ValueError, match="timeout"):
        TaskOptions(timeout=timeout)


@pytest.mark.parametrize("timeout", [True, "5", b"5", [1]])
def test_rejects_a_timeout_that_is_not_a_number(timeout: Any) -> None:
    with pytest.raises(TypeError, match="timeout"):
        TaskOptions(timeout=timeout)


def test_with_defaults_fills_an_unset_timeout_and_keeps_a_set_one() -> None:
    app = TaskOptions(timeout=30)
    assert TaskOptions().with_defaults(app).timeout == 30
    assert TaskOptions(timeout=5).with_defaults(app).timeout == 5
    assert TaskOptions(timeout=False).with_defaults(app).timeout is False
    assert TaskOptions(timeout=5).with_defaults(TaskOptions()).timeout == 5
