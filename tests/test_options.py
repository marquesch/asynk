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


def test_timeout_is_unset_by_default() -> None:
    assert TaskOptions().timeout is None
    assert TaskOptions().time_limit is None


@pytest.mark.parametrize("timeout", [0.5, 3])
def test_accepts_a_positive_number(timeout: float) -> None:
    assert TaskOptions(timeout=timeout).timeout == timeout
    assert TaskOptions(timeout=timeout).time_limit == timeout


def test_false_means_no_limit() -> None:
    assert TaskOptions(timeout=False).timeout is False
    assert TaskOptions(timeout=False).time_limit is None


@pytest.mark.parametrize("timeout", [0, -1, -0.5, float("nan"), float("inf"), float("-inf")])
def test_rejects_a_timeout_that_is_not_positive_and_finite(timeout: float) -> None:
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
    assert TaskOptions(timeout=5).with_defaults(TaskOptions()).timeout == 5


def test_with_defaults_keeps_a_false_timeout_over_the_default() -> None:
    assert TaskOptions(timeout=False).with_defaults(TaskOptions(timeout=30)).timeout is False
