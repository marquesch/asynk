from __future__ import annotations

import asyncio
from fractions import Fraction
from typing import Any

import pytest

import asynk.retry.backoff
from asynk.middleware import TaskStatus
from asynk.retry import Exponential, RetryPolicy, retry_delay, retry_status


def test_exponential_doubles_from_base_and_caps() -> None:
    e = Exponential(base=1, factor=2, max=60)
    assert [e.delay(n) for n in (1, 2, 3, 7)] == [1, 2, 4, 60]


def test_exponential_caps_huge_retry_numbers() -> None:
    assert Exponential(base=1, factor=2, max=60).delay(5000) == 60


def test_exponential_jitter_draws_up_to_the_capped_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    class Half:
        def uniform(self, a: float, b: float) -> float:
            return (a + b) / 2

    monkeypatch.setattr(asynk.retry.backoff, "random", Half())
    assert Exponential(base=1, factor=2, max=60, jitter=True).delay(3) == 2.0


def test_exponential_jitter_stays_in_bounds() -> None:
    e = Exponential(base=1, factor=2, max=8, jitter=True)
    assert all(0 <= e.delay(n) <= min(8, 2 ** (n - 1)) for n in range(1, 50))


@pytest.mark.parametrize(
    "kwargs",
    [
        {"base": 0, "factor": 2, "max": 1},
        {"base": 1, "factor": 0.5, "max": 1},
        {"base": 2, "factor": 2, "max": 1},
    ],
)
def test_exponential_rejects_bad_values(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        Exponential(**kwargs)


def test_retry_policy_defaults() -> None:
    p = RetryPolicy()
    assert (p.max_retries, p.backoff, p.retry_on) == (1, None, (Exception,))


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"max_retries": -1}, ValueError),
        ({"max_retries": True}, TypeError),
        ({"max_retries": 1.5}, TypeError),
        ({"backoff": -1}, ValueError),
        ({"backoff": float("nan")}, ValueError),
        ({"backoff": True}, TypeError),
        ({"backoff": "1"}, TypeError),
        ({"retry_on": ()}, ValueError),
        ({"retry_on": (KeyboardInterrupt,)}, TypeError),
        ({"retry_on": (ValueError(),)}, TypeError),
    ],
)
def test_retry_policy_validates(kwargs: dict[str, Any], error: type[Exception]) -> None:
    with pytest.raises(error):
        RetryPolicy(**kwargs)


def test_empty_retry_on_is_rejected() -> None:
    with pytest.raises(ValueError, match="empty"):
        RetryPolicy(retry_on=())


class Sub(ValueError): ...


@pytest.mark.parametrize(
    ("policy", "attempt", "error", "status"),
    [
        (None, 0, ValueError(), TaskStatus.FAILED),
        (RetryPolicy(), 0, ValueError(), TaskStatus.RETRYING),
        (RetryPolicy(), 1, ValueError(), TaskStatus.RETRIES_EXCEEDED),
        (RetryPolicy(max_retries=0), 0, ValueError(), TaskStatus.RETRIES_EXCEEDED),
        (RetryPolicy(max_retries=None), 10**6, ValueError(), TaskStatus.RETRYING),
        (RetryPolicy(retry_on=(ValueError,)), 0, Sub(), TaskStatus.RETRYING),
        (RetryPolicy(retry_on=(ValueError,)), 0, KeyError(), TaskStatus.FAILED),
        (RetryPolicy(retry_on=(ValueError,)), 5, KeyError(), TaskStatus.FAILED),
    ],
)
def test_retry_status(
    policy: RetryPolicy | None, attempt: int, error: BaseException, status: TaskStatus
) -> None:
    assert retry_status(policy, attempt, error) is status


def test_retry_delay() -> None:
    assert retry_delay(None, 0) is None
    assert retry_delay(RetryPolicy(), 0) is None
    assert retry_delay(RetryPolicy(backoff=0), 3) is None
    assert retry_delay(RetryPolicy(backoff=1.5), 7) == 1.5
    e = RetryPolicy(backoff=Exponential(base=1, factor=2, max=60))
    assert (retry_delay(e, 0), retry_delay(e, 2)) == (1, 4)


def test_retry_delay_passes_the_retry_number_and_drops_non_positive() -> None:
    seen: list[int] = []

    class Rec:
        def delay(self, retry: int) -> float:
            seen.append(retry)
            return -1.0

    assert retry_delay(RetryPolicy(backoff=Rec()), 4) is None
    assert seen == [5]


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (Fraction(1, 2), TypeError),
        (True, TypeError),
        (-1, ValueError),
        (float("nan"), ValueError),
        (float("inf"), ValueError),
    ],
)
def test_constant_backoff_validation(value: Any, error: type[Exception]) -> None:
    with pytest.raises(error):
        RetryPolicy(backoff=value)


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"base": True, "factor": 2, "max": 4}, TypeError),
        ({"base": "1", "factor": 2, "max": 4}, TypeError),
        ({"base": 1, "factor": Fraction(2), "max": 4}, TypeError),
        ({"base": 1, "factor": 2, "max": float("inf")}, ValueError),
        ({"base": float("nan"), "factor": 2, "max": 4}, ValueError),
        ({"base": 1, "factor": float("inf"), "max": 4}, ValueError),
        ({"base": 1, "factor": 2, "max": 4, "jitter": 1}, TypeError),
    ],
)
def test_exponential_validates_types_and_finiteness(
    kwargs: dict[str, Any], error: type[Exception]
) -> None:
    with pytest.raises(error):
        Exponential(**kwargs)


def test_delays_are_floats() -> None:
    assert type(Exponential(base=1, factor=2, max=60).delay(2)) is float
    assert type(Exponential(base=1, factor=2, max=60).delay(9)) is float
    assert type(retry_delay(RetryPolicy(backoff=2), 0)) is float
    assert type(retry_delay(RetryPolicy(backoff=Exponential(1, 2, 8)), 0)) is float


def test_max_retries_boundary() -> None:
    policy = RetryPolicy(max_retries=3)
    assert retry_status(policy, 2, ValueError()) is TaskStatus.RETRYING
    assert retry_status(policy, 3, ValueError()) is TaskStatus.RETRIES_EXCEEDED


def test_exponential_factor_one_is_constant() -> None:
    e = Exponential(base=2, factor=1, max=10)
    assert [e.delay(n) for n in (1, 5, 5000)] == [2, 2, 2]


def test_exponential_float_overflow_returns_max() -> None:
    assert Exponential(base=1, factor=1e308, max=60).delay(3) == 60


def test_single_exception_class_as_retry_on_is_rejected() -> None:
    with pytest.raises(TypeError):
        RetryPolicy(retry_on=ValueError)  # type: ignore[arg-type]


def test_base_exception_only_errors_fail() -> None:
    assert retry_status(RetryPolicy(), 0, asyncio.CancelledError()) is TaskStatus.FAILED
