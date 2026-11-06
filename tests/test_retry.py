from __future__ import annotations

from typing import Any

import pytest

import asynk.retry
from asynk.hooks import TaskStatus
from asynk.retry import Exponential, RetryPolicy, as_retry_policy, retry_delay, retry_status


def test_exponential_doubles_from_base_and_caps() -> None:
    e = Exponential(base=1, factor=2, max=60)
    assert [e.delay(n) for n in (1, 2, 3, 7)] == [1, 2, 4, 60]


def test_exponential_caps_huge_retry_numbers() -> None:
    assert Exponential(base=1, factor=2, max=60).delay(5000) == 60


def test_exponential_jitter_draws_up_to_the_capped_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    class Half:
        def uniform(self, a: float, b: float) -> float:
            return (a + b) / 2

    monkeypatch.setattr(asynk.retry, "random", Half())
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


def test_empty_retry_on_points_to_retry_false() -> None:
    with pytest.raises(ValueError, match="retry=False"):
        RetryPolicy(retry_on=())


def test_as_retry_policy_converts_a_mapping() -> None:
    assert as_retry_policy({"max_retries": 3}) == RetryPolicy(max_retries=3)
    assert as_retry_policy({"retry_on": [ValueError]}).retry_on == (ValueError,)
    policy = RetryPolicy()
    assert as_retry_policy(policy) is policy


def test_as_retry_policy_names_unknown_keys() -> None:
    with pytest.raises(TypeError, match="max_retires"):
        as_retry_policy({"max_retires": 3})


@pytest.mark.parametrize("value", [True, False, 3, "x", None])
def test_as_retry_policy_rejects_other_values(value: Any) -> None:
    with pytest.raises(TypeError):
        as_retry_policy(value)


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
