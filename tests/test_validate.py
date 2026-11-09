from __future__ import annotations

from typing import Any

import pytest

from asynk._validate import check_number


@pytest.mark.parametrize("value", [0, 0.0, 1, 2.5, 10**9])
def test_accepts_numbers_at_or_above_the_default_minimum(value: float) -> None:
    check_number("x", value)


@pytest.mark.parametrize("value", [True, False, None, "1", b"1", [1], 1j])
def test_rejects_non_numbers_and_bools(value: Any) -> None:
    with pytest.raises(TypeError, match="x"):
        check_number("x", value)


@pytest.mark.parametrize("value", [-1, -0.5, float("nan"), float("inf"), float("-inf")])
def test_rejects_negative_and_non_finite_numbers(value: float) -> None:
    with pytest.raises(ValueError, match="x"):
        check_number("x", value)


def test_minimum_is_inclusive_by_default() -> None:
    check_number("x", 1, minimum=1)
    with pytest.raises(ValueError, match="x"):
        check_number("x", 0.99, minimum=1)


def test_exclusive_minimum_rejects_the_bound_itself() -> None:
    check_number("x", 0.1, exclusive=True)
    with pytest.raises(ValueError, match="x"):
        check_number("x", 0, exclusive=True)
