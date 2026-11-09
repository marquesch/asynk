"""Argument validation shared by the option classes."""

from __future__ import annotations

import math


def check_number(name: str, value: object, *, minimum: float = 0, exclusive: bool = False) -> None:
    # An int or float (not a bool) that is finite and not below `minimum`.
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"{name} must be a number, got {value!r}")
    if not math.isfinite(value) or value < minimum or (exclusive and value == minimum):
        bound = f"> {minimum!r}" if exclusive else f">= {minimum!r}"
        raise ValueError(f"{name} must be a finite number {bound}, got {value!r}")
