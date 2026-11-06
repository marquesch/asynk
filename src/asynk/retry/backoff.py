"""Backoff strategies for retries."""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Protocol


class Backoff(Protocol):
    """A strategy that decides how long to wait before a retry."""

    def delay(self, retry: int) -> float:
        """Returns the seconds to wait before a retry.

        Args:
            retry: The retry number, counted from 1.

        Returns:
            The delay in seconds; a value of zero or less means no delay.
        """
        ...


@dataclass(frozen=True)
class Exponential:
    """Exponential backoff: the delay grows by `factor` on each retry.

    The first retry waits `base`, the next `base * factor`, and so on, never
    more than `max`.

    Attributes:
        base: Seconds to wait before the first retry; must be positive.
        factor: Multiplier applied for each further retry; at least 1.
        max: Upper bound of the delay in seconds; at least `base`.
        jitter: When true, the delay is drawn uniformly between zero and the
            capped delay.

    Raises:
        ValueError: If `base`, `factor` or `max` is out of range.
    """

    base: float
    factor: float
    max: float
    jitter: bool = False

    def __post_init__(self) -> None:
        for name in ("base", "factor", "max"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int | float):
                raise TypeError(f"{name} must be an int or float, got {value!r}")
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite, got {value!r}")
        if not isinstance(self.jitter, bool):
            raise TypeError(f"jitter must be a bool, got {self.jitter!r}")
        if not self.base > 0:
            raise ValueError(f"base must be positive, got {self.base!r}")
        if not self.factor >= 1:
            raise ValueError(f"factor must be at least 1, got {self.factor!r}")
        if not self.max >= self.base:
            raise ValueError(f"max must be at least base ({self.base!r}), got {self.max!r}")

    def delay(self, retry: int) -> float:
        """Returns the seconds to wait before a retry.

        Args:
            retry: The retry number, counted from 1.

        Returns:
            The delay in seconds, at most `max`.
        """
        exponent = retry - 1
        if exponent > 0 and exponent * math.log(self.factor) >= math.log(self.max / self.base):
            capped = self.max
        else:
            try:
                capped = min(self.max, self.base * self.factor**exponent)
            except OverflowError:
                capped = self.max
        return random.uniform(0, capped) if self.jitter else float(capped)
