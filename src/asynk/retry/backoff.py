"""Backoff strategies for retries."""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Protocol

from asynk._validate import check_number


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
        check_number("base", self.base, exclusive=True)
        check_number("factor", self.factor, minimum=1)
        check_number("max", self.max, minimum=self.base)
        if not isinstance(self.jitter, bool):
            raise TypeError(f"jitter must be a bool, got {self.jitter!r}")

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
