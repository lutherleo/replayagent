"""Retry policies with exponential backoff.

The engine, not workflow code, evaluates retry policies, so the computed retry time is
free to use randomness (jitter): it is written into history as ``retry_at`` and never
recomputed on replay.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff: ``initial_interval * backoff_coefficient ** (attempt - 1)``.

    ``maximum_attempts`` counts every attempt including the first; ``0`` means unlimited.
    ``jitter`` is a fraction in ``[0, 1]``: the delay is scaled by a uniform factor in
    ``[1 - jitter, 1 + jitter]``.
    """

    initial_interval: float = 1.0
    backoff_coefficient: float = 2.0
    maximum_interval: float = 60.0
    maximum_attempts: int = 3
    jitter: float = 0.0
    non_retryable_error_types: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.initial_interval < 0 or not math.isfinite(self.initial_interval):
            raise ValueError("initial_interval must be a finite number >= 0")
        if self.backoff_coefficient < 1:
            raise ValueError("backoff_coefficient must be >= 1")
        if self.maximum_interval < self.initial_interval:
            raise ValueError("maximum_interval must be >= initial_interval")
        if self.maximum_attempts < 0:
            raise ValueError("maximum_attempts must be >= 0 (0 = unlimited)")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter must be within [0, 1]")

    def delay(self, attempt: int, rand: float = 0.5) -> float:
        """Delay before attempt ``attempt + 1`` after attempt ``attempt`` failed.

        ``rand`` is a uniform sample in ``[0, 1)``; it only matters when ``jitter > 0``.
        """
        if attempt < 1:
            raise ValueError("attempt numbers start at 1")
        exponent = min(attempt - 1, 1000)
        base = min(
            self.initial_interval * self.backoff_coefficient**exponent, self.maximum_interval
        )
        if self.jitter:
            base *= 1.0 + self.jitter * (2.0 * rand - 1.0)
        return max(0.0, base)

    def allows_retry(self, attempt: int, error: dict[str, Any]) -> bool:
        """Whether a failure of attempt ``attempt`` with ``error`` should be retried."""
        if error.get("non_retryable"):
            return False
        if error.get("type") in self.non_retryable_error_types:
            return False
        return self.maximum_attempts == 0 or attempt < self.maximum_attempts

    def to_dict(self) -> dict[str, Any]:
        return {
            "initial_interval": self.initial_interval,
            "backoff_coefficient": self.backoff_coefficient,
            "maximum_interval": self.maximum_interval,
            "maximum_attempts": self.maximum_attempts,
            "jitter": self.jitter,
            "non_retryable_error_types": list(self.non_retryable_error_types),
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> RetryPolicy:
        return cls(
            initial_interval=float(raw.get("initial_interval", 1.0)),
            backoff_coefficient=float(raw.get("backoff_coefficient", 2.0)),
            maximum_interval=float(raw.get("maximum_interval", 60.0)),
            maximum_attempts=int(raw.get("maximum_attempts", 3)),
            jitter=float(raw.get("jitter", 0.0)),
            non_retryable_error_types=tuple(raw.get("non_retryable_error_types", ())),
        )


NO_RETRY = RetryPolicy(maximum_attempts=1)
