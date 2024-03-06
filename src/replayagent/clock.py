"""Clocks. The worker never calls :func:`time.time` directly so tests can run timers,
backoff and lease expiry on virtual time."""

from __future__ import annotations

import threading
import time
from typing import Protocol


class Clock(Protocol):
    #: ``True`` when sleeping advances time instantly (tests); the worker then jumps to
    #: the next timer instead of waiting for it.
    virtual: bool

    def now(self) -> float:
        """Seconds since the epoch."""
        ...

    def sleep(self, seconds: float) -> None:
        """Block (or, for virtual clocks, advance) for ``seconds``."""
        ...


class SystemClock:
    """Wall-clock time."""

    virtual = False

    def now(self) -> float:
        return time.time()

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)


class FakeClock:
    """Manually advanced clock for deterministic tests."""

    virtual = True

    def __init__(self, start: float = 1_767_225_600.0) -> None:
        self._now = start
        self._lock = threading.Lock()

    def now(self) -> float:
        with self._lock:
            return self._now

    def sleep(self, seconds: float) -> None:
        self.advance(seconds)

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("cannot move a clock backwards")
        with self._lock:
            self._now += seconds
