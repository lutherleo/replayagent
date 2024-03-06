"""Crash injection.

The engine calls :func:`crashpoint` at every place where a real crash would leave
durable state half-way through a step. A crash plan (from the ``REPLAYAGENT_CRASH``
environment variable or :func:`install`) picks a point and the hit number on which to
die. In ``exit`` mode the process ends with :func:`os._exit` - no ``finally`` blocks, no
flushing, no SQLite commit - which is what ``kill -9`` looks like to user space. In
``raise`` mode a :class:`SimulatedCrash` (a ``BaseException``) unwinds the worker so
property tests can simulate many crashes in one process.

Format: ``point[@n][,point[@n]...]`` where ``n`` is the 1-based hit that crashes.
"""

from __future__ import annotations

import os
import sys
import threading
from dataclasses import dataclass, field
from typing import Literal

CRASH_ENV = "REPLAYAGENT_CRASH"
CRASH_MODE_ENV = "REPLAYAGENT_CRASH_MODE"
CRASH_EXIT_CODE = 77

#: Every crash point the engine exposes, in rough workflow order.
CRASH_POINTS: tuple[str, ...] = (
    "after_workflow_claimed",
    "after_commands_appended",
    "after_activity_started",
    "activity_after_execute",
    "after_activity_completed",
    "after_timer_fired",
    "after_signal_received",
    "after_signal_consumed",
)

Mode = Literal["exit", "raise"]


class SimulatedCrash(BaseException):
    """Raised by a crash point in ``raise`` mode."""

    def __init__(self, point: str) -> None:
        super().__init__(f"simulated crash at {point}")
        self.point = point


@dataclass
class CrashPlan:
    targets: dict[str, int]
    mode: Mode = "exit"
    hits: dict[str, int] = field(default_factory=dict)
    fired: bool = False

    @classmethod
    def parse(cls, spec: str, mode: Mode = "exit") -> CrashPlan:
        targets: dict[str, int] = {}
        for part in filter(None, (p.strip() for p in spec.split(","))):
            point, _, nth = part.partition("@")
            if point not in CRASH_POINTS:
                raise ValueError(f"unknown crash point {point!r}; known: {', '.join(CRASH_POINTS)}")
            count = int(nth) if nth else 1
            if count < 1:
                raise ValueError("crash hit numbers start at 1")
            targets[point] = count
        return cls(targets=targets, mode=mode)

    def hit(self, point: str) -> bool:
        """Record a pass through ``point``; return True if this pass must crash."""
        if self.fired or point not in self.targets:
            return False
        self.hits[point] = self.hits.get(point, 0) + 1
        if self.hits[point] == self.targets[point]:
            self.fired = True
            return True
        return False


_lock = threading.Lock()
_plan: CrashPlan | None = None


def install(plan: CrashPlan | None) -> None:
    """Activate ``plan`` (or clear crash injection with ``None``)."""
    global _plan
    with _lock:
        _plan = plan


def install_from_env() -> None:
    """Load a crash plan from ``REPLAYAGENT_CRASH`` if it is set."""
    spec = os.environ.get(CRASH_ENV, "")
    if not spec:
        return
    mode_raw = os.environ.get(CRASH_MODE_ENV, "exit")
    if mode_raw not in ("exit", "raise"):
        raise ValueError(f"{CRASH_MODE_ENV} must be 'exit' or 'raise'")
    mode: Mode = "exit" if mode_raw == "exit" else "raise"
    install(CrashPlan.parse(spec, mode))


def crashpoint(point: str) -> None:
    """Crash here if the active plan says so."""
    plan = _plan
    if plan is None:
        return
    with _lock:
        crash = plan.hit(point)
    if not crash:
        return
    if plan.mode == "raise":
        raise SimulatedCrash(point)
    sys.stderr.write(f"replayagent: injected crash at {point}\n")
    sys.stderr.flush()
    os._exit(CRASH_EXIT_CODE)
