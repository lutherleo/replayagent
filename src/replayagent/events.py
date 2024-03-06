"""History event model.

A workflow's history is an append-only list of :class:`Event` objects numbered ``1..N``.
*Command* events are produced by workflow code (schedule an activity, start a timer,
wait for a signal, record a side effect, mark a version); each one carries a ``cmd``
index ``0..M`` in the order the code issued them. *Resolution* events are produced by the
engine and point back at the command they resolve through the same ``cmd`` index.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class EventType(StrEnum):
    WORKFLOW_STARTED = "WorkflowStarted"
    ACTIVITY_SCHEDULED = "ActivityScheduled"
    ACTIVITY_STARTED = "ActivityStarted"
    ACTIVITY_ATTEMPT_FAILED = "ActivityAttemptFailed"
    ACTIVITY_COMPLETED = "ActivityCompleted"
    ACTIVITY_FAILED = "ActivityFailed"
    TIMER_STARTED = "TimerStarted"
    TIMER_FIRED = "TimerFired"
    SIGNAL_RECEIVED = "SignalReceived"
    SIGNAL_WAIT_STARTED = "SignalWaitStarted"
    SIGNAL_CONSUMED = "SignalConsumed"
    SIGNAL_WAIT_TIMED_OUT = "SignalWaitTimedOut"
    SIDE_EFFECT = "SideEffectRecorded"
    VERSION_MARKER = "VersionMarker"
    CANCEL_REQUESTED = "CancelRequested"
    WORKFLOW_COMPLETED = "WorkflowCompleted"
    WORKFLOW_FAILED = "WorkflowFailed"
    WORKFLOW_CANCELLED = "WorkflowCancelled"


COMMAND_TYPES: frozenset[EventType] = frozenset(
    {
        EventType.ACTIVITY_SCHEDULED,
        EventType.TIMER_STARTED,
        EventType.SIGNAL_WAIT_STARTED,
        EventType.SIDE_EFFECT,
        EventType.VERSION_MARKER,
    }
)

TERMINAL_TYPES: frozenset[EventType] = frozenset(
    {EventType.WORKFLOW_COMPLETED, EventType.WORKFLOW_FAILED, EventType.WORKFLOW_CANCELLED}
)


class WorkflowStatus(StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    BLOCKED = "blocked"

    @property
    def is_terminal(self) -> bool:
        return self in (WorkflowStatus.COMPLETED, WorkflowStatus.FAILED, WorkflowStatus.CANCELLED)


@dataclass(frozen=True)
class Event:
    """One immutable history entry."""

    seq: int
    type: EventType
    ts: float
    cmd: int | None = None
    data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "type": self.type.value,
            "ts": self.ts,
            "cmd": self.cmd,
            "data": self.data,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> Event:
        return cls(
            seq=int(raw["seq"]),
            type=EventType(raw["type"]),
            ts=float(raw["ts"]),
            cmd=None if raw.get("cmd") is None else int(raw["cmd"]),
            data=dict(raw.get("data") or {}),
        )


@dataclass(frozen=True)
class PendingEvent:
    """An event about to be appended; the store assigns ``seq``."""

    type: EventType
    cmd: int | None = None
    data: dict[str, Any] = field(default_factory=dict)
