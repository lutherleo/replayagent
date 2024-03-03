"""What an activity can know about itself while it runs."""

from __future__ import annotations

import contextvars
from dataclasses import dataclass


@dataclass(frozen=True)
class ActivityInfo:
    """Context for the running activity attempt.

    ``idempotency_key`` is stable across retries *and* crash restarts of the same
    activity (it is ``<workflow_id>/<cmd>``). Pass it to downstream systems (payment
    APIs, ``INSERT ... ON CONFLICT DO NOTHING``) to turn at-least-once execution into an
    effectively-once side effect.
    """

    workflow_id: str
    workflow_type: str
    activity_name: str
    cmd: int
    attempt: int
    idempotency_key: str
    scheduled_at: float
    deadline: float | None


_current: contextvars.ContextVar[ActivityInfo | None] = contextvars.ContextVar(
    "replayagent_activity", default=None
)


def info() -> ActivityInfo:
    """Return the :class:`ActivityInfo` of the activity running in this thread."""
    current = _current.get()
    if current is None:
        raise RuntimeError("activity.info() called outside of an activity")
    return current


def in_activity() -> bool:
    return _current.get() is not None


def _set(value: ActivityInfo | None) -> contextvars.Token[ActivityInfo | None]:
    return _current.set(value)


def _reset(token: contextvars.Token[ActivityInfo | None]) -> None:
    _current.reset(token)
