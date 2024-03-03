"""replayagent - a single-node durable execution runtime for LLM agents.

Workflows are plain Python functions ``fn(ctx, input)``. LLM calls, tool calls and any
other side effects run as *activities* whose results are recorded in an event-sourced
SQLite history. After a crash the workflow function is replayed against that history:
recorded steps return their recorded results instantly and only unfinished work runs.
"""

from replayagent.activity_context import ActivityInfo
from replayagent.activity_context import info as activity_info
from replayagent.client import Client
from replayagent.clock import Clock, FakeClock, SystemClock
from replayagent.errors import (
    ActivityError,
    ApplicationError,
    NonDeterminismError,
    ReplayAgentError,
    SignalTimeout,
    VersionError,
    WorkflowBlockedError,
    WorkflowCancelledError,
    WorkflowFailedError,
    WorkflowTaskError,
)
from replayagent.events import Event, EventType, WorkflowStatus
from replayagent.registry import ActivityOptions, App, activity, workflow
from replayagent.replay import DEFAULT_VERSION, ActivityHandle, TimerHandle, WorkflowContext
from replayagent.retry import NO_RETRY, RetryPolicy
from replayagent.store import Store
from replayagent.worker import Worker, WorkerStats

__version__ = "0.1.0"

__all__ = [
    "DEFAULT_VERSION",
    "NO_RETRY",
    "ActivityError",
    "ActivityHandle",
    "ActivityInfo",
    "ActivityOptions",
    "App",
    "ApplicationError",
    "Client",
    "Clock",
    "Event",
    "EventType",
    "FakeClock",
    "NonDeterminismError",
    "ReplayAgentError",
    "RetryPolicy",
    "SignalTimeout",
    "Store",
    "SystemClock",
    "TimerHandle",
    "VersionError",
    "Worker",
    "WorkerStats",
    "WorkflowBlockedError",
    "WorkflowCancelledError",
    "WorkflowContext",
    "WorkflowFailedError",
    "WorkflowStatus",
    "WorkflowTaskError",
    "activity",
    "activity_info",
    "workflow",
]
