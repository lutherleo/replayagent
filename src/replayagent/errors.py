"""Exception hierarchy for replayagent.

Two families matter for correctness:

* Errors that describe a *workflow outcome* (``ActivityError``, ``SignalTimeout``) are
  raised inside workflow code from recorded history, so they are deterministic and the
  workflow may catch them.
* Errors that describe a *broken replay* (``NonDeterminismError``, ``VersionError``,
  ``WorkflowTaskError``) mean the workflow code no longer matches its history. The worker
  never records them as workflow failures; it parks the workflow as ``blocked`` so a code
  fix can be deployed and the workflow resumed without losing progress.
"""

from __future__ import annotations

from typing import Any


class ReplayAgentError(Exception):
    """Base class for every error raised by replayagent."""


class SerializationError(ReplayAgentError):
    """A value that must be stored in history is not JSON-serialisable."""


class HistoryCorruptError(ReplayAgentError):
    """A history violates a structural invariant (sequence gap, event after terminal...)."""


class WorkflowNotFound(ReplayAgentError):
    """No workflow with the given id exists in the store."""


class WorkflowAlreadyExists(ReplayAgentError):
    """A workflow with the given id was already started."""


class WorkflowNotRunning(ReplayAgentError):
    """The operation needs a running workflow (for example sending a signal)."""


class UnknownWorkflowType(ReplayAgentError):
    """The workflow type is not registered with the application."""


class UnknownActivity(ReplayAgentError):
    """The activity name is not registered with the application."""


class LeaseLost(ReplayAgentError):
    """Another worker owns the workflow now; this worker must stop touching it."""


class WorkflowTaskError(ReplayAgentError):
    """Workflow code misbehaved in a way that is not a normal failure.

    Examples: swallowing the internal suspension signal with ``except BaseException``, or
    scheduling an activity that is not registered.
    """


class NonDeterminismError(WorkflowTaskError):
    """Replaying the workflow code produced a different command than the one recorded."""

    def __init__(self, message: str, *, cmd: int | None = None) -> None:
        super().__init__(message)
        self.cmd = cmd


class VersionError(WorkflowTaskError):
    """A recorded ``get_version`` marker is outside the range the current code supports."""


class ActivityError(ReplayAgentError):
    """Raised inside workflow code when an activity failed after exhausting its retries."""

    def __init__(self, name: str, cmd: int, error: dict[str, Any]) -> None:
        self.name = name
        self.cmd = cmd
        self.error = error
        self.error_type: str = str(error.get("type", "Error"))
        self.error_message: str = str(error.get("message", ""))
        self.attempts: int = int(error.get("attempts", 0))
        super().__init__(
            f"activity {name!r} (command #{cmd}) failed after {self.attempts} attempt(s): "
            f"{self.error_type}: {self.error_message}"
        )


class SignalTimeout(ReplayAgentError):
    """Raised inside workflow code when ``wait_signal`` reached its timeout."""

    def __init__(self, name: str, timeout: float | None) -> None:
        self.name = name
        self.timeout = timeout
        super().__init__(f"no {name!r} signal within {timeout}s")


class ApplicationError(Exception):
    """Raise from an activity to control retry behaviour.

    ``non_retryable=True`` fails the activity immediately regardless of its retry policy.
    ``type`` overrides the error type recorded in history (defaults to the class name).
    """

    def __init__(self, message: str, *, non_retryable: bool = False, type: str | None = None):
        super().__init__(message)
        self.non_retryable = non_retryable
        self.type = type


class WorkflowFailedError(ReplayAgentError):
    """Returned to clients waiting on a workflow that failed."""

    def __init__(self, workflow_id: str, error: dict[str, Any]) -> None:
        self.workflow_id = workflow_id
        self.error = error
        kind, message = error.get("type", "Error"), error.get("message", "")
        super().__init__(f"workflow {workflow_id} failed: {kind}: {message}")


class WorkflowCancelledError(ReplayAgentError):
    """Returned to clients waiting on a workflow that was cancelled."""


class WorkflowBlockedError(ReplayAgentError):
    """Returned to clients waiting on a workflow parked for a code fix."""
