"""Client API: start workflows, signal them, cancel them, wait for results.

A client never writes history. Starting a workflow inserts its first event; everything
else (signals, cancellation) goes through the inbox and is moved into history by the
worker that owns the workflow. The client can therefore run in another process - for
example the CLI - while a worker is serving.
"""

from __future__ import annotations

import secrets
import time
import uuid
from collections.abc import Callable
from typing import Any

from replayagent import serde
from replayagent.clock import Clock, SystemClock
from replayagent.errors import (
    WorkflowBlockedError,
    WorkflowCancelledError,
    WorkflowFailedError,
    WorkflowNotRunning,
)
from replayagent.events import Event, EventType, PendingEvent, WorkflowStatus
from replayagent.registry import App
from replayagent.store import Store, WorkflowRecord


class Client:
    def __init__(self, store: Store, app: App | None = None, *, clock: Clock | None = None):
        self.store = store
        self.app = app
        self.clock: Clock = clock or SystemClock()

    def start(
        self,
        workflow: str | Callable[..., Any],
        input: Any = None,
        *,
        workflow_id: str | None = None,
    ) -> str:
        """Start a workflow and return its id. Raises ``WorkflowAlreadyExists``."""
        code_hash: str | None = None
        if self.app is not None:
            defn = self.app.resolve_workflow(workflow)
            wf_type, code_hash = defn.name, defn.code_hash
        elif isinstance(workflow, str):
            wf_type = workflow
        else:
            raise ValueError("pass a workflow name, or give the client an App")
        wid = workflow_id or f"{wf_type}-{uuid.uuid4().hex[:12]}"
        started = PendingEvent(
            EventType.WORKFLOW_STARTED,
            None,
            {
                "type": wf_type,
                "input": serde.roundtrip(input),
                "seed": secrets.randbits(63),
                "code_hash": code_hash,
            },
        )
        self.store.create_workflow(wid, wf_type, started, self.clock.now(), code_hash)
        return wid

    def signal(self, workflow_id: str, name: str, payload: Any = None) -> None:
        """Queue a signal for a running workflow (buffered until it waits for it)."""
        self._require_open(workflow_id)
        self.store.enqueue(
            workflow_id, "signal", now=self.clock.now(), name=name, payload=serde.roundtrip(payload)
        )

    def cancel(self, workflow_id: str, reason: str | None = None) -> None:
        """Request a hard stop; in-flight activities are abandoned."""
        self._require_open(workflow_id)
        self.store.enqueue(workflow_id, "cancel", now=self.clock.now(), payload=reason)

    def resume(self, workflow_id: str) -> bool:
        """Unblock a workflow parked by a determinism/version error, after a code fix."""
        return self.store.resume(workflow_id, now=self.clock.now())

    def get(self, workflow_id: str) -> WorkflowRecord:
        return self.store.get_workflow(workflow_id)

    def history(self, workflow_id: str) -> list[Event]:
        self.store.get_workflow(workflow_id)
        return self.store.load_events(workflow_id)

    def result(self, workflow_id: str, *, timeout: float | None = None, poll: float = 0.05) -> Any:
        """Wait for a workflow to finish and return its result.

        Raises :class:`WorkflowFailedError`, :class:`WorkflowCancelledError`,
        :class:`WorkflowBlockedError` or :class:`TimeoutError`.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            rec = self.store.get_workflow(workflow_id)
            if rec.status is WorkflowStatus.COMPLETED:
                return rec.result
            if rec.status is WorkflowStatus.FAILED:
                raise WorkflowFailedError(workflow_id, rec.error or {})
            if rec.status is WorkflowStatus.CANCELLED:
                raise WorkflowCancelledError(f"workflow {workflow_id} was cancelled")
            if rec.status is WorkflowStatus.BLOCKED:
                msg = (rec.error or {}).get("message", "")
                raise WorkflowBlockedError(f"workflow {workflow_id} is blocked: {msg}")
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"workflow {workflow_id} is still {rec.status.value}")
            time.sleep(poll)

    def _require_open(self, workflow_id: str) -> None:
        rec = self.store.get_workflow(workflow_id)
        if rec.status.is_terminal:
            raise WorkflowNotRunning(f"workflow {workflow_id} is {rec.status.value}")
