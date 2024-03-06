"""Folding a history into state.

:class:`HistoryView` is a pure, incremental fold over events. The replayer uses it to
answer "what did command #k resolve to?", the worker uses it to know which activities are
in flight and which signal waits are open, and ``describe`` uses it for the CLI. Because
every consumer reads the same fold, there is exactly one definition of workflow state.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from replayagent.errors import HistoryCorruptError
from replayagent.events import COMMAND_TYPES, TERMINAL_TYPES, Event, EventType


@dataclass(frozen=True)
class Resolution:
    """Outcome of a blocking command, as recorded in history."""

    seq: int
    ok: bool
    value: Any = None
    error: dict[str, Any] | None = None


@dataclass
class ActivityState:
    cmd: int
    name: str
    input: list[Any]
    options: dict[str, Any]
    attempt: int = 0
    inflight: bool = False
    retry_at: float | None = None
    last_error: dict[str, Any] | None = None
    done: bool = False

    @property
    def delivery(self) -> str:
        return str(self.options.get("delivery", "at_least_once"))


@dataclass
class TimerState:
    cmd: int
    duration: float
    fire_at: float
    fired: bool = False


@dataclass
class SignalWaitState:
    cmd: int
    name: str
    timeout: float | None
    deadline: float | None
    resolved: bool = False


class HistoryView:
    """Incremental fold of a workflow history."""

    def __init__(self, events: Iterable[Event] = ()) -> None:
        self.events: list[Event] = []
        self.workflow_type = ""
        self.input: Any = None
        self.seed = 0
        self.started_at = 0.0
        self.code_hash: str | None = None
        self.commands: list[Event] = []
        self.resolutions: dict[int, Resolution] = {}
        self.activities: dict[int, ActivityState] = {}
        self.timers: dict[int, TimerState] = {}
        self.signal_waits: dict[int, SignalWaitState] = {}
        self.mailbox: list[Event] = []
        self.terminal: Event | None = None
        self.cancel_requested = False
        for event in events:
            self.apply(event)

    @property
    def last_seq(self) -> int:
        return len(self.events)

    def apply(self, ev: Event) -> None:
        """Fold one event; raises :class:`HistoryCorruptError` on invariant violations."""
        if ev.seq != len(self.events) + 1:
            raise HistoryCorruptError(f"expected event seq {len(self.events) + 1}, got {ev.seq}")
        if self.terminal is not None:
            raise HistoryCorruptError(f"event {ev.seq} ({ev.type}) follows a terminal event")
        if not self.events and ev.type is not EventType.WORKFLOW_STARTED:
            raise HistoryCorruptError("history must begin with WorkflowStarted")
        if ev.type in COMMAND_TYPES and ev.cmd != len(self.commands):
            raise HistoryCorruptError(
                f"command event {ev.seq} has cmd={ev.cmd}, expected {len(self.commands)}"
            )
        self.events.append(ev)
        handler = getattr(self, "_on_" + ev.type.name.lower())
        handler(ev)
        if ev.type in TERMINAL_TYPES:
            self.terminal = ev

    # -- helpers -------------------------------------------------------------------------

    def _cmd(self, ev: Event) -> int:
        if ev.cmd is None or ev.cmd >= len(self.commands):
            raise HistoryCorruptError(f"event {ev.seq} ({ev.type}) references unknown command")
        return ev.cmd

    def _activity(self, ev: Event) -> ActivityState:
        state = self.activities.get(self._cmd(ev))
        if state is None:
            raise HistoryCorruptError(f"event {ev.seq} ({ev.type}) is not about an activity")
        if state.done:
            raise HistoryCorruptError(f"event {ev.seq} ({ev.type}) after activity resolved")
        return state

    def _resolve(self, ev: Event, res: Resolution) -> None:
        cmd = self._cmd(ev)
        if cmd in self.resolutions:
            raise HistoryCorruptError(f"command #{cmd} resolved twice (event {ev.seq})")
        self.resolutions[cmd] = res

    # -- handlers ------------------------------------------------------------------------

    def _on_workflow_started(self, ev: Event) -> None:
        if len(self.events) != 1:
            raise HistoryCorruptError("WorkflowStarted must be the first event")
        self.workflow_type = str(ev.data["type"])
        self.input = ev.data.get("input")
        self.seed = int(ev.data.get("seed", 0))
        self.started_at = ev.ts
        self.code_hash = ev.data.get("code_hash")

    def _on_activity_scheduled(self, ev: Event) -> None:
        self.commands.append(ev)
        cmd = self._cmd(ev)
        self.activities[cmd] = ActivityState(
            cmd=cmd,
            name=str(ev.data["name"]),
            input=list(ev.data.get("input", [])),
            options=dict(ev.data.get("options", {})),
        )

    def _on_activity_started(self, ev: Event) -> None:
        state = self._activity(ev)
        attempt = int(ev.data["attempt"])
        if attempt != state.attempt + 1 or state.inflight:
            raise HistoryCorruptError(f"event {ev.seq}: unexpected start of attempt {attempt}")
        state.attempt = attempt
        state.inflight = True
        state.retry_at = None

    def _on_activity_attempt_failed(self, ev: Event) -> None:
        state = self._activity(ev)
        state.inflight = False
        state.retry_at = float(ev.data["retry_at"])
        state.last_error = dict(ev.data["error"])

    def _on_activity_completed(self, ev: Event) -> None:
        state = self._activity(ev)
        state.inflight = False
        state.done = True
        self._resolve(ev, Resolution(seq=ev.seq, ok=True, value=ev.data.get("result")))

    def _on_activity_failed(self, ev: Event) -> None:
        state = self._activity(ev)
        state.inflight = False
        state.done = True
        error = dict(ev.data["error"])
        error.setdefault("attempts", state.attempt)
        state.last_error = error
        self._resolve(ev, Resolution(seq=ev.seq, ok=False, error=error))

    def _on_timer_started(self, ev: Event) -> None:
        self.commands.append(ev)
        cmd = self._cmd(ev)
        self.timers[cmd] = TimerState(
            cmd=cmd, duration=float(ev.data["duration"]), fire_at=float(ev.data["fire_at"])
        )

    def _on_timer_fired(self, ev: Event) -> None:
        timer = self.timers.get(self._cmd(ev))
        if timer is None or timer.fired:
            raise HistoryCorruptError(f"event {ev.seq}: TimerFired for unknown or fired timer")
        timer.fired = True
        self._resolve(ev, Resolution(seq=ev.seq, ok=True))

    def _on_signal_received(self, ev: Event) -> None:
        self.mailbox.append(ev)

    def _on_signal_wait_started(self, ev: Event) -> None:
        self.commands.append(ev)
        cmd = self._cmd(ev)
        timeout = ev.data.get("timeout")
        deadline = ev.data.get("deadline")
        self.signal_waits[cmd] = SignalWaitState(
            cmd=cmd,
            name=str(ev.data["name"]),
            timeout=None if timeout is None else float(timeout),
            deadline=None if deadline is None else float(deadline),
        )

    def _wait(self, ev: Event) -> SignalWaitState:
        wait = self.signal_waits.get(self._cmd(ev))
        if wait is None or wait.resolved:
            raise HistoryCorruptError(f"event {ev.seq}: resolves unknown or resolved signal wait")
        return wait

    def _on_signal_consumed(self, ev: Event) -> None:
        wait = self._wait(ev)
        signal_seq = int(ev.data["signal_seq"])
        for i, sig in enumerate(self.mailbox):
            if sig.seq == signal_seq:
                if sig.data.get("name") != wait.name:
                    raise HistoryCorruptError(f"event {ev.seq}: signal name mismatch")
                del self.mailbox[i]
                break
        else:
            raise HistoryCorruptError(f"event {ev.seq}: consumes unknown signal {signal_seq}")
        wait.resolved = True
        self._resolve(ev, Resolution(seq=ev.seq, ok=True, value=ev.data.get("payload")))

    def _on_signal_wait_timed_out(self, ev: Event) -> None:
        wait = self._wait(ev)
        wait.resolved = True
        self._resolve(
            ev,
            Resolution(
                seq=ev.seq,
                ok=False,
                error={"type": "SignalTimeout", "message": f"no {wait.name!r} signal"},
            ),
        )

    def _on_side_effect(self, ev: Event) -> None:
        self.commands.append(ev)

    def _on_version_marker(self, ev: Event) -> None:
        self.commands.append(ev)

    def _on_cancel_requested(self, ev: Event) -> None:
        self.cancel_requested = True

    def _on_workflow_completed(self, ev: Event) -> None:
        pass

    def _on_workflow_failed(self, ev: Event) -> None:
        pass

    def _on_workflow_cancelled(self, ev: Event) -> None:
        pass

    # -- queries -------------------------------------------------------------------------

    def pending_activities(self) -> list[ActivityState]:
        return [a for a in self.activities.values() if not a.done]

    def pending_timers(self) -> list[TimerState]:
        return [t for t in self.timers.values() if not t.fired]

    def open_signal_waits(self) -> list[SignalWaitState]:
        return sorted(
            (w for w in self.signal_waits.values() if not w.resolved), key=lambda w: w.cmd
        )
