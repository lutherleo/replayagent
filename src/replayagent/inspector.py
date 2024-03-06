"""Read-only views of histories: describe, pretty-print, export, and offline replay.

:func:`replay_history` is the "replayer" used before deploying a code change: it runs the
*current* workflow code against a *recorded* history without touching any database and
reports the first divergence. Committed golden histories plus this function make
determinism a regular unit test.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from replayagent import serde
from replayagent.errors import HistoryCorruptError, ReplayAgentError, WorkflowTaskError
from replayagent.events import Event, EventType, WorkflowStatus
from replayagent.history import HistoryView
from replayagent.registry import App
from replayagent.replay import describe_command, execute_task
from replayagent.store import Store, TimerRow, WorkflowRecord

EXPORT_FORMAT = "replayagent-history/1"


# -- describe -------------------------------------------------------------------------------


@dataclass(frozen=True)
class PendingActivity:
    cmd: int
    name: str
    state: str
    attempt: int
    retry_at: float | None
    last_error: str | None


@dataclass(frozen=True)
class WorkflowDescription:
    record: WorkflowRecord
    events: int
    commands: int
    activities: list[PendingActivity]
    timers: list[TimerRow]
    signal_waits: list[tuple[int, str, float | None]]
    buffered_signals: list[str]
    undelivered_inbox: int
    last_event: Event | None
    code_hash_current: str | None = None


def describe(store: Store, workflow_id: str, app: App | None = None) -> WorkflowDescription:
    record = store.get_workflow(workflow_id)
    view = HistoryView(store.load_events(workflow_id))
    acts = []
    for st in view.pending_activities():
        if st.inflight:
            state = "running"
        elif st.retry_at is not None:
            state = "retrying"
        else:
            state = "scheduled"
        last = None if st.last_error is None else str(st.last_error.get("type"))
        acts.append(PendingActivity(st.cmd, st.name, state, st.attempt, st.retry_at, last))
    current_hash = None
    if app is not None and view.workflow_type in app.workflow_names:
        current_hash = app.get_workflow(view.workflow_type).code_hash
    return WorkflowDescription(
        record=record,
        events=view.last_seq,
        commands=len(view.commands),
        activities=acts,
        timers=[t for t in store.timers_for(workflow_id) if t.kind == "timer"],
        signal_waits=[(w.cmd, w.name, w.deadline) for w in view.open_signal_waits()],
        buffered_signals=[str(e.data.get("name")) for e in view.mailbox],
        undelivered_inbox=store.pending_inbox(workflow_id),
        last_event=view.events[-1] if view.events else None,
        code_hash_current=current_hash,
    )


def _when(ts: float | None) -> str:
    if ts is None:
        return "-"
    return dt.datetime.fromtimestamp(ts, tz=dt.UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"


def _short(value: Any, width: int) -> str:
    text = value if isinstance(value, str) else serde.dumps(value)
    text = text.replace("\n", "\\n")
    return text if width <= 0 or len(text) <= width else text[: max(width - 1, 1)] + "…"


def format_description(desc: WorkflowDescription, now: float) -> str:
    rec = desc.record
    hash_note = rec.code_hash or "-"
    if desc.code_hash_current is not None and rec.code_hash:
        same = desc.code_hash_current == rec.code_hash
        hash_note += f" (current {desc.code_hash_current}{'' if same else ', CHANGED since start'})"
    lines = [
        f"workflow   {rec.id}",
        f"type       {rec.type}",
        f"status     {rec.status.value}",
        f"created    {_when(rec.created_at)}",
        f"updated    {_when(rec.updated_at)}",
        f"history    {desc.events} events, {desc.commands} commands",
        f"code hash  {hash_note}",
        f"owner      {rec.owner or '-'}",
    ]
    if rec.status is WorkflowStatus.COMPLETED:
        lines.append(f"result     {_short(rec.result, 300)}")
    if rec.error:
        lines.append(
            f"error      {rec.error.get('type')}: {_short(rec.error.get('message', ''), 300)}"
        )
    pending: list[str] = []
    for act in desc.activities:
        extra = f" attempt {act.attempt}" if act.attempt else ""
        if act.retry_at is not None:
            extra += f", retry in {max(act.retry_at - now, 0):.2f}s after {act.last_error}"
        pending.append(f"  activity #{act.cmd} {act.name}: {act.state}{extra}")
    for timer in desc.timers:
        pending.append(f"  timer #{timer.cmd}: fires in {timer.fire_at - now:.2f}s")
    for cmd, name, deadline in desc.signal_waits:
        until = "no timeout" if deadline is None else f"times out in {deadline - now:.1f}s"
        pending.append(f"  wait_signal #{cmd} {name!r}: {until}")
    if pending:
        lines.append("pending")
        lines.extend(pending)
    if desc.buffered_signals:
        lines.append(f"buffered   signals {', '.join(desc.buffered_signals)}")
    if desc.undelivered_inbox:
        lines.append(f"inbox      {desc.undelivered_inbox} item(s) waiting for a worker")
    return "\n".join(lines)


# -- history table --------------------------------------------------------------------------


def event_details(ev: Event, width: int = 70) -> str:
    d = ev.data
    t = ev.type
    if t is EventType.WORKFLOW_STARTED:
        return f"type={d.get('type')} input={_short(d.get('input'), width)}"
    if t is EventType.ACTIVITY_SCHEDULED:
        opts = d.get("options", {})
        delivery = (
            "" if opts.get("delivery", "at_least_once") == "at_least_once" else " at-most-once"
        )
        return f"{d.get('name')}{delivery} args={_short(d.get('input'), width)}"
    if t is EventType.ACTIVITY_STARTED:
        return f"attempt={d.get('attempt')}"
    if t is EventType.ACTIVITY_ATTEMPT_FAILED:
        err = d.get("error", {})
        delay = max(float(d.get("retry_at", ev.ts)) - ev.ts, 0.0)
        msg = _short(err.get("message", ""), width // 2)
        return f"attempt={d.get('attempt')} {err.get('type')}: {msg} retry_in={delay:.3f}s"
    if t is EventType.ACTIVITY_COMPLETED:
        return f"attempt={d.get('attempt')} result={_short(d.get('result'), width)}"
    if t is EventType.ACTIVITY_FAILED:
        err = d.get("error", {})
        msg = _short(err.get("message", ""), width)
        return f"attempts={err.get('attempts')} {err.get('type')}: {msg}"
    if t is EventType.TIMER_STARTED:
        return f"duration={d.get('duration')}s"
    if t is EventType.SIGNAL_RECEIVED:
        return f"{d.get('name')} payload={_short(d.get('payload'), width)}"
    if t is EventType.SIGNAL_WAIT_STARTED:
        return f"{d.get('name')} timeout={d.get('timeout')}"
    if t is EventType.SIGNAL_CONSUMED:
        payload = _short(d.get("payload"), width)
        return f"{d.get('name')} from event {d.get('signal_seq')} payload={payload}"
    if t is EventType.SIDE_EFFECT:
        return f"{d.get('label') or '-'} value={_short(d.get('value'), width)}"
    if t is EventType.VERSION_MARKER:
        return f"{d.get('change_id')}={d.get('version')}"
    if t is EventType.CANCEL_REQUESTED:
        return f"reason={_short(d.get('reason'), width)}"
    if t is EventType.WORKFLOW_COMPLETED:
        return f"result={_short(d.get('result'), width)}"
    if t is EventType.WORKFLOW_FAILED:
        err = d.get("error", {})
        return f"{err.get('type')}: {_short(err.get('message', ''), width)}"
    return ""


def format_history(events: list[Event], *, width: int = 70) -> str:
    if not events:
        return "(empty history)"
    t0 = events[0].ts
    rows = [f"{'seq':>4}  {'t+s':>9}  {'event':<22} {'cmd':>4}  details"]
    for ev in events:
        cmd = "-" if ev.cmd is None else str(ev.cmd)
        details = event_details(ev, width)
        rows.append(f"{ev.seq:>4}  {ev.ts - t0:>9.3f}  {ev.type.value:<22} {cmd:>4}  {details}")
    return "\n".join(rows)


# -- export / import ------------------------------------------------------------------------


def export_history(record: WorkflowRecord, events: list[Event]) -> dict[str, Any]:
    return {
        "format": EXPORT_FORMAT,
        "workflow": {"id": record.id, "type": record.type, "status": record.status.value},
        "events": [e.to_dict() for e in events],
    }


def load_history_file(path: str | Path) -> tuple[str, list[Event]]:
    """Read an exported history; returns ``(workflow_id, events)``."""
    raw = serde.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or raw.get("format") != EXPORT_FORMAT:
        raise HistoryCorruptError(f"{path}: not a {EXPORT_FORMAT} file")
    return str(raw["workflow"]["id"]), [Event.from_dict(e) for e in raw["events"]]


# -- offline replay -------------------------------------------------------------------------


@dataclass
class ReplayReport:
    workflow_id: str
    workflow_type: str
    events: int
    history_commands: int
    replayed_commands: int
    outcome: str
    ok: bool
    problem: str | None = None
    next_commands: list[str] = field(default_factory=list)

    def summary(self) -> str:
        if not self.ok:
            return f"DIVERGED  {self.workflow_id}: {self.problem}"
        tail = ""
        if self.next_commands:
            tail = f"; next: {', '.join(self.next_commands)}"
        return (
            f"OK  {self.workflow_id} ({self.workflow_type}): replayed {self.replayed_commands}/"
            f"{self.history_commands} commands over {self.events} events, "
            f"outcome {self.outcome}{tail}"
        )


def replay_history(app: App, events: list[Event], workflow_id: str = "replay") -> ReplayReport:
    """Re-run current code against ``events`` and compare; never writes anything."""
    view = HistoryView(events)
    report = ReplayReport(
        workflow_id=workflow_id,
        workflow_type=view.workflow_type,
        events=view.last_seq,
        history_commands=len(view.commands),
        replayed_commands=0,
        outcome="error",
        ok=False,
    )
    last_ts = events[-1].ts if events else 0.0
    try:
        defn = app.get_workflow(view.workflow_type)
        res = execute_task(defn, workflow_id=workflow_id, view=view, app=app, now=lambda: last_ts)
    except WorkflowTaskError as exc:
        report.problem = str(exc)
        return report
    except ReplayAgentError as exc:
        report.problem = f"{type(exc).__name__}: {exc}"
        return report
    report.outcome = res.status
    report.replayed_commands = res.replayed_commands
    report.next_commands = [describe_command(c.type, c.data) for c in res.commands]
    terminal = view.terminal
    problem = None
    if terminal is not None and terminal.type is not EventType.WORKFLOW_CANCELLED:
        if res.commands:
            problem = f"history is closed but code issues new command {report.next_commands[0]}"
        elif terminal.type is EventType.WORKFLOW_COMPLETED:
            if res.status != "completed":
                problem = f"history completed but code ended {res.status}"
            elif serde.digest(res.result) != serde.digest(terminal.data.get("result")):
                problem = "code returns a different result than the recorded one"
        elif terminal.type is EventType.WORKFLOW_FAILED:
            recorded = (terminal.data.get("error") or {}).get("type")
            if res.status != "failed":
                problem = f"history failed ({recorded}) but code ended {res.status}"
            elif (res.error or {}).get("type") != recorded:
                problem = (
                    f"code fails with {(res.error or {}).get('type')}, history with {recorded}"
                )
    report.problem = problem
    report.ok = problem is None
    return report
