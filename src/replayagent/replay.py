"""Deterministic replay: running workflow code against its history.

A *workflow task* re-executes the workflow function from the top. Every durable operation
the code performs (``activity``, ``sleep``, ``wait_signal``, ``side_effect``,
``get_version``) is a *command* with a position ``cmd``. For each command:

* if history already has a command event at that position, the two are compared - any
  difference is a :class:`~replayagent.errors.NonDeterminismError` - and the recorded
  outcome is returned without doing the work again;
* otherwise the command is new and is collected for the worker to record.

When code blocks on a command whose outcome is not in history yet, the context raises the
internal :class:`_Suspend` signal, which unwinds the function. The worker records the new
commands, makes progress on them (runs activities, waits for timers or signals), appends
the outcomes and runs another task. The function therefore only ever *observes*
history, which is what makes crash recovery a pure re-read of the event log.
"""

from __future__ import annotations

import logging
import random
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal, NoReturn

from replayagent import serde
from replayagent.errors import (
    ActivityError,
    NonDeterminismError,
    SerializationError,
    SignalTimeout,
    UnknownActivity,
    VersionError,
    WorkflowTaskError,
)
from replayagent.events import Event, EventType
from replayagent.history import HistoryView, Resolution
from replayagent.registry import ActivityOptions, App, WorkflowDefinition, activity_name_of

#: ``get_version`` returns this for workflows whose history predates the change.
DEFAULT_VERSION = -1

_log = logging.getLogger("replayagent.workflow")


class _Suspend(BaseException):
    """Unwinds workflow code when it blocks on an unresolved command.

    It derives from ``BaseException`` so ``except Exception`` in workflow code does not
    swallow it. Code that catches ``BaseException`` is detected after the fact.
    """


@dataclass(frozen=True)
class NewCommand:
    """A command issued by workflow code that history does not contain yet."""

    cmd: int
    type: EventType
    data: dict[str, Any]


@dataclass
class TaskResult:
    status: Literal["suspended", "completed", "failed"]
    commands: list[NewCommand] = field(default_factory=list)
    result: Any = None
    error: dict[str, Any] | None = None
    replayed_commands: int = 0


class ReplayAwareLogger:
    """Logger for workflow code that stays quiet while history is being replayed."""

    def __init__(self, ctx: WorkflowContext) -> None:
        self._ctx = ctx

    def _log(self, level: int, msg: str, *args: object) -> None:
        if not self._ctx.is_replaying:
            _log.log(level, "[%s] " + msg, self._ctx.workflow_id, *args)

    def debug(self, msg: str, *args: object) -> None:
        self._log(logging.DEBUG, msg, *args)

    def info(self, msg: str, *args: object) -> None:
        self._log(logging.INFO, msg, *args)

    def warning(self, msg: str, *args: object) -> None:
        self._log(logging.WARNING, msg, *args)

    def error(self, msg: str, *args: object) -> None:
        self._log(logging.ERROR, msg, *args)


class _Handle:
    def __init__(self, ctx: WorkflowContext, cmd: int) -> None:
        self._ctx = ctx
        self.cmd = cmd

    def _resolution(self) -> Resolution | None:
        return self._ctx._resolution(self.cmd)


class ActivityHandle(_Handle):
    """A scheduled activity. :meth:`result` blocks (suspends) until it is resolved."""

    def __init__(self, ctx: WorkflowContext, cmd: int, name: str) -> None:
        super().__init__(ctx, cmd)
        self.name = name

    def result(self) -> Any:
        res = self._ctx._await(self.cmd)
        if res.ok:
            return res.value
        raise ActivityError(self.name, self.cmd, res.error or {})


class TimerHandle(_Handle):
    """A durable timer. :meth:`wait` blocks (suspends) until it fires."""

    def wait(self) -> None:
        self._ctx._await(self.cmd)

    def result(self) -> None:
        self.wait()


class WorkflowContext:
    """The only door from workflow code to the outside world.

    Workflow code must be deterministic: given the same history it must issue the same
    commands. Use the context instead of ``time.time()``, ``random`` or I/O.
    """

    def __init__(
        self,
        *,
        workflow_id: str,
        view: HistoryView,
        app: App,
        now: Callable[[], float],
    ) -> None:
        self.workflow_id = workflow_id
        self.workflow_type = view.workflow_type
        self._view = view
        self._app = app
        self._now = now
        self._cursor = 0
        self._new: list[NewCommand] = []
        self._suspended = False
        self._violation: WorkflowTaskError | None = None
        self._rng: random.Random | None = None
        #: Version decided for each change id in this execution (first call wins).
        self._versions: dict[str, int] = {}
        #: Positions of version markers already consumed out of order by ``get_version``.
        self._consumed_markers: set[int] = set()
        self.logger = ReplayAwareLogger(self)

    # -- introspection -------------------------------------------------------------------

    @property
    def is_replaying(self) -> bool:
        """True while the code is re-issuing commands that history already contains."""
        return self._cursor < len(self._view.commands)

    @property
    def start_time(self) -> float:
        """Time the workflow was started (recorded, so replay-safe)."""
        return self._view.started_at

    # -- activities ----------------------------------------------------------------------

    def start_activity(
        self,
        act: str | Callable[..., Any],
        *args: Any,
        options: ActivityOptions | None = None,
    ) -> ActivityHandle:
        """Schedule an activity without waiting for it (use for fan-out)."""
        self._guard()
        name = activity_name_of(act)
        try:
            defn = self._app.get_activity(name)
        except UnknownActivity as exc:
            self._fail(WorkflowTaskError(str(exc)))
        try:
            encoded_args = serde.roundtrip(list(args))
        except SerializationError as exc:
            self._fail(WorkflowTaskError(f"activity {name!r} arguments: {exc}"))
        data = {
            "name": name,
            "input": encoded_args,
            "input_hash": serde.digest(encoded_args),
            "options": defn.options.merged(options).to_history(),
        }
        cmd = self._issue(EventType.ACTIVITY_SCHEDULED, data)
        return ActivityHandle(self, cmd, name)

    def activity(
        self,
        act: str | Callable[..., Any],
        *args: Any,
        options: ActivityOptions | None = None,
    ) -> Any:
        """Run an activity and return its result (raises :class:`ActivityError`)."""
        return self.start_activity(act, *args, options=options).result()

    def gather(self, *handles: ActivityHandle) -> list[Any]:
        """Wait for every handle; results in argument order. The first failure in
        argument order (once all earlier handles resolved) is raised."""
        return [h.result() for h in handles]

    def wait_any(self, *handles: _Handle) -> _Handle:
        """Return the handle whose outcome was recorded *first* in history.

        Choosing by history position (not by "what is ready now") keeps the choice
        identical on every replay.
        """
        self._guard()
        if not handles:
            raise ValueError("wait_any needs at least one handle")
        resolved = [(res.seq, h) for h in handles if (res := h._resolution()) is not None]
        if not resolved:
            self._suspend()
        return min(resolved, key=lambda pair: pair[0])[1]

    # -- timers & signals ----------------------------------------------------------------

    def start_timer(self, seconds: float) -> TimerHandle:
        self._guard()
        if seconds < 0:
            raise ValueError("timer duration must be >= 0")
        cmd = self._issue(EventType.TIMER_STARTED, {"duration": float(seconds)})
        return TimerHandle(self, cmd)

    def sleep(self, seconds: float) -> None:
        """Durable sleep: survives crashes and restarts."""
        self.start_timer(seconds).wait()

    def wait_signal(self, name: str, *, timeout: float | None = None) -> Any:
        """Block until an external signal ``name`` arrives and return its payload.

        Signals sent before the wait are buffered. Raises :class:`SignalTimeout` if
        ``timeout`` seconds pass first.
        """
        self._guard()
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be > 0")
        cmd = self._issue(
            EventType.SIGNAL_WAIT_STARTED,
            {"name": name, "timeout": None if timeout is None else float(timeout)},
        )
        res = self._await(cmd)
        if res.ok:
            return res.value
        raise SignalTimeout(name, timeout)

    # -- non-determinism escape hatches ---------------------------------------------------

    def side_effect(self, fn: Callable[[], Any], *, label: str = "") -> Any:
        """Run ``fn`` once and record its JSON result; replays return the recorded value.

        For cheap, local non-determinism (ids, clocks). Anything with external effects
        belongs in an activity.
        """
        self._guard()
        self._skip_stale_markers(EventType.SIDE_EFFECT)
        cmd = self._cursor
        hist = self._view.commands
        if cmd < len(hist):
            self._match(cmd, hist[cmd], EventType.SIDE_EFFECT, {"label": label})
            self._cursor += 1
            return hist[cmd].data.get("value")
        try:
            value = serde.roundtrip(fn())
        except SerializationError as exc:
            self._fail(WorkflowTaskError(f"side_effect {label!r}: {exc}"))
        self._record(EventType.SIDE_EFFECT, {"label": label, "value": value})
        return value

    def now(self) -> float:
        """Current time, recorded once per call so replays see the same value."""
        return float(self.side_effect(self._now, label="now"))

    def random(self) -> random.Random:
        """A PRNG seeded from history; the same sequence on every replay."""
        if self._rng is None:
            self._rng = random.Random(self._view.seed)
        return self._rng

    def uuid4(self) -> str:
        """A random-looking UUID derived from :meth:`random` (replay-safe)."""
        return str(uuid.UUID(int=self.random().getrandbits(128), version=4))

    def get_version(self, change_id: str, min_supported: int, max_supported: int) -> int:
        """Branch safely around a code change (Temporal-style ``GetVersion``).

        New executions record ``max_supported``. Executions whose history predates the
        change get :data:`DEFAULT_VERSION`. A recorded version outside
        ``[min_supported, max_supported]`` blocks the workflow with :class:`VersionError`.

        The version is decided once per ``change_id`` and execution: later calls with the
        same id return the same value (and record nothing), so one change may guard several
        sites. Recorded markers are matched by change id within the run of markers at the
        current position, so deleting or reordering ``get_version`` calls for *other* ids
        does not disturb this one.
        """
        self._guard()
        if min_supported > max_supported:
            raise ValueError("min_supported must be <= max_supported")
        recorded = self._take_marker(change_id)
        if recorded is not None:
            # History is the truth, even if an older build recorded a second marker for an
            # id that this execution already decided.
            version = recorded
            self._versions.setdefault(change_id, version)
        elif change_id in self._versions:
            version = self._versions[change_id]
        elif self._cursor < len(self._view.commands):
            version = DEFAULT_VERSION
            self._versions[change_id] = version
        else:
            version = max_supported
            self._versions[change_id] = version
            self._record(EventType.VERSION_MARKER, {"change_id": change_id, "version": version})
        if not min_supported <= version <= max_supported:
            self._fail(
                VersionError(
                    f"get_version({change_id!r}): history has version {version}, code supports "
                    f"[{min_supported}, {max_supported}]"
                )
            )
        return version

    def _take_marker(self, change_id: str) -> int | None:
        """Consume the recorded marker for ``change_id`` in the marker run at the cursor."""
        hist = self._view.commands
        pos = self._cursor
        while pos < len(hist) and hist[pos].type is EventType.VERSION_MARKER:
            if pos not in self._consumed_markers and hist[pos].data.get("change_id") == change_id:
                self._consumed_markers.add(pos)
                # Advance over the consumed prefix; unconsumed markers ahead of a consumed one
                # stay for later calls and are skipped as stale by the next other command.
                while self._cursor in self._consumed_markers:
                    self._cursor += 1
                return int(hist[pos].data["version"])
            pos += 1
        return None

    # -- internals -----------------------------------------------------------------------

    def _guard(self) -> None:
        # After a suspension or a determinism violation, nothing the code does is real.
        if self._suspended or self._violation is not None:
            raise _Suspend

    def _suspend(self) -> NoReturn:
        self._suspended = True
        raise _Suspend

    def _fail(self, err: WorkflowTaskError) -> NoReturn:
        self._violation = err
        raise err

    def _skip_stale_markers(self, issuing: EventType) -> None:
        """Skip recorded version markers that the current code no longer asks for."""
        hist = self._view.commands
        if issuing is EventType.VERSION_MARKER:
            return
        while self._cursor < len(hist) and hist[self._cursor].type is EventType.VERSION_MARKER:
            self._cursor += 1

    def _issue(self, etype: EventType, data: dict[str, Any]) -> int:
        self._skip_stale_markers(etype)
        cmd = self._cursor
        hist = self._view.commands
        if cmd < len(hist):
            self._match(cmd, hist[cmd], etype, data)
            self._cursor += 1
            return cmd
        return self._record(etype, data)

    def _record(self, etype: EventType, data: dict[str, Any]) -> int:
        cmd = self._cursor
        self._new.append(NewCommand(cmd=cmd, type=etype, data=data))
        self._cursor += 1
        return cmd

    def _match(self, cmd: int, ev: Event, etype: EventType, data: dict[str, Any]) -> None:
        reason = ""
        if ev.type is not etype:
            reason = "command type differs"
        elif etype is EventType.ACTIVITY_SCHEDULED:
            if ev.data.get("name") != data["name"]:
                reason = "activity name differs"
            elif ev.data.get("input_hash") != data["input_hash"]:
                reason = "activity input differs"
        elif etype is EventType.TIMER_STARTED:
            if float(ev.data.get("duration", -1)) != data["duration"]:
                reason = "timer duration differs"
        elif etype is EventType.SIGNAL_WAIT_STARTED:
            if ev.data.get("name") != data["name"]:
                reason = "signal name differs"
            elif ev.data.get("timeout") != data["timeout"]:
                reason = "signal timeout differs"
        elif etype is EventType.SIDE_EFFECT and ev.data.get("label", "") != data["label"]:
            reason = "side effect label differs"
        if reason:
            self._fail(
                NonDeterminismError(
                    f"non-deterministic workflow {self.workflow_id!r} at command #{cmd}: "
                    f"{reason}; history has {describe_command(ev.type, ev.data)} "
                    f"(event {ev.seq}) but code issued {describe_command(etype, data)}",
                    cmd=cmd,
                )
            )

    def _resolution(self, cmd: int) -> Resolution | None:
        if cmd >= len(self._view.commands):
            return None
        return self._view.resolutions.get(cmd)

    def _await(self, cmd: int) -> Resolution:
        self._guard()
        res = self._resolution(cmd)
        if res is None:
            self._suspend()
        return res

    def _remaining_history_commands(self) -> list[Event]:
        return [
            ev
            for ev in self._view.commands[self._cursor :]
            if ev.type is not EventType.VERSION_MARKER
        ]


def describe_command(etype: EventType, data: dict[str, Any]) -> str:
    """Human-readable one-liner for a command (used in error messages and the CLI)."""
    if etype is EventType.ACTIVITY_SCHEDULED:
        return f"activity {data.get('name')!r} (input {data.get('input_hash', '?')})"
    if etype is EventType.TIMER_STARTED:
        return f"timer {data.get('duration')}s"
    if etype is EventType.SIGNAL_WAIT_STARTED:
        return f"wait_signal {data.get('name')!r} (timeout {data.get('timeout')})"
    if etype is EventType.SIDE_EFFECT:
        return f"side_effect {data.get('label', '')!r}"
    if etype is EventType.VERSION_MARKER:
        return f"version {data.get('change_id')!r}={data.get('version')}"
    return etype.value


def execute_task(
    defn: WorkflowDefinition,
    *,
    workflow_id: str,
    view: HistoryView,
    app: App,
    now: Callable[[], float],
) -> TaskResult:
    """Run one workflow task. Raises :class:`WorkflowTaskError` (incl. non-determinism)."""
    ctx = WorkflowContext(workflow_id=workflow_id, view=view, app=app, now=now)
    status: Literal["completed", "failed"]
    value: Any = None
    error: dict[str, Any] | None = None
    try:
        value = defn.fn(ctx, view.input)
        status = "completed"
    except _Suspend:
        if ctx._violation is not None:
            raise ctx._violation from None
        return TaskResult("suspended", ctx._new, replayed_commands=ctx._cursor - len(ctx._new))
    except Exception as exc:
        if ctx._violation is not None:
            raise ctx._violation from None
        if ctx._suspended:
            raise WorkflowTaskError(
                "workflow code swallowed a suspension (does it catch BaseException?)"
            ) from exc
        status = "failed"
        error = serde.encode_error(exc)
    if ctx._violation is not None:
        raise ctx._violation
    if ctx._suspended:
        raise WorkflowTaskError(
            "workflow returned after a suspension was swallowed (does it catch BaseException?)"
        )
    leftover = ctx._remaining_history_commands()
    if leftover:
        first = leftover[0]
        raise NonDeterminismError(
            f"non-deterministic workflow {workflow_id!r}: code finished after command "
            f"#{ctx._cursor - len(ctx._new)} but history has {len(leftover)} more command(s), "
            f"first {describe_command(first.type, first.data)} (event {first.seq})",
            cmd=first.cmd,
        )
    if status == "completed":
        try:
            value = serde.roundtrip(value)
        except SerializationError as exc:
            status, value, error = "failed", None, serde.encode_error(exc)
    replayed = ctx._cursor - len(ctx._new)
    return TaskResult(status, ctx._new, result=value, error=error, replayed_commands=replayed)
