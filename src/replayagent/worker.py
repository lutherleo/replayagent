"""The worker: leases workflows, runs workflow tasks and activities, fires timers.

The worker is single-writer per workflow. Its main thread owns every history append;
activity functions run on a thread pool and hand their outcomes back through a queue.
Each loop iteration (:meth:`Worker.tick`) does, in order:

1. record activity outcomes that came back from the pool;
2. claim workflows that are new, or whose previous owner died / let its lease expire,
   and *recover* them (see :meth:`Worker._recover`);
3. move signals and cancellations from the inbox into history;
4. fire due timers (durable sleeps, retry backoff, activity and signal timeouts);
5. run a workflow task for every workflow whose history grew.

Every step that changes state is one SQLite transaction guarded by a lease
compare-and-set, and every gap between two transactions is a named crash point
(:mod:`replayagent.faults`), so the crash tests can kill the process in each gap.
"""

from __future__ import annotations

import functools
import logging
import queue
import random
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from replayagent import activity_context as activity_ctx
from replayagent import serde
from replayagent.clock import Clock, SystemClock
from replayagent.errors import HistoryCorruptError, LeaseLost, UnknownActivity, WorkflowTaskError
from replayagent.events import EventType, PendingEvent, WorkflowStatus
from replayagent.faults import SimulatedCrash, crashpoint
from replayagent.history import ActivityState, HistoryView
from replayagent.registry import App, WorkflowDefinition
from replayagent.replay import execute_task
from replayagent.retry import RetryPolicy
from replayagent.store import InboxItem, Store, TimerDel, TimerPut, TimerRow, new_owner_id

_log = logging.getLogger("replayagent.worker")


@dataclass
class WorkerStats:
    """Counters for observability, benchmarks and tests."""

    claimed: int = 0
    workflow_tasks: int = 0
    replayed_commands: int = 0
    activities_started: int = 0
    activities_completed: int = 0
    activity_attempt_failures: int = 0
    activities_failed: int = 0
    activities_recovered: int = 0
    timers_fired: int = 0
    signals_received: int = 0
    workflows_completed: int = 0
    workflows_blocked: int = 0


@dataclass
class _Runtime:
    workflow_id: str
    defn: WorkflowDefinition
    view: HistoryView
    dirty: bool = True


@dataclass(frozen=True)
class _Outcome:
    workflow_id: str
    cmd: int
    attempt: int
    ok: bool
    value: Any = None
    error: dict[str, Any] | None = None


@dataclass(frozen=True)
class _Crashed:
    crash: SimulatedCrash


@dataclass
class _Pending:
    """Events and timer-index changes destined for one append."""

    events: list[PendingEvent] = field(default_factory=list)
    timers: list[TimerPut | TimerDel] = field(default_factory=list)


class Worker:
    """Runs workflows from a :class:`~replayagent.store.Store` using an :class:`App`."""

    def __init__(
        self,
        store: Store,
        app: App,
        *,
        clock: Clock | None = None,
        identity: str | None = None,
        activity_threads: int = 8,
        lease_ttl: float = 30.0,
        poll_interval: float = 0.02,
        max_workflows: int = 1000,
        jitter_seed: int | None = None,
    ) -> None:
        if lease_ttl <= 0 or poll_interval <= 0:
            raise ValueError("lease_ttl and poll_interval must be > 0")
        self.store = store
        self.app = app
        self.clock: Clock = clock or SystemClock()
        self.identity = identity or new_owner_id()
        self.stats = WorkerStats()
        self._pool = ThreadPoolExecutor(
            max_workers=activity_threads, thread_name_prefix="replayagent-activity"
        )
        self._outcomes: queue.SimpleQueue[_Outcome | _Crashed] = queue.SimpleQueue()
        self._runtimes: dict[str, _Runtime] = {}
        self._inflight: set[tuple[str, int, int]] = set()
        self._lease_ttl = lease_ttl
        self._poll = poll_interval
        self._max_workflows = max_workflows
        self._rng = random.Random(jitter_seed)
        self._last_renew_mono = time.monotonic()
        self._last_renew_clock = self.clock.now()
        self._closed = False

    # -- public loop drivers ---------------------------------------------------------------

    def tick(self) -> bool:
        """Do one round of work; returns True if anything happened."""
        self._ensure_open()
        progressed = self._drain_outcomes(timeout=0.0)
        progressed |= self._claim()
        progressed |= self._process_inbox()
        progressed |= self._fire_due_timers()
        progressed |= self._run_dirty_tasks()
        self._maybe_renew()
        return progressed

    def run_until_idle(self, *, max_timer_wait: float = 0.0, timeout: float = 60.0) -> None:
        """Work until nothing is runnable.

        Timers due within ``max_timer_wait`` seconds are waited for (instantly on a
        virtual clock); later timers and signal waits leave the worker idle.
        """
        deadline = time.monotonic() + timeout
        while True:
            if time.monotonic() > deadline:
                raise TimeoutError("worker did not become idle in time")
            if self.tick():
                continue
            if self._inflight:
                self._drain_outcomes(timeout=self._poll)
                continue
            next_at = self.store.next_timer(self.identity)
            if next_at is None:
                return
            delay = next_at - self.clock.now()
            if delay > max_timer_wait:
                return
            self._idle(delay)

    def run_until_complete(self, workflow_id: str, *, timeout: float = 60.0) -> WorkflowStatus:
        """Work until ``workflow_id`` leaves the running state; returns its status."""
        deadline = time.monotonic() + timeout
        while True:
            status = self.store.get_workflow(workflow_id).status
            if status is not WorkflowStatus.RUNNING:
                return status
            if time.monotonic() > deadline:
                raise TimeoutError(f"workflow {workflow_id} still running after {timeout}s")
            if self.tick():
                continue
            if self._inflight:
                self._drain_outcomes(timeout=self._poll)
                continue
            next_at = self.store.next_timer(self.identity)
            if next_at is None:
                self._pause()
            else:
                self._idle(next_at - self.clock.now())

    def run_forever(self, stop: threading.Event | None = None) -> None:
        """Serve until ``stop`` is set (or forever)."""
        stop = stop or threading.Event()
        while not stop.is_set():
            if self.tick():
                continue
            next_at = self.store.next_timer(self.identity)
            if next_at is None or self.clock.virtual:
                self._pause()
            else:
                self._idle(next_at - self.clock.now())

    def close(self) -> None:
        """Finish in-flight activities, record their outcomes and release leases."""
        if self._closed:
            return
        self._pool.shutdown(wait=True)
        try:
            self._drain_outcomes(timeout=0.0)
        finally:
            self._closed = True
            self.store.release(self.identity)
            self._runtimes.clear()

    def __enter__(self) -> Worker:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def owned_workflows(self) -> list[str]:
        return sorted(self._runtimes)

    # -- plumbing ----------------------------------------------------------------------------

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("worker is closed")

    def _idle(self, delay: float) -> None:
        """Wait for the next timer: jump on a virtual clock, poll-sized naps otherwise."""
        if delay <= 0:
            return
        if self.clock.virtual:
            self.clock.sleep(delay)
        else:
            self._drain_outcomes(timeout=min(delay, self._poll))

    def _pause(self) -> None:
        """Real-time nap while waiting for outside input (signals, other processes)."""
        self._drain_outcomes(timeout=self._poll)

    def _drop(self, workflow_id: str) -> None:
        if self._runtimes.pop(workflow_id, None) is not None:
            self.store.release(self.identity, [workflow_id])

    def _guarded(self, rt: _Runtime, op: Callable[[], None]) -> None:
        try:
            op()
        except LeaseLost:
            _log.warning("lost lease on %s; dropping it", rt.workflow_id)
            self._runtimes.pop(rt.workflow_id, None)

    def _append(
        self,
        rt: _Runtime,
        pending: _Pending,
        *,
        status: WorkflowStatus | None = None,
        result: Any = None,
        error: dict[str, Any] | None = None,
        consume_inbox: Sequence[int] = (),
    ) -> None:
        stored = self.store.append(
            rt.workflow_id,
            self.identity,
            rt.view.last_seq,
            pending.events,
            now=self.clock.now(),
            timers=pending.timers,
            status=status,
            result=result,
            error=error,
            consume_inbox=consume_inbox,
        )
        for ev in stored:
            rt.view.apply(ev)
        if status is not None and status.is_terminal:
            self._runtimes.pop(rt.workflow_id, None)
            self.stats.workflows_completed += 1
            _log.info("workflow %s finished: %s", rt.workflow_id, status.value)

    def _maybe_renew(self) -> None:
        interval = self._lease_ttl / 3
        mono, now = time.monotonic(), self.clock.now()
        if mono - self._last_renew_mono < interval and now - self._last_renew_clock < interval:
            return
        self._last_renew_mono, self._last_renew_clock = mono, now
        held = self.store.renew(self.identity, until=now + self._lease_ttl)
        for wid in [w for w in self._runtimes if w not in held]:
            _log.warning("lease on %s was taken over", wid)
            self._runtimes.pop(wid, None)

    # -- claiming and recovery -----------------------------------------------------------------

    def _claim(self) -> bool:
        room = self._max_workflows - len(self._runtimes)
        if room <= 0:
            return False
        ids = self.store.claim(
            self.identity,
            now=self.clock.now(),
            ttl=self._lease_ttl,
            types=self.app.workflow_names,
            limit=room,
        )
        for wid in ids:
            try:
                view = HistoryView(self.store.load_events(wid))
                defn = self.app.get_workflow(view.workflow_type)
            except HistoryCorruptError as exc:
                _log.error("history of %s is corrupt: %s", wid, exc)
                self.store.block(wid, self.identity, serde.encode_error(exc), now=self.clock.now())
                continue
            rt = _Runtime(wid, defn, view)
            self._runtimes[wid] = rt
            self.stats.claimed += 1
            _log.info("claimed %s (%d events)", wid, view.last_seq)
            crashpoint("after_workflow_claimed")
            self._guarded(rt, functools.partial(self._recover, rt))
        return bool(ids)

    def _recover(self, rt: _Runtime) -> None:
        """Bring a freshly claimed workflow's in-flight work back under control.

        * Scheduled but never started -> start attempt 1.
        * Started without an outcome -> the previous owner died mid-attempt. For
          ``at_least_once`` activities that attempt is recorded as failed with
          ``WorkerLost`` and retried immediately (it counts toward ``maximum_attempts``
          so an activity that kills its worker cannot crash-loop forever). For
          ``at_most_once`` activities the outcome is unknowable, so the activity fails
          with the non-retryable ``ActivityOutcomeUnknown`` and the workflow decides.
        * Waiting for a retry -> nothing to do; its timer row survived the crash.
        * Open signal waits are matched against signals already in history.
        """
        to_start: list[tuple[int, int]] = []
        for st in rt.view.pending_activities():
            if st.inflight:
                self.stats.activities_recovered += 1
                if st.delivery == "at_most_once":
                    error = serde.make_error(
                        "ActivityOutcomeUnknown",
                        f"worker stopped during attempt {st.attempt} of an at-most-once "
                        "activity; it may or may not have taken effect",
                        non_retryable=True,
                    )
                    self._finish_failed(rt, st, error)
                else:
                    error = serde.make_error(
                        "WorkerLost", f"worker stopped while attempt {st.attempt} was running"
                    )
                    self._fail_attempt(rt, st, error, immediate=True)
            elif st.attempt == 0:
                to_start.append((st.cmd, 1))
        if to_start:
            self._start_attempts(rt, to_start)
        # A crash between recording a wait and matching it to an already-buffered
        # signal leaves both in history with nothing left to trigger the match.
        self._resolve_signals(rt)
        rt.dirty = True

    # -- activities -------------------------------------------------------------------------

    def _start_attempts(
        self,
        rt: _Runtime,
        items: Sequence[tuple[int, int]],
        extra_timers: Sequence[TimerPut | TimerDel] = (),
    ) -> None:
        now = self.clock.now()
        pending = _Pending(timers=list(extra_timers))
        for cmd, attempt in items:
            st = rt.view.activities[cmd]
            pending.events.append(
                PendingEvent(EventType.ACTIVITY_STARTED, cmd, {"attempt": attempt})
            )
            timeout = st.options.get("timeout")
            if timeout:
                pending.timers.append(
                    TimerPut(cmd, "activity_timeout", now + float(timeout), attempt)
                )
        self._append(rt, pending)
        crashpoint("after_activity_started")
        for cmd, attempt in items:
            self._submit(rt, rt.view.activities[cmd], attempt, now)

    def _submit(self, rt: _Runtime, st: ActivityState, attempt: int, now: float) -> None:
        key = (rt.workflow_id, st.cmd, attempt)
        self._inflight.add(key)
        self.stats.activities_started += 1
        try:
            fn = self.app.get_activity(st.name).fn
        except UnknownActivity as exc:
            error = serde.make_error("UnknownActivity", str(exc), non_retryable=True)
            self._outcomes.put(_Outcome(rt.workflow_id, st.cmd, attempt, ok=False, error=error))
            return
        timeout = st.options.get("timeout")
        info = activity_ctx.ActivityInfo(
            workflow_id=rt.workflow_id,
            workflow_type=rt.view.workflow_type,
            activity_name=st.name,
            cmd=st.cmd,
            attempt=attempt,
            idempotency_key=f"{rt.workflow_id}/{st.cmd}",
            scheduled_at=now,
            deadline=None if not timeout else now + float(timeout),
        )
        self._pool.submit(self._execute_activity, info, fn, list(st.input))

    def _execute_activity(
        self, info: activity_ctx.ActivityInfo, fn: Callable[..., Any], args: list[Any]
    ) -> None:
        token = activity_ctx._set(info)
        try:
            value = serde.roundtrip(fn(*args))
            crashpoint("activity_after_execute")
            outcome = _Outcome(info.workflow_id, info.cmd, info.attempt, ok=True, value=value)
        except SimulatedCrash as crash:
            self._outcomes.put(_Crashed(crash))
            return
        except BaseException as exc:  # an activity must never take the worker down
            outcome = _Outcome(
                info.workflow_id, info.cmd, info.attempt, ok=False, error=serde.encode_error(exc)
            )
        finally:
            activity_ctx._reset(token)
        self._outcomes.put(outcome)

    def _drain_outcomes(self, *, timeout: float) -> bool:
        handled = False
        block = timeout > 0
        while True:
            try:
                item = self._outcomes.get(block=block, timeout=timeout if block else None)
            except queue.Empty:
                return handled
            block = False
            if isinstance(item, _Crashed):
                raise item.crash
            self._inflight.discard((item.workflow_id, item.cmd, item.attempt))
            rt = self._runtimes.get(item.workflow_id)
            if rt is not None:
                self._guarded(rt, functools.partial(self._record_outcome, rt, item))
            handled = True

    def _record_outcome(self, rt: _Runtime, out: _Outcome) -> None:
        st = rt.view.activities.get(out.cmd)
        if st is None or st.done or not st.inflight or st.attempt != out.attempt:
            _log.debug("ignoring stale outcome %s#%d/%d", out.workflow_id, out.cmd, out.attempt)
            return
        if out.ok:
            pending = _Pending(
                [
                    PendingEvent(
                        EventType.ACTIVITY_COMPLETED,
                        out.cmd,
                        {"attempt": out.attempt, "result": out.value},
                    )
                ],
                [TimerDel(out.cmd, "activity_timeout")],
            )
            self._append(rt, pending)
            self.stats.activities_completed += 1
            crashpoint("after_activity_completed")
            rt.dirty = True
        else:
            self._fail_attempt(rt, st, out.error or serde.make_error("Error", "unknown"))

    def _fail_attempt(
        self,
        rt: _Runtime,
        st: ActivityState,
        error: dict[str, Any],
        *,
        immediate: bool = False,
    ) -> None:
        policy = RetryPolicy.from_dict(st.options.get("retry", {}))
        if not policy.allows_retry(st.attempt, error):
            self._finish_failed(rt, st, error)
            return
        now = self.clock.now()
        retry_at = now if immediate else now + policy.delay(st.attempt, self._rng.random())
        pending = _Pending(
            [
                PendingEvent(
                    EventType.ACTIVITY_ATTEMPT_FAILED,
                    st.cmd,
                    {"attempt": st.attempt, "error": error, "retry_at": retry_at},
                )
            ],
            [TimerDel(st.cmd, "activity_timeout"), TimerPut(st.cmd, "retry", retry_at, st.attempt)],
        )
        self._append(rt, pending)
        self.stats.activity_attempt_failures += 1
        _log.info(
            "%s: activity %s attempt %d failed (%s); retry at +%.3fs",
            rt.workflow_id,
            st.name,
            st.attempt,
            error.get("type"),
            retry_at - now,
        )

    def _finish_failed(self, rt: _Runtime, st: ActivityState, error: dict[str, Any]) -> None:
        final = {**error, "attempts": st.attempt}
        pending = _Pending(
            [
                PendingEvent(
                    EventType.ACTIVITY_FAILED, st.cmd, {"attempt": st.attempt, "error": final}
                )
            ],
            [TimerDel(st.cmd, "activity_timeout"), TimerDel(st.cmd, "retry")],
        )
        self._append(rt, pending)
        self.stats.activities_failed += 1
        rt.dirty = True

    # -- timers -------------------------------------------------------------------------------

    def _fire_due_timers(self) -> bool:
        rows = self.store.due_timers(self.identity, self.clock.now())
        fired = False
        for row in rows:
            rt = self._runtimes.get(row.workflow_id)
            if rt is None:
                continue
            fired = True
            self._guarded(rt, functools.partial(self._fire, rt, row))
        return fired

    def _fire(self, rt: _Runtime, row: TimerRow) -> None:
        view = rt.view
        cleanup = _Pending(timers=[TimerDel(row.cmd, row.kind)])
        if row.kind == "timer":
            timer = view.timers.get(row.cmd)
            if timer is not None and not timer.fired:
                cleanup.events.append(PendingEvent(EventType.TIMER_FIRED, row.cmd, {}))
                self._append(rt, cleanup)
                self.stats.timers_fired += 1
                crashpoint("after_timer_fired")
                rt.dirty = True
                return
        elif row.kind == "retry":
            st = view.activities.get(row.cmd)
            if st is not None and not st.done and not st.inflight and st.attempt == row.attempt:
                self._start_attempts(rt, [(row.cmd, st.attempt + 1)], cleanup.timers)
                return
        elif row.kind == "activity_timeout":
            st = view.activities.get(row.cmd)
            if st is not None and st.inflight and st.attempt == row.attempt:
                self._inflight.discard((rt.workflow_id, row.cmd, row.attempt))
                timeout = st.options.get("timeout")
                error = serde.make_error(
                    "ActivityTimeout", f"attempt {st.attempt} exceeded its {timeout}s timeout"
                )
                self._fail_attempt(rt, st, error)
                return
        elif row.kind == "signal_timeout":
            wait = view.signal_waits.get(row.cmd)
            if wait is not None and not wait.resolved:
                cleanup.events.append(PendingEvent(EventType.SIGNAL_WAIT_TIMED_OUT, row.cmd, {}))
                self._append(rt, cleanup)
                rt.dirty = True
                return
        self._append(rt, cleanup)  # stale index row: just delete it

    # -- inbox and signals -----------------------------------------------------------------------

    def _process_inbox(self) -> bool:
        items = self.store.inbox_for_owner(self.identity)
        for item in items:
            rt = self._runtimes.get(item.workflow_id)
            if rt is None:
                continue
            handler = self._deliver_signal if item.kind == "signal" else self._deliver_cancel
            self._guarded(rt, functools.partial(handler, rt, item))
        return bool(items)

    def _deliver_signal(self, rt: _Runtime, item: InboxItem) -> None:
        event = PendingEvent(
            EventType.SIGNAL_RECEIVED, None, {"name": item.name, "payload": item.payload}
        )
        self._append(rt, _Pending([event]), consume_inbox=[item.id])
        self.stats.signals_received += 1
        crashpoint("after_signal_received")
        self._resolve_signals(rt)

    def _deliver_cancel(self, rt: _Runtime, item: InboxItem) -> None:
        """Cancellation is a hard stop: in-flight activities are abandoned."""
        pending = _Pending(
            [
                PendingEvent(EventType.CANCEL_REQUESTED, None, {"reason": item.payload}),
                PendingEvent(EventType.WORKFLOW_CANCELLED, None, {}),
            ]
        )
        self._append(rt, pending, status=WorkflowStatus.CANCELLED, consume_inbox=[item.id])

    def _resolve_signals(self, rt: _Runtime) -> None:
        """Match open ``wait_signal`` commands (in command order) to buffered signals."""
        view = rt.view
        waits = view.open_signal_waits()
        if not waits or not view.mailbox:
            return
        available = list(view.mailbox)
        pending = _Pending()
        for wait in waits:
            for i, sig in enumerate(available):
                if sig.data.get("name") == wait.name:
                    pending.events.append(
                        PendingEvent(
                            EventType.SIGNAL_CONSUMED,
                            wait.cmd,
                            {
                                "signal_seq": sig.seq,
                                "name": wait.name,
                                "payload": sig.data.get("payload"),
                            },
                        )
                    )
                    if wait.deadline is not None:
                        pending.timers.append(TimerDel(wait.cmd, "signal_timeout"))
                    del available[i]
                    break
        if pending.events:
            self._append(rt, pending)
            crashpoint("after_signal_consumed")
            rt.dirty = True

    # -- workflow tasks --------------------------------------------------------------------------

    def _run_dirty_tasks(self) -> bool:
        ran = False
        for rt in list(self._runtimes.values()):
            if rt.dirty and rt.workflow_id in self._runtimes:
                ran = True
                self._guarded(rt, functools.partial(self._run_task, rt))
        return ran

    def _run_task(self, rt: _Runtime) -> None:
        rt.dirty = False
        view = rt.view
        self.stats.workflow_tasks += 1
        try:
            res = execute_task(
                rt.defn, workflow_id=rt.workflow_id, view=view, app=self.app, now=self.clock.now
            )
        except WorkflowTaskError as err:
            _log.error("blocking workflow %s: %s", rt.workflow_id, err)
            self.store.block(
                rt.workflow_id,
                self.identity,
                serde.encode_error(err, include_traceback=False),
                now=self.clock.now(),
            )
            self._runtimes.pop(rt.workflow_id, None)
            self.stats.workflows_blocked += 1
            return
        self.stats.replayed_commands += res.replayed_commands
        now = self.clock.now()
        pending = _Pending()
        to_start: list[tuple[int, int]] = []
        for nc in res.commands:
            data = dict(nc.data)
            if nc.type is EventType.TIMER_STARTED:
                data["fire_at"] = now + float(data["duration"])
                pending.timers.append(TimerPut(nc.cmd, "timer", data["fire_at"]))
            elif nc.type is EventType.SIGNAL_WAIT_STARTED:
                timeout = data.get("timeout")
                data["deadline"] = None if timeout is None else now + float(timeout)
                if data["deadline"] is not None:
                    pending.timers.append(TimerPut(nc.cmd, "signal_timeout", data["deadline"]))
            elif nc.type is EventType.ACTIVITY_SCHEDULED:
                to_start.append((nc.cmd, 1))
            pending.events.append(PendingEvent(nc.type, nc.cmd, data))
        status: WorkflowStatus | None = None
        if res.status == "completed":
            status = WorkflowStatus.COMPLETED
            pending.events.append(
                PendingEvent(EventType.WORKFLOW_COMPLETED, None, {"result": res.result})
            )
        elif res.status == "failed":
            status = WorkflowStatus.FAILED
            pending.events.append(
                PendingEvent(EventType.WORKFLOW_FAILED, None, {"error": res.error})
            )
        if not pending.events:
            return
        self._append(rt, pending, status=status, result=res.result, error=res.error)
        crashpoint("after_commands_appended")
        if status is not None:
            return
        if to_start:
            self._start_attempts(rt, to_start)
        self._resolve_signals(rt)
