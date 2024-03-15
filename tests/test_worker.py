"""End-to-end engine behaviour on a virtual clock."""

from __future__ import annotations

import threading
import time
from collections import Counter
from collections.abc import Callable
from typing import Any

import pytest

from replayagent import (
    NO_RETRY,
    ActivityError,
    App,
    ApplicationError,
    Client,
    RetryPolicy,
    SignalTimeout,
    Store,
    SystemClock,
    Worker,
    WorkflowContext,
    WorkflowStatus,
    activity,
    activity_info,
    workflow,
)
from replayagent.errors import (
    WorkflowBlockedError,
    WorkflowCancelledError,
    WorkflowFailedError,
    WorkflowNotRunning,
)
from replayagent.events import EventType
from replayagent.faults import CRASH_POINTS, CrashPlan, SimulatedCrash, install
from tests.conftest import Env

EnvFactory = Callable[..., Env]

calls: Counter[str] = Counter()
_lock = threading.Lock()
flaky_failures: dict[str, int] = {}


@pytest.fixture(autouse=True)
def _reset() -> None:
    calls.clear()
    flaky_failures.clear()


def count(name: str) -> None:
    with _lock:
        calls[name] += 1


@activity(retry=RetryPolicy(initial_interval=1, backoff_coefficient=2, maximum_attempts=4))
def flaky(key: str) -> str:
    count(key)
    left = flaky_failures.get(key, 0)
    if left:
        flaky_failures[key] = left - 1
        raise ConnectionError(f"{key} transient")
    return f"{key}:ok"


@activity
def double(x: int) -> int:
    count("double")
    return x * 2


@activity(retry=RetryPolicy(maximum_attempts=5))
def fatal() -> None:
    count("fatal")
    raise ApplicationError("bad input", non_retryable=True)


@activity(retry=RetryPolicy(initial_interval=0.5, maximum_attempts=2))
def always_fails() -> None:
    count("always")
    raise RuntimeError("down")


@activity
def whoami() -> dict[str, Any]:
    info = activity_info()
    return {"key": info.idempotency_key, "attempt": info.attempt, "name": info.activity_name}


@workflow
def pipeline(ctx: WorkflowContext, n: int) -> dict[str, Any]:
    first = ctx.activity(double, n)
    fanned = ctx.gather(*[ctx.start_activity(double, first + i) for i in range(3)])
    ctx.sleep(60)
    return {"first": first, "fanned": fanned, "me": ctx.activity(whoami)}


@workflow
def retrying(ctx: WorkflowContext, key: str) -> str:
    return str(ctx.activity(flaky, key))


@workflow
def handles_failure(ctx: WorkflowContext, _: Any) -> dict[str, Any]:
    out = {}
    for act in (fatal, always_fails):
        try:
            ctx.activity(act)
        except ActivityError as exc:
            out[exc.name] = [exc.error_type, exc.attempts]
    return out


@workflow
def unhandled(ctx: WorkflowContext, _: Any) -> None:
    ctx.activity(always_fails)


@workflow
def approval(ctx: WorkflowContext, timeout: float | None) -> Any:
    try:
        first = ctx.wait_signal("go", timeout=timeout)
    except SignalTimeout:
        return "timed out"
    second = ctx.wait_signal("go")
    return [first, second]


APP = App(
    workflows=[pipeline, retrying, handles_failure, unhandled, approval],
    activities=[flaky, double, fatal, always_fails, whoami],
)


def test_pipeline_runs_to_completion(make_env: EnvFactory) -> None:
    env = make_env(APP)
    wid = env.client.start(pipeline, 5, workflow_id="p1")
    start = env.clock.now()
    worker = env.run()
    assert env.client.result(wid) == {
        "first": 10,
        "fanned": [20, 22, 24],
        "me": {"key": "p1/5", "attempt": 1, "name": "whoami"},
    }
    assert env.clock.now() - start == pytest.approx(60)
    assert calls["double"] == 4
    assert worker.stats.workflows_completed == 1
    assert worker.stats.timers_fired == 1


def test_retries_follow_backoff_schedule(make_env: EnvFactory) -> None:
    env = make_env(APP)
    flaky_failures["k"] = 3
    wid = env.client.start(retrying, "k")
    env.run()
    assert env.client.result(wid) == "k:ok"
    events = env.client.history(wid)
    gaps = [
        round(e.data["retry_at"] - e.ts, 6)
        for e in events
        if e.type is EventType.ACTIVITY_ATTEMPT_FAILED
    ]
    assert gaps == [1, 2, 4]
    starts = [e.data["attempt"] for e in events if e.type is EventType.ACTIVITY_STARTED]
    assert starts == [1, 2, 3, 4]
    assert calls["k"] == 4


def test_failures_surface_as_activity_errors(make_env: EnvFactory) -> None:
    env = make_env(APP)
    wid = env.client.start(handles_failure, None)
    env.run()
    assert env.client.result(wid) == {
        "fatal": ["ApplicationError", 1],
        "always_fails": ["RuntimeError", 2],
    }
    assert calls["fatal"] == 1  # non_retryable short-circuits the 5-attempt policy


def test_uncaught_activity_error_fails_workflow(make_env: EnvFactory) -> None:
    env = make_env(APP)
    wid = env.client.start(unhandled, None)
    env.run()
    with pytest.raises(WorkflowFailedError, match="ActivityError"):
        env.client.result(wid)
    assert env.client.get(wid).status is WorkflowStatus.FAILED


def test_signals_are_buffered_and_consumed_in_order(make_env: EnvFactory) -> None:
    env = make_env(APP)
    wid = env.client.start(approval, None)
    env.client.signal(wid, "go", 1)  # before the workflow even ran
    env.run()
    assert env.client.get(wid).status is WorkflowStatus.RUNNING
    env.client.signal(wid, "other", "ignored")
    env.client.signal(wid, "go", 2)
    env.run()
    assert env.client.result(wid) == [1, 2]
    with pytest.raises(WorkflowNotRunning):
        env.client.signal(wid, "go", 3)


def test_signal_timeout_and_parking(make_env: EnvFactory) -> None:
    env = make_env(APP)
    wid = env.client.start(approval, 300)
    env.run(max_timer_wait=10)  # the 300s deadline is out of reach: worker idles
    assert env.client.get(wid).status is WorkflowStatus.RUNNING
    env.run()  # follow timers
    assert env.client.result(wid) == "timed out"


def test_cancel_is_a_hard_stop(make_env: EnvFactory) -> None:
    env = make_env(APP)
    wid = env.client.start(approval, None)
    env.run()
    env.client.cancel(wid, "user abort")
    env.run()
    with pytest.raises(WorkflowCancelledError):
        env.client.result(wid)
    types = [e.type for e in env.client.history(wid)][-2:]
    assert types == [EventType.CANCEL_REQUESTED, EventType.WORKFLOW_CANCELLED]


def test_activity_timeout_triggers_retry(tmp_path: Any) -> None:
    attempts: list[int] = []

    @activity(timeout=0.05, retry=RetryPolicy(initial_interval=0.01, maximum_attempts=3))
    def slow() -> str:
        attempts.append(activity_info().attempt)
        if len(attempts) == 1:
            time.sleep(0.4)
        return "done"

    @workflow
    def wf(ctx: WorkflowContext, _: Any) -> str:
        return str(ctx.activity(slow))

    app = App(workflows=[wf], activities=[slow])
    store = Store(tmp_path / "t.db")
    client = Client(store, app)
    wid = client.start(wf)
    with Worker(store, app, clock=SystemClock(), poll_interval=0.005) as worker:
        assert worker.run_until_complete(wid, timeout=10) is WorkflowStatus.COMPLETED
    assert client.result(wid) == "done"
    errors = [
        e.data["error"]["type"]
        for e in client.history(wid)
        if e.type is EventType.ACTIVITY_ATTEMPT_FAILED
    ]
    assert errors == ["ActivityTimeout"]
    store.close()


def test_code_change_blocks_then_fix_resumes(make_env: EnvFactory) -> None:
    @workflow(name="evolving")
    def v1(ctx: WorkflowContext, _: Any) -> list[int]:
        a = ctx.activity(double, 1)
        ctx.wait_signal("next")
        return [a, ctx.activity(double, 2)]

    @workflow(name="evolving")
    def v2_broken(ctx: WorkflowContext, _: Any) -> list[int]:
        a = ctx.activity(double, 100)  # changed argument, no version guard
        ctx.wait_signal("next")
        return [a, ctx.activity(double, 2)]

    def app_with(fn: Callable[..., Any]) -> App:
        return App(workflows=[fn], activities=[double])

    env = make_env(app_with(v1))
    wid = env.client.start("evolving", None)
    env.run()
    env.client.signal(wid, "next")
    env.run(app_with(v2_broken))
    rec = env.client.get(wid)
    assert rec.status is WorkflowStatus.BLOCKED
    assert rec.error and rec.error["type"] == "NonDeterminismError"
    assert "activity input differs" in rec.error["message"]
    with pytest.raises(WorkflowBlockedError):
        env.client.result(wid)
    assert env.client.history(wid)[-1].type is EventType.SIGNAL_CONSUMED  # no failure recorded
    assert env.client.resume(wid)
    env.run(app_with(v1))  # roll back the bad deploy
    assert env.client.result(wid) == [2, 4]


def test_unknown_activity_blocks_workflow(make_env: EnvFactory) -> None:
    @workflow
    def calls_missing(ctx: WorkflowContext, _: Any) -> Any:
        return ctx.activity("missing")

    env = make_env(App(workflows=[calls_missing]))
    wid = env.client.start(calls_missing)
    worker = env.run()
    assert env.client.get(wid).status is WorkflowStatus.BLOCKED
    assert worker.stats.workflows_blocked == 1


def test_activity_unregistered_at_dispatch_fails_cleanly(make_env: EnvFactory) -> None:
    env = make_env(APP)
    wid = env.client.start(pipeline, 1)
    worker = env.worker()
    worker.tick()  # schedules and runs double(1)
    worker.close()
    # A worker whose app lost the activity but kept the workflow: replay blocks it.
    env.run(App(workflows=[pipeline]))
    assert env.client.get(wid).status is WorkflowStatus.BLOCKED


def test_worker_close_releases_leases_and_rejects_use(make_env: EnvFactory) -> None:
    env = make_env(APP)
    wid = env.client.start(approval, None)
    worker = env.worker()
    worker.run_until_idle()
    assert worker.owned_workflows == [wid]
    assert env.client.get(wid).owner == worker.identity
    worker.close()
    worker.close()  # idempotent
    assert env.client.get(wid).owner is None
    with pytest.raises(RuntimeError):
        worker.tick()


def test_second_worker_takes_over_expired_lease(make_env: EnvFactory) -> None:
    env = make_env(APP)
    wid = env.client.start(approval, None)
    a = env.worker(lease_ttl=5)
    a.run_until_idle()
    b = env.worker(lease_ttl=5)
    b.run_until_idle()
    assert b.owned_workflows == []  # a's lease is live
    env.clock.advance(6)
    b.run_until_idle()
    assert b.owned_workflows == [wid]
    env.client.signal(wid, "go", "x")
    env.client.signal(wid, "go", "y")
    a.tick()  # a renews, learns it lost the lease, drops the workflow
    assert a.owned_workflows == []
    b.run_until_idle()
    assert env.client.result(wid) == ["x", "y"]
    a.close()
    b.close()


def test_run_until_complete_and_forever(make_env: EnvFactory) -> None:
    env = make_env(APP)
    wid = env.client.start(pipeline, 2)
    with env.worker() as worker:
        assert worker.run_until_complete(wid) is WorkflowStatus.COMPLETED
        wid2 = env.client.start(approval, None)
        stop = threading.Event()
        thread = threading.Thread(target=worker.run_forever, args=(stop,))
        thread.start()
        env.client.signal(wid2, "go", 1)
        env.client.signal(wid2, "go", 2)
        assert env.client.result(wid2, timeout=10) == [1, 2]
        stop.set()
        thread.join(timeout=10)
    env.client.start(approval, None, workflow_id="stuck")
    with env.worker() as idle, pytest.raises(TimeoutError):
        idle.run_until_complete("stuck", timeout=0.05)
    with pytest.raises(TimeoutError):
        env.client.result("stuck", timeout=0.01)


def test_invalid_worker_settings(make_env: EnvFactory) -> None:
    env = make_env(APP)
    with pytest.raises(ValueError):
        Worker(env.store, APP, lease_ttl=0)


def test_client_requires_app_for_function_starts(make_env: EnvFactory) -> None:
    env = make_env(APP)
    with pytest.raises(ValueError):
        Client(env.store).start(pipeline)
    wid = Client(env.store).start("pipeline", 1)
    assert env.client.get(wid).code_hash is None


# -- in-process crash simulation ---------------------------------------------------------------

ledger: dict[str, int] = {}


@activity(retry=RetryPolicy(initial_interval=0.1, maximum_attempts=10))
def charge(amount: int) -> str:
    key = activity_info().idempotency_key
    with _lock:
        calls["charge-executions"] += 1
        ledger.setdefault(key, amount)  # idempotent write keyed by the activity
    return key


@activity(delivery="at_most_once", retry=NO_RETRY)
def email(to: str) -> str:
    count("email")
    return f"sent to {to}"


@workflow
def checkout(ctx: WorkflowContext, items: list[int]) -> dict[str, Any]:
    receipts = ctx.gather(*[ctx.start_activity(charge, i) for i in items])
    ctx.sleep(30)
    approval = ctx.wait_signal("ship")
    total = ctx.activity(charge, sum(items))
    return {"receipts": receipts, "total": total, "approval": approval, "id": ctx.uuid4()}


CRASH_APP = App(workflows=[checkout], activities=[charge, email])


def drive_with_crashes(env: Env, wid: str, *, max_crashes: int = 10) -> int:
    crashes = 0
    while env.client.get(wid).status is WorkflowStatus.RUNNING:
        worker = env.worker(lease_ttl=5)
        try:
            worker.run_until_idle(max_timer_wait=float("inf"))
        except SimulatedCrash:
            crashes += 1
            assert crashes <= max_crashes
            env.clock.advance(6)  # let the dead worker's lease expire
            continue
        worker.close()
        if env.client.get(wid).status is WorkflowStatus.RUNNING:
            env.client.signal(wid, "ship", "ok")
    return crashes


@pytest.mark.parametrize("point", CRASH_POINTS)
@pytest.mark.parametrize("nth", [1, 2])
def test_in_process_crash_at_every_point_is_recovered(
    make_env: EnvFactory, point: str, nth: int
) -> None:
    ledger.clear()
    env = make_env(CRASH_APP)
    wid = env.client.start(checkout, [1, 2, 3], workflow_id="order-1")
    install(CrashPlan.parse(f"{point}@{nth}", mode="raise"))
    drive_with_crashes(env, wid)
    result = env.client.result(wid)
    assert result["receipts"] == ["order-1/0", "order-1/1", "order-1/2"]
    assert ledger == {"order-1/0": 1, "order-1/1": 2, "order-1/2": 3, "order-1/5": 6}
    # At-least-once: attempts in flight when the worker died (up to the 3 parallel
    # charges) may run again - but only as a new, recorded attempt. A completed attempt
    # is never re-executed and the idempotency key keeps the ledger exact.
    started = [e for e in env.client.history(wid) if e.type is EventType.ACTIVITY_STARTED]
    assert calls["charge-executions"] <= len(started) <= 4 + 3
    if point in ("after_timer_fired", "after_signal_received", "after_signal_consumed"):
        assert calls["charge-executions"] == 4  # nothing was in flight at the crash


def test_at_most_once_activity_reports_unknown_outcome(make_env: EnvFactory) -> None:
    @workflow
    def notify(ctx: WorkflowContext, to: str) -> str:
        try:
            return str(ctx.activity(email, to))
        except ActivityError as exc:
            return exc.error_type

    app = App(workflows=[notify], activities=[email])
    env = make_env(app)
    wid = env.client.start(notify, "ops@example.test")
    install(CrashPlan.parse("activity_after_execute", mode="raise"))
    worker = env.worker(lease_ttl=5)
    with pytest.raises(SimulatedCrash):
        worker.run_until_idle()
    env.clock.advance(6)
    recovered = env.run()
    assert env.client.result(wid) == "ActivityOutcomeUnknown"
    assert calls["email"] == 1  # never re-run
    assert recovered.stats.activities_recovered == 1
