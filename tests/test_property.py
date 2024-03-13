"""Property test: random workflow programs survive random crash sequences.

Hypothesis generates a small program (activities, parallel fan-out, flaky activities
that need retries, durable sleeps, signal waits) and up to three crashes at arbitrary
crash points. The worker is killed in-process (``SimulatedCrash``) and replaced by a new
worker each time. Whatever the crash sequence, the workflow must produce the same
result as the pure interpretation of the program and every idempotent effect must be
recorded exactly once.
"""

from __future__ import annotations

import tempfile
import threading
import uuid
from pathlib import Path
from typing import Any

from hypothesis import event, given, settings
from hypothesis import strategies as st

from replayagent import (
    App,
    Client,
    FakeClock,
    RetryPolicy,
    Store,
    Worker,
    WorkflowContext,
    WorkflowStatus,
    activity,
    activity_info,
    faults,
    workflow,
)
from replayagent.events import EventType

_lock = threading.Lock()
effects: dict[str, int] = {}
flaky_seen: set[str] = set()


@activity(retry=RetryPolicy(initial_interval=0.5, maximum_attempts=50))
def effect(k: int) -> int:
    with _lock:
        effects.setdefault(activity_info().idempotency_key, k)
    return k * 2


@activity(retry=RetryPolicy(initial_interval=1.0, backoff_coefficient=3, maximum_attempts=50))
def flaky(k: int) -> int:
    key = activity_info().idempotency_key
    with _lock:
        first = key not in flaky_seen
        flaky_seen.add(key)
    if first:
        raise ConnectionError("first attempt always fails")
    return k * 3


@workflow
def interpreter(ctx: WorkflowContext, program: list[list[Any]]) -> dict[str, Any]:
    out: list[Any] = []
    sides: list[Any] = []
    for op in program:
        kind = op[0]
        if kind == "act":
            out.append(ctx.activity(effect, op[1]))
        elif kind == "par":
            out.append(ctx.gather(*[ctx.start_activity(effect, k) for k in op[1]]))
        elif kind == "flaky":
            out.append(ctx.activity(flaky, op[1]))
        elif kind == "sleep":
            ctx.sleep(op[1])
            out.append(None)
        elif kind == "signal":
            out.append(ctx.wait_signal(op[1]))
        elif kind == "side":
            sides.append(ctx.side_effect(lambda: ctx.random().random()))
            out.append("side")
    return {"out": out, "sides": sides}


APP = App(workflows=[interpreter], activities=[effect, flaky])

op = st.one_of(
    st.tuples(st.just("act"), st.integers(0, 99)),
    st.tuples(st.just("par"), st.lists(st.integers(0, 99), min_size=1, max_size=4)),
    st.tuples(st.just("flaky"), st.integers(0, 99)),
    st.tuples(st.just("sleep"), st.sampled_from([0, 1, 30])),
    st.tuples(st.just("signal"), st.sampled_from(["a", "b"])),
    st.tuples(st.just("side")),
)
crash = st.tuples(st.sampled_from(faults.CRASH_POINTS), st.integers(1, 4))


def expected(program: list[list[Any]], payloads: dict[str, list[str]]) -> list[Any]:
    out: list[Any] = []
    taken: dict[str, int] = {}
    for item in program:
        kind = item[0]
        if kind == "act":
            out.append(item[1] * 2)
        elif kind == "par":
            out.append([k * 2 for k in item[1]])
        elif kind == "flaky":
            out.append(item[1] * 3)
        elif kind == "sleep":
            out.append(None)
        elif kind == "signal":
            i = taken.get(item[1], 0)
            taken[item[1]] = i + 1
            out.append(payloads[item[1]][i])
        else:
            out.append("side")
    return out


@settings(max_examples=150, deadline=None)
@given(program=st.lists(op, min_size=1, max_size=8), crashes=st.lists(crash, max_size=3))
def test_random_programs_survive_random_crashes(
    program: list[tuple[Any, ...]], crashes: list[tuple[str, int]]
) -> None:
    effects.clear()
    flaky_seen.clear()
    prog = [list(p) for p in program]
    payloads: dict[str, list[str]] = {"a": [], "b": []}
    for item in prog:
        if item[0] == "signal":
            payloads[item[1]].append(f"{item[1]}-{len(payloads[item[1]])}")
    with tempfile.TemporaryDirectory() as tmp:
        store = Store(Path(tmp) / "p.db", synchronous="NORMAL")
        clock = FakeClock()
        client = Client(store, APP, clock=clock)
        # Unique id: threads of a "crashed" worker may still finish after the crash.
        wid = client.start(interpreter, prog, workflow_id=f"prop-{uuid.uuid4().hex[:8]}")
        for name, values in payloads.items():
            for value in values:
                client.signal(wid, name, value)
        plans = [faults.CrashPlan.parse(f"{p}@{n}", mode="raise") for p, n in crashes]
        fired = 0
        try:
            for _ in range(len(plans) + 2):
                faults.install(plans.pop(0) if plans else None)
                worker = Worker(store, APP, clock=clock, lease_ttl=5, poll_interval=0.002)
                try:
                    worker.run_until_idle(max_timer_wait=float("inf"))
                except faults.SimulatedCrash:
                    fired += 1
                    clock.advance(6)  # the dead worker's lease expires
                    continue
                worker.close()
                if client.get(wid).status is not WorkflowStatus.RUNNING:
                    break
        finally:
            faults.install(None)
        event(f"crashes survived: {fired}")
        assert client.get(wid).status is WorkflowStatus.COMPLETED
        result = client.result(wid)
        events = store.load_events(wid)
        store.close()
    assert result["out"] == expected(prog, payloads)
    recorded_sides = [e.data["value"] for e in events if e.type is EventType.SIDE_EFFECT]
    assert result["sides"] == recorded_sides
    effect_keys = {
        f"{wid}/{e.cmd}"
        for e in events
        if e.type is EventType.ACTIVITY_SCHEDULED and e.data["name"] == "effect"
    }
    with _lock:
        recorded = {k for k in effects if k.startswith(f"{wid}/")}
    assert recorded == effect_keys  # every scheduled effect happened, keyed exactly once
