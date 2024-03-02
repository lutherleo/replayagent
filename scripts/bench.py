"""Micro-benchmarks (``make bench``). Prints markdown tables; numbers are machine-specific.

1. Throughput: workflows of 10 sequential no-op activities, SQLite synchronous=FULL vs
   NORMAL (WAL). Every activity costs 4 events in 3 transactions.
2. Replay cost: one full replay (re-running the workflow function against its complete
   history) as history grows. A workflow task replays from the top, so this is the
   per-step overhead, and the total work for an n-step workflow is O(n^2).
3. Recovery: time for a fresh worker to claim an orphaned workflow, fold its history and
   replay it to the first new command.
"""

from __future__ import annotations

import statistics
import tempfile
import time
from pathlib import Path
from typing import Any

from replayagent import (
    App,
    Client,
    Store,
    SystemClock,
    Worker,
    WorkflowContext,
    activity,
    faults,
    workflow,
)
from replayagent.history import HistoryView
from replayagent.replay import execute_task


@activity
def noop(i: int) -> int:
    return i


@workflow
def steps(ctx: WorkflowContext, n: int) -> int:
    total = 0
    for i in range(n):
        total += ctx.activity(noop, i)
    return total


APP = App(workflows=[steps], activities=[noop])


def throughput(sync: str, workflows: int = 40, n: int = 10) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as tmp:
        store = Store(Path(tmp) / "b.db", synchronous=sync)  # type: ignore[arg-type]
        client = Client(store, APP)
        ids = [client.start(steps, n) for _ in range(workflows)]
        t0 = time.perf_counter()
        with Worker(store, APP, clock=SystemClock(), poll_interval=0.001) as worker:
            worker.run_until_idle(timeout=600)
        elapsed = time.perf_counter() - t0
        events = sum(store.get_workflow(i).history_len for i in ids)
        assert all(client.result(i) == sum(range(n)) for i in ids)
        store.close()
    return {
        "sync": sync,
        "workflows": workflows,
        "activities": workflows * n,
        "seconds": elapsed,
        "activities_per_s": workflows * n / elapsed,
        "events_per_s": events / elapsed,
    }


def replay_cost(n: int, repeats: int = 5) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as tmp:
        store = Store(Path(tmp) / "r.db", synchronous="NORMAL")
        client = Client(store, APP)
        wid = client.start(steps, n)
        t0 = time.perf_counter()
        with Worker(store, APP, clock=SystemClock(), poll_interval=0.001) as worker:
            worker.run_until_idle(timeout=600)
            tasks = worker.stats.workflow_tasks
            replayed = worker.stats.replayed_commands
        run_s = time.perf_counter() - t0
        events = store.load_events(wid)
        store.close()
    defn = APP.get_workflow("steps")
    samples = []
    for _ in range(repeats):
        t = time.perf_counter()
        view = HistoryView(events)
        res = execute_task(defn, workflow_id=wid, view=view, app=APP, now=time.time)
        samples.append(time.perf_counter() - t)
        assert res.status == "completed"
    return {
        "n": n,
        "events": len(events),
        "run_s": run_s,
        "tasks": tasks,
        "replayed_commands": replayed,
        "full_replay_ms": statistics.median(samples) * 1000,
    }


def recovery(n: int) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as tmp:
        store = Store(Path(tmp) / "c.db", synchronous="NORMAL")
        client = Client(store, APP)
        wid = client.start(steps, n + 1)
        faults.install(faults.CrashPlan.parse(f"after_activity_completed@{n}", mode="raise"))
        dead = Worker(store, APP, clock=SystemClock(), poll_interval=0.001)
        try:
            dead.run_until_idle(timeout=600)
        except faults.SimulatedCrash:
            pass
        finally:
            faults.install(None)
        dead_owner = dead.identity
        store.release(dead_owner)  # what lease expiry / dead-pid detection would do
        t0 = time.perf_counter()
        with Worker(store, APP, clock=SystemClock(), poll_interval=0.001) as fresh:
            fresh.tick()  # claim + fold + recover + replay + schedule the next step
            recovered_ms = (time.perf_counter() - t0) * 1000
            fresh.run_until_complete(wid, timeout=600)
        assert client.result(wid) == sum(range(n + 1))
        store.close()
    return {"history_commands": n, "recovery_ms": recovered_ms}


def main() -> None:
    print("## 1. Throughput (40 workflows x 10 sequential activities, 1 worker)\n")
    print("| synchronous | activities | seconds | activities/s | events/s |")
    print("|---|---:|---:|---:|---:|")
    for sync in ("FULL", "NORMAL"):
        r = throughput(sync)
        print(
            f"| {r['sync']} | {r['activities']} | {r['seconds']:.2f} | "
            f"{r['activities_per_s']:.0f} | {r['events_per_s']:.0f} |"
        )
    print("\n## 2. Replay cost vs. history length (one workflow, n sequential activities)\n")
    print(
        "| n | events | end-to-end run s | workflow tasks | commands replayed in total "
        "| one full replay ms |"
    )
    print("|---:|---:|---:|---:|---:|---:|")
    for n in (10, 100, 500, 1000):
        r = replay_cost(n)
        print(
            f"| {r['n']} | {r['events']} | {r['run_s']:.2f} | {r['tasks']} | "
            f"{r['replayed_commands']} | {r['full_replay_ms']:.2f} |"
        )
    print("\n## 3. Recovery after a crash (fresh worker's first tick)\n")
    print("| completed commands in history | claim+fold+replay+schedule ms |")
    print("|---:|---:|")
    for n in (100, 1000):
        r = recovery(n)
        print(f"| {r['history_commands']} | {r['recovery_ms']:.1f} |")


if __name__ == "__main__":
    main()
