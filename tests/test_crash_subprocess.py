"""Real crash tests: the worker process dies (``os._exit`` at a crash point, or SIGKILL
from the parent at a random moment) and a fresh process resumes the workflow.

Invariants checked after every recovery:

* the workflow result is exactly what an uninterrupted run returns;
* every idempotent side effect (a ``charges`` row) exists exactly once;
* every execution of an activity corresponds to a *recorded* attempt, i.e. work whose
  completion reached history is never executed again;
* the model is called no more often than ``llm.chat`` attempts were recorded;
* the stored history replays cleanly against the code.
"""

from __future__ import annotations

import contextlib
import json
import os
import random
import signal
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import pytest

from replayagent import Store
from replayagent.events import Event, EventType
from replayagent.faults import CRASH_EXIT_CODE
from replayagent.inspector import replay_history
from tests.crash_harness import WORKFLOW_ID, build_app

HARNESS = Path(__file__).with_name("crash_harness.py")
SINGLE_HIT = {"after_workflow_claimed", "after_timer_fired", "after_signal_received"}
MULTI_HIT = [
    "after_commands_appended",
    "after_activity_started",
    "activity_after_execute",
    "after_activity_completed",
]
PLANS = [f"{p}@1" for p in sorted(SINGLE_HIT | {"after_signal_consumed"})] + [
    f"{p}@{n}" for p in MULTI_HIT for n in (1, 3, 6)
]


def harness(
    workdir: Path, *extra: str, crash: str | None = None
) -> subprocess.CompletedProcess[str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("REPLAYAGENT_")}
    if crash:
        env["REPLAYAGENT_CRASH"] = crash
    return subprocess.run(
        [sys.executable, "-u", str(HARNESS), str(workdir), *extra],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
        check=False,
    )


def load_history(workdir: Path) -> list[Event]:
    store = Store(workdir / "history.db")
    try:
        return store.load_events(WORKFLOW_ID)
    finally:
        store.close()


def ledger(workdir: Path) -> tuple[dict[str, int], Counter[str], int]:
    with contextlib.closing(sqlite3.connect(workdir / "ledger.sqlite")) as db:
        charges = dict(db.execute("SELECT key, amount FROM charges").fetchall())
        executions = Counter(k for (k,) in db.execute("SELECT key FROM executions"))
        counter = int(db.execute("SELECT n FROM counter").fetchone()[0])
    return charges, executions, counter


def llm_calls(workdir: Path) -> int:
    lines = (workdir / "llm.jsonl").read_text().splitlines()
    return sum(1 for line in lines if not json.loads(line)["failed"])


def check_invariants(workdir: Path, result: dict[str, Any], steps: int) -> dict[str, Any]:
    events = load_history(workdir)
    names = {e.cmd: e.data["name"] for e in events if e.type is EventType.ACTIVITY_SCHEDULED}
    starts = Counter(e.cmd for e in events if e.type is EventType.ACTIVITY_STARTED)
    amounts = [1, 2, 3, *[10 + i for i in range(steps)]]
    expected_keys = [f"{WORKFLOW_ID}/{cmd}" for cmd in range(1, 4 + steps)]
    assert result["receipts"] == expected_keys
    assert result["approval"] == {"by": "reviewer"}
    assert result["plan"] == "ok: plan the order"

    charges, executions, counter = ledger(workdir)
    assert charges == dict(zip(expected_keys, amounts, strict=True)), "duplicate or lost charge"
    for key, n in executions.items():
        cmd = int(key.rsplit("/", 1)[1])
        assert n <= starts[cmd], f"{key} executed {n}x but only {starts[cmd]} attempt(s) recorded"

    llm_starts = sum(n for cmd, n in starts.items() if names[cmd] == "llm.chat")
    calls = llm_calls(workdir)
    assert 2 <= calls <= llm_starts <= 3, (calls, llm_starts)
    bump_cmd = next(cmd for cmd, name in names.items() if name == "bump_counter")
    assert 1 <= counter <= starts[bump_cmd]

    report = replay_history(build_app(workdir, 0.0), events, WORKFLOW_ID)
    assert report.ok, report.problem
    return {"llm_calls": calls, "llm_starts": llm_starts, "counter": counter, "starts": starts}


@pytest.mark.parametrize("plan", PLANS)
def test_crash_point_then_resume(tmp_path: Path, plan: str) -> None:
    crashed = harness(tmp_path, crash=plan)
    assert crashed.returncode == CRASH_EXIT_CODE, crashed.stderr
    assert "injected crash" in crashed.stderr
    resumed = harness(tmp_path)
    assert resumed.returncode == 0, resumed.stderr
    stats = check_invariants(tmp_path, json.loads(resumed.stdout), steps=3)
    if not plan.startswith(("activity_after_execute", "after_activity_started")):
        assert stats["llm_calls"] == 2  # replay never calls the model again


def test_non_idempotent_effect_can_repeat_after_mid_activity_crash(tmp_path: Path) -> None:
    """The honest caveat: at-least-once execution. ``bump_counter`` is the 8th activity
    to finish executing; dying right after it ran but before its completion was recorded
    makes the recovery run it again. Idempotent charges are unaffected."""
    crashed = harness(tmp_path, crash="activity_after_execute@8")
    assert crashed.returncode == CRASH_EXIT_CODE
    resumed = harness(tmp_path)
    result = json.loads(resumed.stdout)
    stats = check_invariants(tmp_path, result, steps=3)
    assert stats["counter"] == 2  # the non-idempotent counter was bumped twice
    assert result["bumped"] == 2


@pytest.mark.parametrize("seed", [44, 4444])
def test_random_sigkill_soak(tmp_path: Path, seed: int) -> None:
    rng = random.Random(seed)
    steps = 12
    kills = 0
    for _ in range(40):
        env = {k: v for k, v in os.environ.items() if not k.startswith("REPLAYAGENT_")}
        proc = subprocess.Popen(
            [
                sys.executable,
                "-u",
                str(HARNESS),
                str(tmp_path),
                "--steps",
                str(steps),
                "--slow",
                "0.03",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        time.sleep(rng.uniform(0.15, 0.6))
        if proc.poll() is None:
            proc.send_signal(signal.SIGKILL)
            proc.communicate()
            kills += 1
            continue
        out, err = proc.communicate()
        assert proc.returncode == 0, err
        check_invariants(tmp_path, json.loads(out), steps=steps)
        break
    else:
        pytest.fail("workflow never completed")
    assert kills >= 1
