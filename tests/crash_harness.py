"""Subprocess target for the crash tests: one durable "order" workflow.

    python tests/crash_harness.py <workdir> [--steps N] [--slow S]

Starts the workflow on first run (and queues its approval signal), then runs a worker
until the workflow completes and prints the result as JSON. Crash injection comes from
the REPLAYAGENT_CRASH environment variable (or from the parent sending SIGKILL).
"""

from __future__ import annotations

import argparse
import contextlib
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from replayagent import (
    App,
    Client,
    RetryPolicy,
    Store,
    Worker,
    WorkflowContext,
    activity,
    activity_info,
    faults,
    workflow,
)
from replayagent.errors import WorkflowAlreadyExists
from replayagent.llm import FakeProvider, chat, register_llm, user

WORKFLOW_ID = "order-42"
LEDGER_SCHEMA = """
CREATE TABLE IF NOT EXISTS charges (key TEXT PRIMARY KEY, amount INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS executions (id INTEGER PRIMARY KEY, key TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY CHECK (id = 1), n INTEGER NOT NULL);
INSERT OR IGNORE INTO counter(id, n) VALUES (1, 0);
"""


def build_app(workdir: Path, slow: float) -> App:
    ledger = workdir / "ledger.sqlite"

    def connect() -> sqlite3.Connection:
        db = sqlite3.connect(ledger, timeout=30, isolation_level=None)
        db.executescript(LEDGER_SCHEMA)
        return db

    @activity(retry=RetryPolicy(initial_interval=0.01, maximum_attempts=20))
    def charge(amount: int) -> str:
        """Idempotent side effect: one ledger row per idempotency key."""
        key = activity_info().idempotency_key
        time.sleep(slow)
        with contextlib.closing(connect()) as db:
            db.execute("INSERT INTO executions(key) VALUES (?)", (key,))
            db.execute("INSERT OR IGNORE INTO charges(key, amount) VALUES (?, ?)", (key, amount))
        return key

    @activity(retry=RetryPolicy(initial_interval=0.01, maximum_attempts=20))
    def bump_counter() -> int:
        """Deliberately NOT idempotent (control case for the at-least-once caveat)."""
        with contextlib.closing(connect()) as db:
            db.execute("UPDATE counter SET n = n + 1 WHERE id = 1")
            return int(db.execute("SELECT n FROM counter").fetchone()[0])

    @workflow
    def order(ctx: WorkflowContext, params: dict[str, Any]) -> dict[str, Any]:
        plan = chat(ctx, [user("plan the order")]).text
        receipts = ctx.gather(*[ctx.start_activity(charge, i) for i in (1, 2, 3)])
        for step in range(params["steps"]):
            receipts.append(ctx.activity(charge, 10 + step))
        ctx.sleep(0.05)
        approval = ctx.wait_signal("approve", timeout=600)
        bumped = ctx.activity(bump_counter)
        summary = chat(ctx, [user(f"summarise {len(receipts)} receipts")]).text
        return {
            "plan": plan,
            "receipts": receipts,
            "approval": approval,
            "bumped": bumped,
            "summary": summary,
        }

    app = App(workflows=[order], activities=[charge, bump_counter])
    provider = FakeProvider(
        lambda req: f"ok: {req.messages[-1].content}", log_path=workdir / "llm.jsonl"
    )
    register_llm(app, provider, retry=RetryPolicy(initial_interval=0.01, maximum_attempts=20))
    return app


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("workdir", type=Path)
    parser.add_argument("--steps", type=int, default=3)
    parser.add_argument("--slow", type=float, default=0.0)
    args = parser.parse_args()
    faults.install_from_env()
    app = build_app(args.workdir, args.slow)
    store = Store(args.workdir / "history.db")
    client = Client(store, app)
    try:
        client.start("order", {"steps": args.steps}, workflow_id=WORKFLOW_ID)
        client.signal(WORKFLOW_ID, "approve", {"by": "reviewer"})
    except WorkflowAlreadyExists:
        pass
    worker = Worker(store, app, poll_interval=0.005, lease_ttl=30)
    worker.run_until_complete(WORKFLOW_ID, timeout=60)
    worker.close()
    print(json.dumps(client.result(WORKFLOW_ID)), flush=True)
    store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
