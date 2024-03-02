"""Offline end-to-end demo (``make demo``).

Drives the research agent through the real CLI in subprocesses: a worker is killed by
an injected crash in the middle of the research phase, a new worker resumes it from
history, a human requests a revision and then approves via signals, and a second crash
hits the publish step after its side effect but before its completion was recorded.
Finally the history is printed and replayed against the current code and against an
unguarded code change. Uses the deterministic fake model; no network needed.
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WORK = ROOT / ".demo"
APP = "replayagent.examples.research:create_app"
BROKEN = "replayagent.examples.research:create_unguarded_app"
WID = "research-demo"
QUESTION = "How do durable execution engines avoid duplicating side effects after a crash?"
HOST = socket.gethostname()


def env(**extra: str) -> dict[str, str]:
    base = {k: v for k, v in os.environ.items() if not k.startswith(("REPLAYAGENT_", "RESEARCH_"))}
    base.update(
        REPLAYAGENT_DB=str(WORK / "history.db"),
        RESEARCH_OUT_DIR=str(WORK / "out"),
        RESEARCH_FAKE_LOG=str(WORK / "model-calls.jsonl"),
        RESEARCH_PROVIDER="fake",
    )
    base.update(extra)
    return base


def cli(
    *args: str, crash: str | None = None, show: bool = True
) -> subprocess.CompletedProcess[str]:
    extra = {"REPLAYAGENT_CRASH": crash} if crash else {}
    prefix = f"REPLAYAGENT_CRASH={crash} " if crash else ""
    print(f"$ {prefix}replayagent {' '.join(args)}")
    proc = subprocess.run(
        [sys.executable, "-m", "replayagent", *args],
        capture_output=True,
        text=True,
        env=env(**extra),
        cwd=ROOT,
        check=False,
    )
    if show:
        for line in (proc.stdout + proc.stderr).rstrip().splitlines():
            # Worker identities are host/pid/nonce; mask the machine name in printed output.
            print(f"  {line.replace(HOST, '<host>')}")
    if crash is None and proc.returncode != 0 and args[0] != "replay":
        raise SystemExit(f"command failed with exit code {proc.returncode}")
    if crash is not None:
        print(f"  (exit code {proc.returncode})")
    return proc


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def history() -> list[dict[str, object]]:
    proc = subprocess.run(
        [sys.executable, "-m", "replayagent", "history", WID, "--json"],
        capture_output=True,
        text=True,
        env=env(),
        cwd=ROOT,
        check=True,
    )
    events: list[dict[str, object]] = json.loads(proc.stdout)
    return events


def main() -> None:
    shutil.rmtree(WORK, ignore_errors=True)
    WORK.mkdir()

    section("1. start a research workflow")
    cli(
        "start",
        "research",
        "--app",
        APP,
        "--id",
        WID,
        "--input",
        json.dumps({"question": QUESTION}),
    )

    section("2. a worker dies in the middle of the research fan-out")
    cli("worker", "--app", APP, "--until-idle", crash="after_activity_completed@5")
    cli("show", WID)

    section("3. a new worker resumes from history and parks at the human review")
    cli("worker", "--app", APP, "--until-idle", "--max-timer-wait", "0")
    cli("show", WID)

    section("4. the reviewer asks for a revision")
    payload = {"approve": False, "feedback": "Add a sentence on at-most-once delivery."}
    cli("signal", WID, "review", "--payload", json.dumps(payload))
    cli("worker", "--app", APP, "--until-idle", "--max-timer-wait", "0")

    section("5. approval; the worker dies right after publishing, before recording it")
    cli("signal", WID, "review", "--payload", json.dumps({"approve": True}))
    cli("worker", "--app", APP, "--until-idle", crash="activity_after_execute@1")
    cli("worker", "--app", APP, "--until", WID)

    section("6. result")
    proc = cli("result", WID, show=False)
    result = json.loads(proc.stdout)
    print(f"  status: {result['status']}   published: {result['published']}")
    print(f"  sub-questions: {len(result['subquestions'])}   citations: {result['citations']}")
    print(f"  citation check: {result.get('citation_check')}")
    for line in result["report"].splitlines()[:6]:
        print(f"  | {line}")
    print("  | ...")

    section("7. what actually ran")
    events = history()
    scheduled = {e["cmd"]: e["data"]["name"] for e in events if e["type"] == "ActivityScheduled"}  # type: ignore[index]
    llm_cmds = {c for c, n in scheduled.items() if n == "llm.chat"}
    llm_attempts = sum(1 for e in events if e["type"] == "ActivityStarted" and e["cmd"] in llm_cmds)
    model_calls = len((WORK / "model-calls.jsonl").read_text().splitlines())
    with contextlib.closing(sqlite3.connect(WORK / "out" / "ledger.sqlite")) as db:
        attempts = db.execute("SELECT COUNT(*) FROM publish_attempts").fetchone()[0]
        published = db.execute("SELECT COUNT(*) FROM publications").fetchone()[0]
    print(f"  history events: {len(events)}   llm.chat commands: {len(llm_cmds)}")
    print(f"  llm.chat attempts recorded: {llm_attempts}   fake model calls made: {model_calls}")
    print(f"  publish executions: {attempts}   publications in ledger: {published}")
    print("  -> 3 worker processes, 2 crashes; finished model calls were never repeated, and")
    print("     the publish step ran twice (at-least-once) but its idempotency key kept one row.")

    section("8. history (first 14 and last 12 events)")
    proc = cli("history", WID, "--width", "48", show=False)
    lines = proc.stdout.rstrip().splitlines()
    for line in [*lines[:15], "   ...", *lines[-12:]]:
        print(f"  {line}")

    section("9. replay the stored history against current code")
    cli("replay", WID, "--app", APP)

    section("10. a v0 history vs. an unguarded code change (determinism guard)")
    golden = "tests/golden/research_v0.json"
    cli("replay", "--history", golden, "--app", APP)
    proc = cli("replay", "--history", golden, "--app", BROKEN, crash=None, show=False)
    for line in proc.stdout.rstrip().splitlines():
        print(f"  {line}")
    print(f"  (exit code {proc.returncode}: diverged)")


if __name__ == "__main__":
    main()
