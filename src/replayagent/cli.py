"""``replayagent`` command line: run workers, start/signal/cancel workflows, and inspect,
export or replay histories.

Exit codes: 0 success, 1 error, 2 usage error, 3 replay diverged, 4 workflow did not
complete successfully.
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import signal
import sys
import threading
from collections.abc import Sequence
from pathlib import Path
from typing import Any, TextIO

from replayagent import faults, serde
from replayagent.client import Client
from replayagent.clock import SystemClock
from replayagent.errors import (
    ReplayAgentError,
    WorkflowBlockedError,
    WorkflowCancelledError,
    WorkflowFailedError,
)
from replayagent.events import WorkflowStatus
from replayagent.inspector import (
    describe,
    export_history,
    format_description,
    format_history,
    load_history_file,
    replay_history,
)
from replayagent.registry import App
from replayagent.store import Store
from replayagent.worker import Worker

DB_ENV = "REPLAYAGENT_DB"
EXIT_DIVERGED = 3
EXIT_NOT_COMPLETED = 4


class CLIError(Exception):
    """User-facing error; printed without a traceback."""


def load_app(spec: str) -> App:
    """Load ``package.module:attribute`` where the attribute is an App or a factory."""
    module_name, _, attr = spec.partition(":")
    if not module_name or not attr:
        raise CLIError(f"--app must look like 'package.module:attribute', got {spec!r}")
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise CLIError(f"cannot import {module_name!r}: {exc}") from exc
    target = getattr(module, attr, None)
    if target is None:
        raise CLIError(f"{module_name!r} has no attribute {attr!r}")
    app = target if isinstance(target, App) else target() if callable(target) else None
    if not isinstance(app, App):
        raise CLIError(f"{spec} is neither an App nor a function returning one")
    return app


def _json_arg(text: str | None, what: str) -> Any:
    if text is None:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise CLIError(f"{what} is not valid JSON: {exc}") from exc


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="replayagent", description="Durable execution runtime for LLM agents."
    )
    p.add_argument(
        "--db", default=None, help=f"SQLite database (default ${DB_ENV} or ./replayagent.db)"
    )
    p.add_argument("-v", "--verbose", action="count", default=0, help="log more (-vv for debug)")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("start", help="start a workflow")
    s.add_argument("workflow")
    s.add_argument("--app", required=True)
    s.add_argument("--input", default=None, help="workflow input as JSON")
    s.add_argument("--id", dest="workflow_id", default=None)

    w = sub.add_parser("worker", help="run a worker")
    w.add_argument("--app", required=True)
    mode = w.add_mutually_exclusive_group()
    mode.add_argument("--until-idle", action="store_true", help="exit when nothing is runnable")
    mode.add_argument("--until", metavar="ID", help="exit when this workflow stops running")
    w.add_argument("--threads", type=int, default=8)
    w.add_argument("--max-timer-wait", type=float, default=5.0)
    w.add_argument("--timeout", type=float, default=3600.0)

    ls = sub.add_parser("list", help="list workflows")
    ls.add_argument("--status", choices=[s.value for s in WorkflowStatus])
    ls.add_argument("--limit", type=int, default=50)
    ls.add_argument("--json", action="store_true")

    sh = sub.add_parser("show", help="describe a workflow")
    sh.add_argument("workflow_id")
    sh.add_argument("--app", default=None, help="compare against current code hash")

    h = sub.add_parser("history", help="print a workflow's event history")
    h.add_argument("workflow_id")
    h.add_argument("--json", action="store_true")
    h.add_argument("--width", type=int, default=70, help="detail column width (0 = full)")

    e = sub.add_parser("export", help="export a history to JSON")
    e.add_argument("workflow_id")
    e.add_argument("-o", "--output", default=None)

    r = sub.add_parser("replay", help="replay a history against current code")
    r.add_argument("workflow_id", nargs="?")
    r.add_argument("--history", default=None, help="exported history file instead of the DB")
    r.add_argument("--app", required=True)

    sg = sub.add_parser("signal", help="send a signal")
    sg.add_argument("workflow_id")
    sg.add_argument("name")
    sg.add_argument("--payload", default=None, help="JSON payload")

    c = sub.add_parser("cancel", help="cancel a workflow (hard stop)")
    c.add_argument("workflow_id")
    c.add_argument("--reason", default=None)

    rs = sub.add_parser("resume", help="unblock a workflow after a code fix")
    rs.add_argument("workflow_id")

    res = sub.add_parser("result", help="print a workflow result")
    res.add_argument("workflow_id")
    res.add_argument("--wait", type=float, default=0.0, help="seconds to wait for completion")
    return p


def _db_path(args: argparse.Namespace) -> str:
    return str(args.db or os.environ.get(DB_ENV) or "replayagent.db")


def _cmd_start(args: argparse.Namespace, store: Store, out: TextIO) -> int:
    app = load_app(args.app)
    wid = Client(store, app).start(
        args.workflow, _json_arg(args.input, "--input"), workflow_id=args.workflow_id
    )
    out.write(f"{wid}\n")
    return 0


def _cmd_worker(args: argparse.Namespace, store: Store, out: TextIO) -> int:
    app = load_app(args.app)
    faults.install_from_env()
    worker = Worker(store, app, activity_threads=args.threads)
    try:
        if args.until_idle:
            worker.run_until_idle(max_timer_wait=args.max_timer_wait, timeout=args.timeout)
        elif args.until:
            status = worker.run_until_complete(args.until, timeout=args.timeout)
            out.write(f"{args.until} {status.value}\n")
        else:
            stop = threading.Event()
            for sig in (signal.SIGINT, signal.SIGTERM):
                signal.signal(sig, lambda *_: stop.set())
            worker.run_forever(stop)
    finally:
        worker.close()
    s = worker.stats
    out.write(
        f"worker done: {s.workflow_tasks} workflow tasks, {s.activities_started} activity "
        f"attempts, {s.activities_recovered} recovered, {s.workflows_completed} finished, "
        f"{s.workflows_blocked} blocked\n"
    )
    return 0


def _cmd_list(args: argparse.Namespace, store: Store, out: TextIO) -> int:
    status = WorkflowStatus(args.status) if args.status else None
    records = store.list_workflows(status, limit=args.limit)
    if args.json:
        rows = [
            {"id": r.id, "type": r.type, "status": r.status.value, "events": r.history_len}
            for r in records
        ]
        out.write(json.dumps(rows, indent=2) + "\n")
        return 0
    out.write(f"{'id':<36} {'type':<16} {'status':<10} {'events':>6}\n")
    for r in records:
        out.write(f"{r.id:<36} {r.type:<16} {r.status.value:<10} {r.history_len:>6}\n")
    return 0


def _cmd_show(args: argparse.Namespace, store: Store, out: TextIO) -> int:
    app = load_app(args.app) if args.app else None
    desc = describe(store, args.workflow_id, app)
    out.write(format_description(desc, SystemClock().now()) + "\n")
    return 0


def _cmd_history(args: argparse.Namespace, store: Store, out: TextIO) -> int:
    store.get_workflow(args.workflow_id)
    events = store.load_events(args.workflow_id)
    if args.json:
        out.write(json.dumps([e.to_dict() for e in events], indent=1) + "\n")
    else:
        out.write(format_history(events, width=args.width) + "\n")
    return 0


def _cmd_export(args: argparse.Namespace, store: Store, out: TextIO) -> int:
    record = store.get_workflow(args.workflow_id)
    text = json.dumps(export_history(record, store.load_events(args.workflow_id)), indent=1)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
        out.write(f"wrote {args.output}\n")
    else:
        out.write(text + "\n")
    return 0


def _cmd_replay(args: argparse.Namespace, store: Store | None, out: TextIO) -> int:
    app = load_app(args.app)
    if args.history:
        wid, events = load_history_file(args.history)
    elif args.workflow_id and store is not None:
        store.get_workflow(args.workflow_id)
        wid, events = args.workflow_id, store.load_events(args.workflow_id)
    else:
        raise CLIError("replay needs a workflow id or --history FILE")
    report = replay_history(app, events, wid)
    out.write(report.summary() + "\n")
    return 0 if report.ok else EXIT_DIVERGED


def _cmd_signal(args: argparse.Namespace, store: Store, out: TextIO) -> int:
    Client(store).signal(args.workflow_id, args.name, _json_arg(args.payload, "--payload"))
    out.write(f"queued signal {args.name!r} for {args.workflow_id}\n")
    return 0


def _cmd_cancel(args: argparse.Namespace, store: Store, out: TextIO) -> int:
    Client(store).cancel(args.workflow_id, args.reason)
    out.write(f"queued cancellation for {args.workflow_id}\n")
    return 0


def _cmd_resume(args: argparse.Namespace, store: Store, out: TextIO) -> int:
    if not Client(store).resume(args.workflow_id):
        raise CLIError(f"{args.workflow_id} is not blocked")
    out.write(f"{args.workflow_id} is running again\n")
    return 0


def _cmd_result(args: argparse.Namespace, store: Store, out: TextIO) -> int:
    try:
        value = Client(store).result(args.workflow_id, timeout=args.wait)
    except TimeoutError as exc:
        out.write(f"{exc}\n")
        return EXIT_NOT_COMPLETED
    except (WorkflowFailedError, WorkflowCancelledError, WorkflowBlockedError) as exc:
        out.write(f"{exc}\n")
        return EXIT_NOT_COMPLETED
    out.write(serde.dumps(value) + "\n")
    return 0


_COMMANDS = {
    "start": _cmd_start,
    "worker": _cmd_worker,
    "list": _cmd_list,
    "show": _cmd_show,
    "history": _cmd_history,
    "export": _cmd_export,
    "signal": _cmd_signal,
    "cancel": _cmd_cancel,
    "resume": _cmd_resume,
    "result": _cmd_result,
}


def main(argv: Sequence[str] | None = None, out: TextIO | None = None) -> int:
    out = out or sys.stdout
    args = _parser().parse_args(argv)
    level = [logging.WARNING, logging.INFO, logging.DEBUG][min(args.verbose, 2)]
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        if args.command == "replay" and args.history:
            return _cmd_replay(args, None, out)
        store = Store(_db_path(args))
        try:
            if args.command == "replay":
                return _cmd_replay(args, store, out)
            return _COMMANDS[args.command](args, store, out)
        finally:
            store.close()
    except (CLIError, ReplayAgentError, ValueError, OSError) as exc:
        sys.stderr.write(f"replayagent: error: {exc}\n")
        return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
