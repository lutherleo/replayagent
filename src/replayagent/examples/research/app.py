"""Assembles the research agent :class:`~replayagent.App`.

Configuration comes from arguments or environment variables:

* ``RESEARCH_PROVIDER`` - ``fake`` (default, offline and deterministic) or ``ollama``
* ``RESEARCH_MODEL`` - Ollama model name (default ``llama3.1:8b``)
* ``RESEARCH_OUT_DIR`` - where reports and the publish ledger go (default ``research-out``)
* ``RESEARCH_FAKE_LOG`` - optional JSONL file logging every fake model call
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
from pathlib import Path
from typing import Any

from replayagent.activity_context import info as activity_info
from replayagent.agent import register_tools
from replayagent.examples.research.fake_llm import research_responder
from replayagent.examples.research.workflow import (
    RESEARCH_TOOLS,
    check_citations,
    research,
    research_unguarded,
)
from replayagent.llm import FakeProvider, LLMProvider, OllamaProvider, register_llm
from replayagent.registry import ActivityOptions, App
from replayagent.retry import RetryPolicy

LEDGER_SCHEMA = """
CREATE TABLE IF NOT EXISTS publish_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT, idempotency_key TEXT NOT NULL, attempt INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS publications (
    idempotency_key TEXT PRIMARY KEY, workflow_id TEXT NOT NULL, path TEXT NOT NULL,
    chars INTEGER NOT NULL
);
"""


def make_publisher(out_dir: Path) -> Any:
    """The side-effecting step: write the report file and record it in a ledger.

    The ledger insert is keyed by the activity's idempotency key, so re-running the
    activity after a crash (at-least-once execution) never records a second publication.
    ``publish_attempts`` counts raw executions to make that visible.
    """

    def publish(doc: dict[str, Any]) -> dict[str, Any]:
        info = activity_info()
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{info.workflow_id}.md"
        tmp = path.with_suffix(".md.tmp")
        tmp.write_text(str(doc["report"]) + "\n", encoding="utf-8")
        tmp.replace(path)
        with contextlib.closing(sqlite3.connect(out_dir / "ledger.sqlite")) as db, db:
            db.executescript(LEDGER_SCHEMA)
            db.execute(
                "INSERT INTO publish_attempts(idempotency_key, attempt) VALUES (?, ?)",
                (info.idempotency_key, info.attempt),
            )
            db.execute(
                "INSERT OR IGNORE INTO publications(idempotency_key, workflow_id, path, chars) "
                "VALUES (?, ?, ?, ?)",
                (info.idempotency_key, info.workflow_id, path.name, len(str(doc["report"]))),
            )
        return {"path": path.name, "chars": len(str(doc["report"]))}

    return publish


def make_provider(kind: str | None = None, *, log_path: str | None = None) -> LLMProvider:
    kind = (kind or os.environ.get("RESEARCH_PROVIDER") or "fake").lower()
    if kind == "fake":
        return FakeProvider(
            research_responder, log_path=log_path or os.environ.get("RESEARCH_FAKE_LOG") or None
        )
    if kind == "ollama":
        return OllamaProvider(model=os.environ.get("RESEARCH_MODEL", "llama3.1:8b"))
    raise ValueError(f"unknown RESEARCH_PROVIDER {kind!r} (use 'fake' or 'ollama')")


def create_app(
    provider: LLMProvider | None = None,
    out_dir: str | Path | None = None,
    *,
    guarded: bool = True,
) -> App:
    """Build the research agent app (CLI spec: ``replayagent.examples.research:create_app``).

    ``guarded=False`` registers the variant whose citation check is *not* behind
    ``get_version`` - a deliberately broken deploy for the determinism demo.
    """
    app = App()
    app.register_workflow(research if guarded else research_unguarded, name="research")
    register_llm(app, provider or make_provider())
    register_tools(app, RESEARCH_TOOLS, retry=RetryPolicy(initial_interval=0.2, maximum_attempts=3))
    target = Path(out_dir or os.environ.get("RESEARCH_OUT_DIR") or "research-out")
    app.register_activity(
        make_publisher(target),
        name="research.publish",
        options=ActivityOptions(retry=RetryPolicy(initial_interval=0.5, maximum_attempts=5)),
    )
    app.register_activity(check_citations, name="research.check_citations")
    return app


def create_unguarded_app() -> App:
    """CLI spec ``replayagent.examples.research:create_unguarded_app`` (broken deploy)."""
    return create_app(guarded=False)
