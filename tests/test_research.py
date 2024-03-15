from __future__ import annotations

import contextlib
import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from replayagent import activity_context
from replayagent.examples.research import ResearchParams, create_app, make_provider
from replayagent.examples.research.app import create_unguarded_app, make_publisher
from replayagent.examples.research.corpus import Corpus, Document, default_corpus, tokenize
from replayagent.examples.research.fake_llm import research_responder
from replayagent.examples.research.workflow import (
    check_citations,
    citations,
    parse_subquestions,
    read_document,
    search_corpus,
)
from replayagent.llm import FakeProvider, OllamaProvider
from tests.conftest import Env

EnvFactory = Callable[..., Env]
QUESTION = "How do durable execution engines avoid duplicating side effects after a crash?"


def test_corpus_search_ranks_relevant_documents() -> None:
    corpus = default_corpus()
    assert len(corpus.documents) == 10
    hits = corpus.search("idempotency key duplicate side effects", k=2)
    assert hits[0]["doc_id"] == "idempotency-keys"
    assert "idempotency key" in hits[0]["snippet"].lower()
    assert corpus.search("zzzz qqqq") == []
    assert tokenize("The Event, and a LOG!") == ["event", "log"]
    with pytest.raises(ValueError):
        Corpus([])
    tiny = Corpus([Document("d", "D", "# D\nalpha beta. gamma.")])
    assert tiny.search("gamma")[0]["snippet"] == "gamma."


def test_tools_and_helpers() -> None:
    assert search_corpus.invoke({"query": "durable timers", "k": 99})[0]["doc_id"] == (
        "durable-timers"
    )
    assert read_document.invoke({"doc_id": "event-sourcing"}).startswith("# Event sourcing")
    assert "no document" in read_document.invoke({"doc_id": "missing"})
    assert citations("a [x-1] b [y] c [x-1]") == ["x-1", "y"]
    assert check_citations("see [event-sourcing] and [made-up]") == {
        "cited": ["event-sourcing", "made-up"],
        "unknown": ["made-up"],
    }


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ('{"subquestions": ["a", "b", "A", "c"]}', ["a", "b"]),
        ('["x", 3]', ["x", "3"]),
        ("1. first\n- second\n\n", ["first", "second"]),
        ("{}", ["Q"]),
    ],
)
def test_parse_subquestions(text: str, expected: list[str]) -> None:
    assert parse_subquestions(text, "Q", 2) == expected


def test_params_validation() -> None:
    p = ResearchParams.from_input({"question": " q ", "subquestions": 9, "max_revisions": -3})
    assert (p.question, p.subquestions, p.max_revisions) == ("q", 5, 0)
    assert ResearchParams.from_input("plain").question == "plain"
    with pytest.raises(ValueError):
        ResearchParams.from_input({"question": ""})


def test_make_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    assert isinstance(make_provider("fake"), FakeProvider)
    assert isinstance(make_provider("ollama"), OllamaProvider)
    monkeypatch.setenv("RESEARCH_PROVIDER", "nope")
    with pytest.raises(ValueError):
        make_provider()


def run_research(env: Env, reviews: list[dict[str, Any]], **params: Any) -> dict[str, Any]:
    wid = env.client.start("research", {"question": QUESTION, **params}, workflow_id="r")
    env.run(max_timer_wait=0)
    for review in reviews:
        env.client.signal(wid, "review", review)
        env.run(max_timer_wait=0)
    env.run()  # follow remaining timers (review timeout)
    result: dict[str, Any] = env.client.result(wid)
    return result


def test_research_publishes_after_revision(make_env: EnvFactory, tmp_path: Path) -> None:
    provider = FakeProvider(research_responder)
    env = make_env(create_app(provider, tmp_path / "out"))
    result = run_research(
        env, [{"approve": False, "feedback": "Name the trade-offs."}, {"approve": True}]
    )
    assert result["status"] == "published"
    assert len(result["subquestions"]) == 3
    assert all(n["tool_calls"] == 1 for n in result["notes"])
    assert "## Reviewer feedback addressed\n- Name the trade-offs." in result["report"]
    assert result["citation_check"]["unknown"] == []
    assert set(result["citations"]) <= set(default_corpus().documents)
    assert (tmp_path / "out" / "r.md").read_text().startswith("# How do durable")
    assert result["published"] == {"path": "r.md", "chars": len(result["report"])}
    # 1 plan + 3x2 research turns + 1 draft + 1 revision
    assert len(provider.calls) == 9


def test_research_rejection_and_timeout(make_env: EnvFactory, tmp_path: Path) -> None:
    env = make_env(create_app(out_dir=tmp_path / "out"))
    rejected = run_research(env, [{"approve": False}, {"approve": False}], max_revisions=1)
    assert rejected["status"] == "rejected" and rejected["published"] is None
    env2 = make_env(create_app(out_dir=tmp_path / "out"), name="two.db")
    timed_out = run_research(env2, [], review_timeout=60, subquestions=1)
    assert timed_out["status"] == "review_timed_out"
    assert not (tmp_path / "out" / "r.md").exists()


def test_publisher_is_idempotent_per_activity(tmp_path: Path) -> None:
    publish = make_publisher(tmp_path)
    info = activity_context.ActivityInfo(
        "wf-1", "research", "research.publish", 7, 1, "wf-1/7", 0, None
    )
    token = activity_context._set(info)
    try:
        publish({"report": "v1"})
        publish({"report": "v1"})  # re-execution after a crash
    finally:
        activity_context._reset(token)
    with contextlib.closing(sqlite3.connect(tmp_path / "ledger.sqlite")) as db:
        assert db.execute("SELECT COUNT(*) FROM publish_attempts").fetchone()[0] == 2
        assert db.execute("SELECT COUNT(*) FROM publications").fetchone()[0] == 1
    with pytest.raises(RuntimeError):
        publish({"report": "outside an activity"})
    assert not activity_context.in_activity()


def test_unguarded_app_differs_only_in_registration() -> None:
    assert create_unguarded_app().workflow_names == ["research"]
