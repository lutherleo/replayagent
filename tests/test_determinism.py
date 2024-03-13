"""Determinism and versioning against recorded ("golden") histories.

``tests/golden/research_v0.json`` was recorded by the research workflow *before* the
citation check existed (see the git history); ``research_v1.json`` by the current code.
Both must keep replaying; the unguarded variant of the change must be rejected.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from replayagent import WorkflowStatus
from replayagent.errors import HistoryCorruptError
from replayagent.events import EventType
from replayagent.examples.research import create_app
from replayagent.examples.research.app import create_unguarded_app
from replayagent.inspector import export_history, load_history_file, replay_history
from tests.conftest import Env

EnvFactory = Callable[..., Env]
GOLDEN = Path(__file__).with_name("golden")


@pytest.mark.parametrize("name", ["research_v0", "research_v1"])
def test_golden_histories_replay_against_current_code(name: str, tmp_path: Path) -> None:
    wid, events = load_history_file(GOLDEN / f"{name}.json")
    report = replay_history(create_app(out_dir=tmp_path), events, wid)
    assert report.ok, report.problem
    assert report.outcome == "completed"
    assert report.replayed_commands == report.history_commands


def test_v0_history_takes_the_old_branch() -> None:
    _, events = load_history_file(GOLDEN / "research_v0.json")
    assert not any(e.type is EventType.VERSION_MARKER for e in events)
    _, v1 = load_history_file(GOLDEN / "research_v1.json")
    markers = [e.data for e in v1 if e.type is EventType.VERSION_MARKER]
    assert markers == [{"change_id": "citation-check", "version": 1}]


def test_unguarded_change_is_detected_as_non_determinism() -> None:
    wid, events = load_history_file(GOLDEN / "research_v0.json")
    report = replay_history(create_unguarded_app(), events, wid)
    assert not report.ok
    assert report.problem is not None
    assert "command #11" in report.problem
    assert "history has wait_signal 'review'" in report.problem
    assert "code issued activity 'research.check_citations'" in report.problem
    assert report.summary().startswith("DIVERGED")


def test_replay_detects_changed_results_and_outcomes(make_env: EnvFactory, tmp_path: Path) -> None:
    wid, events = load_history_file(GOLDEN / "research_v1.json")
    final = events[-1]
    tampered = [*events[:-1], type(final)(final.seq, final.type, final.ts, None, {"result": {}})]
    report = replay_history(create_app(out_dir=tmp_path), tampered, wid)
    assert report.problem == "code returns a different result than the recorded one"
    failed = [
        *events[:-1],
        type(final)(final.seq, EventType.WORKFLOW_FAILED, final.ts, None, {"error": {"type": "X"}}),
    ]
    report = replay_history(create_app(out_dir=tmp_path), failed, wid)
    assert report.problem == "history failed (X) but code ended completed"
    running = events[:30]
    report = replay_history(create_app(out_dir=tmp_path), running, wid)
    assert report.ok and report.outcome == "suspended"
    empty = replay_history(create_app(out_dir=tmp_path), [], wid)
    assert empty.problem is not None and "not registered" in empty.problem


def test_live_v1_workflow_survives_redeploy_with_or_without_the_guard(
    make_env: EnvFactory, tmp_path: Path
) -> None:
    good = create_app(out_dir=tmp_path / "out")
    env = make_env(good)
    _, v0 = load_history_file(GOLDEN / "research_v0.json")
    question = v0[0].data["input"]
    # An in-flight workflow started by today's code reaches the review wait...
    wid = env.client.start("research", question, workflow_id="live")
    env.run(max_timer_wait=0)
    exported = export_history(env.client.get(wid), env.client.history(wid))
    assert exported["format"] == "replayagent-history/1"
    # ...then the guarded code replays it fine, and new workflows record the marker.
    assert replay_history(good, env.client.history(wid), wid).ok
    env.client.signal(wid, "review", {"approve": True})
    # The unguarded code still matches *v1* histories.
    env.run(create_app(out_dir=tmp_path / "out", guarded=False))
    assert env.client.get(wid).status is WorkflowStatus.COMPLETED


def test_load_history_file_rejects_other_formats(tmp_path: Path) -> None:
    bad = tmp_path / "x.json"
    bad.write_text('{"format": "something-else"}')
    with pytest.raises(HistoryCorruptError):
        load_history_file(bad)
