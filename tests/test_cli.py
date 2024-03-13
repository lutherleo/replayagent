from __future__ import annotations

import io
import json
from collections.abc import Callable
from pathlib import Path

import pytest

from replayagent.cli import load_app, main
from replayagent.examples.research import create_app

APP = "replayagent.examples.research:create_app"
BROKEN = "replayagent.examples.research:create_unguarded_app"
GOLDEN = Path(__file__).with_name("golden")


@pytest.fixture
def cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Callable[..., tuple[int, str]]:
    monkeypatch.setenv("RESEARCH_OUT_DIR", str(tmp_path / "out"))
    db = str(tmp_path / "cli.db")

    def run(*argv: str) -> tuple[int, str]:
        out = io.StringIO()
        code = main(["--db", db, *argv], out=out)
        return code, out.getvalue()

    return run


def test_full_lifecycle(
    cli: Callable[..., tuple[int, str]], capsys: pytest.CaptureFixture[str]
) -> None:
    code, out = cli(
        "start", "research", "--app", APP, "--id", "c1", "--input", '{"question": "q?"}'
    )
    assert (code, out) == (0, "c1\n")
    code, out = cli("worker", "--app", APP, "--until-idle", "--max-timer-wait", "0")
    assert code == 0 and "0 finished" in out
    code, out = cli("show", "c1", "--app", APP)
    assert "status     running" in out and "wait_signal #" in out
    assert cli("signal", "c1", "review", "--payload", '{"approve": true}')[0] == 0
    code, out = cli("show", "c1")
    assert "1 item(s) waiting for a worker" in out
    code, out = cli("worker", "--app", APP, "--until", "c1")
    assert "c1 completed" in out
    code, out = cli("result", "c1")
    assert json.loads(out)["status"] == "published"
    code, out = cli("history", "c1", "--width", "30")
    assert "WorkflowCompleted" in out and "SignalConsumed" in out
    code, out = cli("history", "c1", "--json")
    assert json.loads(out)[0]["type"] == "WorkflowStarted"
    code, out = cli("list", "--json")
    assert json.loads(out)[0]["status"] == "completed"
    code, out = cli("list", "--status", "completed")
    assert "c1" in out
    code, out = cli("replay", "c1", "--app", APP)
    assert code == 0 and out.startswith("OK  c1")
    assert cli("signal", "c1", "review")[0] == 1
    assert "is completed" in capsys.readouterr().err


def test_export_and_replay_file(cli: Callable[..., tuple[int, str]], tmp_path: Path) -> None:
    code, out = cli("replay", "--history", str(GOLDEN / "research_v0.json"), "--app", APP)
    assert code == 0
    code, out = cli("replay", "--history", str(GOLDEN / "research_v0.json"), "--app", BROKEN)
    assert code == 3 and out.startswith("DIVERGED")
    cli("start", "research", "--app", APP, "--id", "e1", "--input", '"why?"')
    target = tmp_path / "e1.json"
    assert cli("export", "e1", "-o", str(target)) == (0, f"wrote {target}\n")
    assert json.loads(target.read_text())["workflow"]["id"] == "e1"
    assert json.loads(cli("export", "e1")[1])["events"][0]["seq"] == 1


def test_cancel_resume_and_result_codes(cli: Callable[..., tuple[int, str]]) -> None:
    cli("start", "research", "--app", APP, "--id", "x1", "--input", '"q"')
    assert cli("result", "x1")[0] == 4  # still running
    assert cli("resume", "x1")[0] == 1  # not blocked
    assert cli("cancel", "x1", "--reason", "test")[0] == 0
    cli("worker", "--app", APP, "--until-idle")
    code, out = cli("result", "x1")
    assert code == 4 and "cancelled" in out
    # Started by the unguarded build, then the guarded build is deployed: blocked.
    cli("start", "research", "--app", BROKEN, "--id", "b1", "--input", '"q"')
    cli("worker", "--app", BROKEN, "--until-idle", "--max-timer-wait", "0")
    cli("signal", "b1", "review", "--payload", '{"approve": true}')
    code, out = cli("worker", "--app", APP, "--until-idle")
    assert "1 blocked" in out
    code, out = cli("result", "b1")
    assert code == 4 and "blocked" in out and "non-deterministic" in out
    assert cli("show", "b1")[1].count("status     blocked") == 1
    assert cli("resume", "b1") == (0, "b1 is running again\n")
    cli("worker", "--app", BROKEN, "--until", "b1")  # roll back to the build that started it
    assert json.loads(cli("result", "b1")[1])["status"] == "published"


def test_errors_are_reported(
    cli: Callable[..., tuple[int, str]], capsys: pytest.CaptureFixture[str]
) -> None:
    assert cli("show", "missing")[0] == 1
    assert cli("start", "research", "--app", "nope", "--input", "1")[0] == 1
    assert cli("start", "research", "--app", APP, "--input", "{bad")[0] == 1
    assert cli("replay", "--app", APP)[0] == 1
    err = capsys.readouterr().err
    assert "no workflow 'missing'" in err and "not valid JSON" in err


def test_load_app_variants() -> None:
    assert load_app(APP).workflow_names == ["research"]
    for bad in ("no-colon", "json:nothing_here", "replayagent.serde:dumps", "missing_mod_xyz:app"):
        with pytest.raises(Exception, match=r"."):
            load_app(bad)
    app = create_app()
    import replayagent.examples.research as mod

    mod.ready_app = app  # type: ignore[attr-defined]
    try:
        assert load_app("replayagent.examples.research:ready_app") is app
    finally:
        del mod.ready_app  # type: ignore[attr-defined]
