"""Workflow-task replay semantics, tested against hand-built histories."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import pytest

from replayagent import DEFAULT_VERSION, App, WorkflowContext, activity, serde
from replayagent.errors import NonDeterminismError, VersionError, WorkflowTaskError
from replayagent.events import Event, EventType
from replayagent.history import HistoryView
from replayagent.registry import WorkflowDefinition, code_fingerprint
from replayagent.replay import TaskResult, execute_task


@activity
def greet(name: str) -> str:
    return f"hi {name}"


@activity
def other(x: int) -> int:
    return x


APP = App(activities=[greet, other])


class H:
    """Tiny history builder."""

    def __init__(self, inp: Any = None) -> None:
        self.events: list[Event] = []
        self._cmd = 0
        self.add(EventType.WORKFLOW_STARTED, type="wf", input=inp, seed=42)

    def add(self, etype: EventType, cmd: int | None = None, **data: Any) -> H:
        self.events.append(Event(len(self.events) + 1, etype, float(len(self.events)), cmd, data))
        return self

    def cmd(self, etype: EventType, **data: Any) -> int:
        c = self._cmd
        self._cmd += 1
        self.add(etype, c, **data)
        return c

    def activity(self, name: str, *args: Any, result: Any = None, done: bool = True) -> H:
        enc = serde.roundtrip(list(args))
        c = self.cmd(
            EventType.ACTIVITY_SCHEDULED,
            name=name,
            input=enc,
            input_hash=serde.digest(enc),
            options={},
        )
        self.add(EventType.ACTIVITY_STARTED, c, attempt=1)
        if done:
            self.add(EventType.ACTIVITY_COMPLETED, c, attempt=1, result=result)
        return self

    def version(self, change_id: str, version: int) -> H:
        self.cmd(EventType.VERSION_MARKER, change_id=change_id, version=version)
        return self


def run(fn: Callable[[WorkflowContext, Any], Any], h: H) -> TaskResult:
    defn = WorkflowDefinition("wf", fn, code_fingerprint(fn))
    return execute_task(defn, workflow_id="w", view=HistoryView(h.events), app=APP, now=lambda: 7.0)


def two_greets(ctx: WorkflowContext, inp: Any) -> list[str]:
    return [ctx.activity(greet, "a"), ctx.activity(greet, "b")]


def test_new_workflow_issues_first_command_and_suspends() -> None:
    res = run(two_greets, H())
    assert res.status == "suspended"
    assert [(c.cmd, c.type, c.data["name"]) for c in res.commands] == [
        (0, EventType.ACTIVITY_SCHEDULED, "greet")
    ]


def test_replay_returns_recorded_results_without_new_commands() -> None:
    h = H().activity("greet", "a", result="A").activity("greet", "b", result="B")
    res = run(two_greets, h)
    assert (res.status, res.result, res.commands, res.replayed_commands) == (
        "completed",
        ["A", "B"],
        [],
        2,
    )


@pytest.mark.parametrize(
    ("history", "reason"),
    [
        (H().activity("other", 1), "activity name differs"),
        (H().activity("greet", "zzz"), "activity input differs"),
        (H().add(EventType.TIMER_STARTED, 0, duration=1, fire_at=2), "command type differs"),
    ],
)
def test_mismatching_history_is_non_deterministic(history: H, reason: str) -> None:
    with pytest.raises(NonDeterminismError, match=reason) as info:
        run(two_greets, history)
    assert info.value.cmd == 0


def test_code_that_stops_early_is_non_deterministic() -> None:
    def one(ctx: WorkflowContext, inp: Any) -> str:
        return str(ctx.activity(greet, "a"))

    h = H().activity("greet", "a", result="A").activity("greet", "b", result="B")
    with pytest.raises(NonDeterminismError, match="history has 1 more command"):
        run(one, h)


def test_other_command_kinds_are_compared() -> None:
    def timer(ctx: WorkflowContext, inp: Any) -> None:
        ctx.sleep(5)

    def sig(ctx: WorkflowContext, inp: Any) -> Any:
        return ctx.wait_signal("go", timeout=3)

    def side(ctx: WorkflowContext, inp: Any) -> Any:
        return ctx.side_effect(lambda: 1, label="b")

    with pytest.raises(NonDeterminismError, match="timer duration"):
        run(timer, H().add(EventType.TIMER_STARTED, 0, duration=9.0, fire_at=1))
    with pytest.raises(NonDeterminismError, match="signal name"):
        run(sig, H().add(EventType.SIGNAL_WAIT_STARTED, 0, name="stop", timeout=3.0))
    with pytest.raises(NonDeterminismError, match="signal timeout"):
        run(sig, H().add(EventType.SIGNAL_WAIT_STARTED, 0, name="go", timeout=4.0))
    with pytest.raises(NonDeterminismError, match="side effect label"):
        run(side, H().add(EventType.SIDE_EFFECT, 0, label="a", value=1))


def test_violation_cannot_be_swallowed_by_user_code() -> None:
    def sneaky(ctx: WorkflowContext, inp: Any) -> str:
        try:
            ctx.activity(greet, "a")
        except Exception:
            return "caught"
        return "ok"

    with pytest.raises(NonDeterminismError):
        run(sneaky, H().activity("other", 1))


def test_swallowed_suspension_is_detected() -> None:
    def swallow_then_return(ctx: WorkflowContext, inp: Any) -> str:
        try:
            ctx.activity(greet, "a")
        except BaseException:
            return "oops"
        return "never"

    def swallow_then_raise(ctx: WorkflowContext, inp: Any) -> str:
        try:
            ctx.activity(greet, "a")
        except BaseException:
            raise ValueError("boom") from None
        return "never"

    with pytest.raises(WorkflowTaskError, match="swallowed"):
        run(swallow_then_return, H())
    with pytest.raises(WorkflowTaskError, match="swallowed"):
        run(swallow_then_raise, H())


def test_bad_commands_are_task_errors() -> None:
    def unknown(ctx: WorkflowContext, inp: Any) -> Any:
        return ctx.activity("not-registered")

    def bad_args(ctx: WorkflowContext, inp: Any) -> Any:
        return ctx.activity(greet, object())

    def bad_side_effect(ctx: WorkflowContext, inp: Any) -> Any:
        return ctx.side_effect(object)

    for fn in (unknown, bad_args, bad_side_effect):
        with pytest.raises(WorkflowTaskError):
            run(fn, H())


def test_workflow_errors_and_unserialisable_results_fail_the_workflow() -> None:
    def raises(ctx: WorkflowContext, inp: Any) -> None:
        raise KeyError("missing")

    def returns_object(ctx: WorkflowContext, inp: Any) -> object:
        return object()

    res = run(raises, H())
    assert res.status == "failed" and res.error and res.error["type"] == "KeyError"
    res = run(returns_object, H())
    assert res.status == "failed" and res.error and res.error["type"] == "SerializationError"


def test_side_effects_now_and_random_are_replay_stable() -> None:
    def wf(ctx: WorkflowContext, inp: Any) -> list[Any]:
        return [ctx.now(), ctx.uuid4(), ctx.random().random(), ctx.side_effect(lambda: [1, 2])]

    first = run(wf, H())
    assert first.status == "completed"
    assert [c.data["label"] for c in first.commands] == ["now", ""]
    h = H()
    for c in first.commands:
        h.cmd(c.type, **c.data)
    again = run(wf, h)
    assert again.result == first.result
    assert again.commands == []


def test_wait_any_picks_earliest_recorded_resolution() -> None:
    def wf(ctx: WorkflowContext, inp: Any) -> str:
        a = ctx.start_activity(greet, "a")
        b = ctx.start_activity(greet, "b")
        first = ctx.wait_any(a, b)
        return "a" if first is a else "b"

    h = H().activity("greet", "a", done=False).activity("greet", "b", result="B")
    assert run(wf, h).result == "b"
    h.add(EventType.ACTIVITY_COMPLETED, 0, attempt=1, result="A")
    assert run(wf, h).result == "b"  # still b: b's outcome was recorded first
    assert run(wf, H()).status == "suspended"

    def empty(ctx: WorkflowContext, inp: Any) -> Any:
        return ctx.wait_any()

    assert run(empty, H()).status == "failed"


def test_gather_suspends_until_all_resolved() -> None:
    def wf(ctx: WorkflowContext, inp: Any) -> list[Any]:
        return ctx.gather(ctx.start_activity(greet, "a"), ctx.start_activity(greet, "b"))

    first = run(wf, H())
    assert [c.data["input"] for c in first.commands] == [["a"], ["b"]]
    h = H().activity("greet", "a", result=1).activity("greet", "b", done=False)
    assert run(wf, h).status == "suspended"


def test_activity_failure_surfaces_as_activity_error() -> None:
    def wf(ctx: WorkflowContext, inp: Any) -> str:
        try:
            ctx.activity(greet, "a")
        except Exception as exc:
            return type(exc).__name__ + ":" + str(getattr(exc, "error_type", ""))
        return "no"

    h = H().activity("greet", "a", done=False)
    h.add(EventType.ACTIVITY_FAILED, 0, attempt=1, error={"type": "Boom", "message": "x"})
    assert run(wf, h).result == "ActivityError:Boom"


def test_signal_timeout_raises_signal_timeout() -> None:
    def wf(ctx: WorkflowContext, inp: Any) -> str:
        try:
            ctx.wait_signal("go", timeout=5)
        except Exception as exc:
            return type(exc).__name__
        return "signalled"

    h = H().add(EventType.SIGNAL_WAIT_STARTED, 0, name="go", timeout=5.0, deadline=6.0)
    h.add(EventType.SIGNAL_WAIT_TIMED_OUT, 0)
    assert run(wf, h).result == "SignalTimeout"


def test_argument_validation() -> None:
    def neg_sleep(ctx: WorkflowContext, inp: Any) -> None:
        ctx.sleep(-1)

    def neg_timeout(ctx: WorkflowContext, inp: Any) -> None:
        ctx.wait_signal("x", timeout=0)

    def bad_version(ctx: WorkflowContext, inp: Any) -> None:
        ctx.get_version("x", 2, 1)

    for fn in (neg_sleep, neg_timeout, bad_version):
        assert run(fn, H()).status == "failed"


# -- get_version -----------------------------------------------------------------------------


def versioned(ctx: WorkflowContext, inp: Any) -> list[Any]:
    out = [ctx.activity(greet, "a")]
    if ctx.get_version("extra-step", DEFAULT_VERSION, 1) >= 1:
        out.append(ctx.activity(other, 1))
    out.append(ctx.activity(greet, "b"))
    return out


def test_get_version_new_execution_records_max_version() -> None:
    h = H().activity("greet", "a", result="A")
    res = run(versioned, h)
    assert [(c.type, c.data.get("version"), c.data.get("name")) for c in res.commands] == [
        (EventType.VERSION_MARKER, 1, None),
        (EventType.ACTIVITY_SCHEDULED, None, "other"),
    ]


def test_get_version_old_history_takes_default_branch() -> None:
    old = H().activity("greet", "a", result="A").activity("greet", "b", result="B")
    assert run(versioned, old).result == ["A", "B"]


def test_get_version_replays_recorded_marker() -> None:
    h = H().activity("greet", "a", result="A").version("extra-step", 1)
    h.activity("other", 1, result=1).activity("greet", "b", result="B")
    assert run(versioned, h).result == ["A", 1, "B"]


def test_removed_get_version_call_skips_stale_marker() -> None:
    def cleaned_up(ctx: WorkflowContext, inp: Any) -> list[Any]:
        return [ctx.activity(greet, "a"), ctx.activity(other, 1), ctx.activity(greet, "b")]

    h = H().activity("greet", "a", result="A").version("extra-step", 1)
    h.activity("other", 1, result=1).activity("greet", "b", result="B")
    assert run(cleaned_up, h).result == ["A", 1, "B"]


def test_removed_one_of_two_adjacent_get_version_calls_replays() -> None:
    def both(ctx: WorkflowContext, inp: Any) -> list[Any]:
        out = [ctx.activity(greet, "a")]
        a = ctx.get_version("a", DEFAULT_VERSION, 1)
        b = ctx.get_version("b", DEFAULT_VERSION, 1)
        out.append(ctx.activity(other, 2) if b >= 1 else ctx.activity(other, -1))
        return [*out, a, b]

    def a_removed(ctx: WorkflowContext, inp: Any) -> list[Any]:
        out = [ctx.activity(greet, "a")]
        b = ctx.get_version("b", DEFAULT_VERSION, 1)
        out.append(ctx.activity(other, 2) if b >= 1 else ctx.activity(other, -1))
        return [*out, b]

    def b_removed(ctx: WorkflowContext, inp: Any) -> list[Any]:
        out = [ctx.activity(greet, "a")]
        a = ctx.get_version("a", DEFAULT_VERSION, 1)
        out.append(ctx.activity(other, 2))
        return [*out, a]

    def h() -> H:
        hist = H().activity("greet", "a", result="A").version("a", 1).version("b", 1)
        return hist.activity("other", 2, result=2)

    assert run(both, h()).result == ["A", 2, 1, 1]
    assert run(a_removed, h()).result == ["A", 2, 1]
    assert run(b_removed, h()).result == ["A", 2, 1]


def test_get_version_markers_matched_by_change_id_not_order() -> None:
    def swapped(ctx: WorkflowContext, inp: Any) -> list[Any]:
        ctx.activity(greet, "a")
        b = ctx.get_version("b", DEFAULT_VERSION, 2)
        a = ctx.get_version("a", DEFAULT_VERSION, 2)
        ctx.activity(other, 2)
        return [a, b]

    h = H().activity("greet", "a", result="A").version("a", 1).version("b", 2)
    h.activity("other", 2, result=2)
    assert run(swapped, h).result == [1, 2]


def test_get_version_same_change_id_is_stable_within_an_execution() -> None:
    def two_sites(ctx: WorkflowContext, inp: Any) -> list[Any]:
        ctx.activity(greet, "a")
        first = ctx.get_version("c", DEFAULT_VERSION, 1)
        ctx.activity(greet, "b")
        second = ctx.get_version("c", DEFAULT_VERSION, 1)
        ctx.activity(other, 3)
        return [first, second]

    # In flight before the change: the first site is replayed (old), so the second site
    # past the end of history must also be old, and no marker may be recorded for it.
    old = H().activity("greet", "a", result="A").activity("greet", "b", result="B")
    res = run(two_sites, old)
    assert res.status == "suspended"
    assert [(c.type, c.data.get("name")) for c in res.commands] == [
        (EventType.ACTIVITY_SCHEDULED, "other")
    ]
    old.activity("other", 3, result=3)
    assert run(two_sites, old).result == [DEFAULT_VERSION, DEFAULT_VERSION]

    # A new execution records one marker and gets the new version at both sites.
    new = H().activity("greet", "a", result="A")
    res = run(two_sites, new)
    assert [c.type for c in res.commands] == [
        EventType.VERSION_MARKER,
        EventType.ACTIVITY_SCHEDULED,
    ]
    new.version("c", 1).activity("greet", "b", result="B")
    res = run(two_sites, new)
    assert [c.type for c in res.commands] == [EventType.ACTIVITY_SCHEDULED]
    new.activity("other", 3, result=3)
    assert run(two_sites, new).result == [1, 1]


def test_get_version_honours_a_second_recorded_marker_for_the_same_change_id() -> None:
    # Histories recorded before versions were cached per change id can hold a second marker
    # for the same id; history is the truth, so it is replayed as recorded.
    def two_sites(ctx: WorkflowContext, inp: Any) -> list[Any]:
        ctx.activity(greet, "a")
        first = ctx.get_version("c", DEFAULT_VERSION, 1)
        ctx.activity(greet, "b")
        second = ctx.get_version("c", DEFAULT_VERSION, 1)
        ctx.activity(other, 3 if second >= 1 else -3)
        return [first, second]

    h = H().activity("greet", "a", result="A").activity("greet", "b", result="B")
    h.version("c", 1).activity("other", 3, result=3)
    assert run(two_sites, h).result == [DEFAULT_VERSION, 1]


def test_get_version_cached_version_still_checked_against_range() -> None:
    def narrowing(ctx: WorkflowContext, inp: Any) -> Any:
        ctx.get_version("c", DEFAULT_VERSION, 2)  # new execution: records and caches 2
        return ctx.get_version("c", 0, 1)

    with pytest.raises(VersionError, match="history has version 2"):
        run(narrowing, H())


def test_get_version_live_records_and_old_history_rejected_when_floor_raised() -> None:
    def raised_floor(ctx: WorkflowContext, inp: Any) -> Any:
        ctx.activity(greet, "a")
        return ctx.get_version("extra-step", 1, 2)

    live = run(raised_floor, H().activity("greet", "a", result="A"))
    assert live.result == 2
    old = H().activity("greet", "a", result="A").activity("greet", "b", result="B")
    with pytest.raises(VersionError):
        run(raised_floor, old)


def test_replay_aware_logger(caplog: pytest.LogCaptureFixture) -> None:
    def wf(ctx: WorkflowContext, inp: Any) -> None:
        ctx.logger.info("before %s", "a")
        ctx.activity(greet, "a")
        ctx.logger.warning("after")
        ctx.logger.debug("debug")
        ctx.logger.error("err")

    caplog.set_level(logging.DEBUG, logger="replayagent.workflow")
    run(wf, H().activity("greet", "a", result="A"))
    messages = [r.getMessage() for r in caplog.records]
    assert "[w] before a" not in messages  # suppressed: replaying
    assert "[w] after" in messages and "[w] err" in messages
