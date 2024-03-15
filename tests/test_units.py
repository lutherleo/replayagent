from __future__ import annotations

import math

import pytest

from replayagent import (
    ActivityOptions,
    App,
    ApplicationError,
    FakeClock,
    RetryPolicy,
    faults,
    serde,
)
from replayagent.errors import (
    HistoryCorruptError,
    SerializationError,
    UnknownActivity,
    UnknownWorkflowType,
)
from replayagent.events import Event, EventType, WorkflowStatus
from replayagent.history import HistoryView
from replayagent.registry import activity, activity_name_of, code_fingerprint, workflow

# -- serde ---------------------------------------------------------------------------------


def test_dumps_is_canonical() -> None:
    assert serde.dumps({"b": 1, "a": [1, 2]}) == '{"a":[1,2],"b":1}'
    assert serde.roundtrip((1, 2)) == [1, 2]
    assert serde.digest({"x": 1}) == serde.digest({"x": 1})
    assert serde.digest({"x": 1}) != serde.digest({"x": 2})


@pytest.mark.parametrize("bad", [object(), float("nan"), {1, 2}])
def test_dumps_rejects_non_json(bad: object) -> None:
    with pytest.raises(SerializationError):
        serde.dumps(bad)


def test_encode_error_keeps_application_flags() -> None:
    err = serde.encode_error(ApplicationError("nope", non_retryable=True, type="Custom"))
    assert err["type"] == "Custom"
    assert err["non_retryable"] is True
    assert "traceback" in err
    plain = serde.encode_error(ValueError("x"), include_traceback=False)
    assert plain == {"type": "ValueError", "message": "x", "non_retryable": False}


# -- retry ---------------------------------------------------------------------------------


def test_backoff_grows_and_caps() -> None:
    policy = RetryPolicy(initial_interval=1, backoff_coefficient=2, maximum_interval=5)
    assert [policy.delay(a) for a in (1, 2, 3, 4, 5)] == [1, 2, 4, 5, 5]
    assert policy.delay(10_000) == 5  # no overflow on huge attempt numbers


def test_jitter_stays_in_band() -> None:
    policy = RetryPolicy(initial_interval=10, maximum_interval=10, jitter=0.5)
    assert policy.delay(1, rand=0.0) == pytest.approx(5)
    assert policy.delay(1, rand=1.0) == pytest.approx(15)


def test_allows_retry_rules() -> None:
    policy = RetryPolicy(maximum_attempts=3, non_retryable_error_types=("Fatal",))
    assert policy.allows_retry(1, {"type": "X"})
    assert not policy.allows_retry(3, {"type": "X"})
    assert not policy.allows_retry(1, {"type": "Fatal"})
    assert not policy.allows_retry(1, {"type": "X", "non_retryable": True})
    assert RetryPolicy(maximum_attempts=0).allows_retry(10**6, {"type": "X"})


def test_policy_roundtrip_and_validation() -> None:
    policy = RetryPolicy(0.5, 3, 9, 4, 0.1, ("A",))
    assert RetryPolicy.from_dict(policy.to_dict()) == policy
    for kwargs in (
        {"initial_interval": -1},
        {"initial_interval": math.inf},
        {"backoff_coefficient": 0.5},
        {"initial_interval": 5, "maximum_interval": 1},
        {"maximum_attempts": -1},
        {"jitter": 2},
    ):
        with pytest.raises(ValueError):
            RetryPolicy(**kwargs)
    with pytest.raises(ValueError):
        policy.delay(0)


# -- clock & faults --------------------------------------------------------------------------


def test_fake_clock() -> None:
    clock = FakeClock(100)
    clock.sleep(5)
    clock.advance(1)
    assert clock.now() == 106
    with pytest.raises(ValueError):
        clock.advance(-1)


def test_crash_plan_parsing_and_hits() -> None:
    plan = faults.CrashPlan.parse("after_timer_fired@2, after_activity_started")
    assert plan.targets == {"after_timer_fired": 2, "after_activity_started": 1}
    assert not plan.hit("after_timer_fired")
    assert plan.hit("after_timer_fired")
    assert not plan.hit("after_activity_started")  # a plan crashes at most once
    with pytest.raises(ValueError):
        faults.CrashPlan.parse("nope")
    with pytest.raises(ValueError):
        faults.CrashPlan.parse("after_timer_fired@0")


def test_crashpoint_raise_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(faults.CRASH_ENV, "after_signal_received")
    monkeypatch.setenv(faults.CRASH_MODE_ENV, "raise")
    faults.install_from_env()
    faults.crashpoint("after_timer_fired")
    with pytest.raises(faults.SimulatedCrash) as info:
        faults.crashpoint("after_signal_received")
    assert info.value.point == "after_signal_received"
    monkeypatch.setenv(faults.CRASH_MODE_ENV, "bogus")
    with pytest.raises(ValueError):
        faults.install_from_env()


# -- registry ----------------------------------------------------------------------------------


def test_registry_resolution() -> None:
    @activity(name="custom", timeout=3)
    def act(x: int) -> int:
        return x

    @workflow(name="wf")
    def wf(ctx: object, inp: object) -> None:
        return None

    def plain(x: int) -> int:
        return x

    app = App(workflows=[wf], activities=[act, plain])
    assert app.activity_names == ["custom", "plain"]
    assert app.workflow_names == ["wf"]
    assert activity_name_of(act) == "custom"
    assert activity_name_of("x") == "x"
    assert app.get_activity("custom").options.timeout == 3
    assert app.resolve_workflow(wf).name == "wf"
    assert app.has_activity("plain")
    with pytest.raises(UnknownActivity):
        app.get_activity("missing")
    with pytest.raises(UnknownWorkflowType):
        app.get_workflow("missing")
    with pytest.raises(ValueError):
        app.register_activity(lambda: None, name="custom")
    with pytest.raises(ValueError):
        app.register_workflow(lambda c, i: None, name="wf")
    assert len(code_fingerprint(plain)) == 12
    assert code_fingerprint(len)  # builtins have no source


def test_activity_options_merge_and_validate() -> None:
    base = ActivityOptions(retry=RetryPolicy(maximum_attempts=2), timeout=5)
    merged = base.merged(ActivityOptions(timeout=1, delivery="at_most_once"))
    assert merged.timeout == 1
    assert merged.retry is not None and merged.retry.maximum_attempts == 2
    assert merged.to_history()["delivery"] == "at_most_once"
    assert base.merged(None) is base
    with pytest.raises(ValueError):
        ActivityOptions(timeout=0)
    with pytest.raises(ValueError):
        ActivityOptions(delivery="twice")  # type: ignore[arg-type]


def test_status_terminal_flags() -> None:
    assert WorkflowStatus.COMPLETED.is_terminal
    assert not WorkflowStatus.BLOCKED.is_terminal


# -- history fold --------------------------------------------------------------------------------


def _ev(seq: int, etype: EventType, cmd: int | None = None, **data: object) -> Event:
    return Event(seq=seq, type=etype, ts=float(seq), cmd=cmd, data=dict(data))


def _started() -> Event:
    return _ev(1, EventType.WORKFLOW_STARTED, type="wf", input=None, seed=1)


def test_history_view_folds_activity_lifecycle() -> None:
    view = HistoryView(
        [
            _started(),
            _ev(2, EventType.ACTIVITY_SCHEDULED, 0, name="a", input=[1], options={}),
            _ev(3, EventType.ACTIVITY_STARTED, 0, attempt=1),
            _ev(
                4, EventType.ACTIVITY_ATTEMPT_FAILED, 0, attempt=1, error={"type": "X"}, retry_at=9
            ),
        ]
    )
    act = view.activities[0]
    assert (act.attempt, act.inflight, act.retry_at) == (1, False, 9.0)
    view.apply(_ev(5, EventType.ACTIVITY_STARTED, 0, attempt=2))
    view.apply(_ev(6, EventType.ACTIVITY_FAILED, 0, attempt=2, error={"type": "X"}))
    assert view.resolutions[0].error == {"type": "X", "attempts": 2}
    assert not view.pending_activities()
    event = Event.from_dict(view.events[1].to_dict())
    assert event == view.events[1]


@pytest.mark.parametrize(
    "events",
    [
        [_ev(1, EventType.TIMER_FIRED, 0)],
        [_started(), _ev(3, EventType.SIDE_EFFECT, 0)],
        [_started(), _ev(2, EventType.SIDE_EFFECT, 5)],
        [_started(), _ev(2, EventType.TIMER_FIRED, 0)],
        [_started(), _ev(2, EventType.WORKFLOW_COMPLETED), _ev(3, EventType.SIDE_EFFECT, 0)],
        [
            _started(),
            _ev(2, EventType.ACTIVITY_SCHEDULED, 0, name="a", input=[], options={}),
            _ev(3, EventType.ACTIVITY_STARTED, 0, attempt=2),
        ],
        [
            _started(),
            _ev(2, EventType.SIGNAL_WAIT_STARTED, 0, name="s", timeout=None, deadline=None),
            _ev(3, EventType.SIGNAL_CONSUMED, 0, signal_seq=99, name="s"),
        ],
        [
            _started(),
            _ev(2, EventType.SIDE_EFFECT, 0, label="", value=1),
            _ev(3, EventType.ACTIVITY_COMPLETED, 0, attempt=1),
        ],
        [
            _started(),
            _ev(2, EventType.TIMER_STARTED, 0, duration=1, fire_at=2),
            _ev(3, EventType.TIMER_FIRED, 0),
            _ev(4, EventType.TIMER_FIRED, 0),
        ],
        [_started(), _ev(2, EventType.WORKFLOW_STARTED, type="wf")],
    ],
)
def test_history_view_rejects_corrupt_histories(events: list[Event]) -> None:
    with pytest.raises(HistoryCorruptError):
        HistoryView(events)


def test_history_view_signal_mailbox() -> None:
    view = HistoryView(
        [
            _started(),
            _ev(2, EventType.SIGNAL_RECEIVED, name="s", payload=1),
            _ev(3, EventType.SIGNAL_RECEIVED, name="t", payload=2),
            _ev(4, EventType.SIGNAL_WAIT_STARTED, 0, name="s", timeout=5.0, deadline=9.0),
        ]
    )
    assert [w.name for w in view.open_signal_waits()] == ["s"]
    view.apply(_ev(5, EventType.SIGNAL_CONSUMED, 0, signal_seq=2, name="s", payload=1))
    assert [e.data["name"] for e in view.mailbox] == ["t"]
    bad = HistoryView(view.events[:4])
    with pytest.raises(HistoryCorruptError):
        bad.apply(_ev(5, EventType.SIGNAL_CONSUMED, 0, signal_seq=3, name="s"))
