from __future__ import annotations

import contextlib
import sqlite3
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from replayagent.errors import LeaseLost, WorkflowAlreadyExists, WorkflowNotFound
from replayagent.events import EventType, PendingEvent, WorkflowStatus
from replayagent.store import Store, TimerDel, TimerPut, new_owner_id, owner_is_dead

STARTED = PendingEvent(EventType.WORKFLOW_STARTED, None, {"type": "wf", "input": 1, "seed": 3})


@pytest.fixture
def store(tmp_path: Path) -> Iterator[Store]:
    s = Store(tmp_path / "s.db")
    yield s
    s.close()


def claim(store: Store, owner: str, now: float = 100.0, ttl: float = 10.0) -> list[str]:
    return store.claim(owner, now=now, ttl=ttl, types=["wf"])


def test_create_and_load(store: Store) -> None:
    store.create_workflow("w1", "wf", STARTED, 1.0, "hash")
    with pytest.raises(WorkflowAlreadyExists):
        store.create_workflow("w1", "wf", STARTED, 1.0)
    rec = store.get_workflow("w1")
    assert (rec.status, rec.history_len, rec.code_hash) == (WorkflowStatus.RUNNING, 1, "hash")
    [ev] = store.load_events("w1")
    assert ev.type is EventType.WORKFLOW_STARTED and ev.data["input"] == 1
    with pytest.raises(WorkflowNotFound):
        store.get_workflow("nope")
    assert [r.id for r in store.list_workflows()] == ["w1"]
    assert store.list_workflows(WorkflowStatus.COMPLETED) == []


def test_append_is_fenced_by_owner_and_length(store: Store) -> None:
    store.create_workflow("w1", "wf", STARTED, 1.0)
    owner = new_owner_id()
    assert claim(store, owner) == ["w1"]
    ev = PendingEvent(EventType.SIDE_EFFECT, 0, {"label": "", "value": 1})
    stored = store.append("w1", owner, 1, [ev], now=2.0)
    assert [e.seq for e in stored] == [2]
    with pytest.raises(LeaseLost):
        store.append("w1", owner, 1, [ev], now=2.0)  # stale expected length
    with pytest.raises(LeaseLost):
        store.append("w1", "someone-else", 2, [], now=2.0)
    assert store.get_workflow("w1").history_len == 2


def test_failed_append_rolls_back_everything(store: Store) -> None:
    store.create_workflow("w1", "wf", STARTED, 1.0)
    owner = new_owner_id()
    claim(store, owner)
    store.enqueue("w1", "signal", now=1.0, name="s", payload=1)
    with pytest.raises(LeaseLost):
        store.append(
            "w1",
            owner,
            1,
            [PendingEvent(EventType.SIGNAL_RECEIVED, None, {"name": "s"})],
            now=2.0,
            timers=[TimerPut(0, "timer", 5.0)],
            consume_inbox=[999],
        )
    assert store.get_workflow("w1").history_len == 1
    assert store.timers_for("w1") == []
    assert store.pending_inbox("w1") == 1


def test_terminal_append_clears_lease_timers_and_inbox(store: Store) -> None:
    store.create_workflow("w1", "wf", STARTED, 1.0)
    owner = new_owner_id()
    claim(store, owner)
    store.append("w1", owner, 1, [], now=1.0, timers=[TimerPut(0, "timer", 5.0)])
    store.enqueue("w1", "signal", now=1.0, name="late")
    store.append(
        "w1",
        owner,
        1,
        [PendingEvent(EventType.WORKFLOW_COMPLETED, None, {"result": 7})],
        now=2.0,
        status=WorkflowStatus.COMPLETED,
        result=7,
    )
    rec = store.get_workflow("w1")
    assert (rec.status, rec.result, rec.owner) == (WorkflowStatus.COMPLETED, 7, None)
    assert store.timers_for("w1") == [] and store.pending_inbox("w1") == 0


def test_claim_rules(store: Store) -> None:
    store.create_workflow("w1", "wf", STARTED, 1.0)
    store.create_workflow("w2", "other-type", STARTED, 1.0)
    a, b = new_owner_id(), new_owner_id()
    assert claim(store, a) == ["w1"]  # only registered types
    assert claim(store, b) == []  # a's lease is live and a (this process) is alive
    assert claim(store, b, now=200.0) == ["w1"]  # expired lease
    assert store.renew(a, until=1e9) == set()
    assert store.renew(b, until=1e9) == {"w1"}
    store.release(b)
    assert store.get_workflow("w1").owner is None
    assert store.claim(a, now=1, ttl=1, types=[]) == []


def test_dead_owner_is_reclaimed_immediately(store: Store) -> None:
    proc = subprocess.run(
        [sys.executable, "-c", "import os; print(os.getpid())"],
        capture_output=True,
        text=True,
        check=True,
    )
    dead = new_owner_id().split("/")
    dead_owner = f"{dead[0]}/{proc.stdout.strip()}/deadbeef"
    assert owner_is_dead(dead_owner)
    assert not owner_is_dead(new_owner_id())  # this process
    assert not owner_is_dead("other-host/1/x")
    assert not owner_is_dead("garbage")
    store.create_workflow("w1", "wf", STARTED, 1.0)
    assert claim(store, dead_owner) == ["w1"]
    assert claim(store, new_owner_id(), now=100.0) == ["w1"]


def test_block_resume_and_release_subset(store: Store) -> None:
    store.create_workflow("w1", "wf", STARTED, 1.0)
    owner = new_owner_id()
    claim(store, owner)
    store.block("w1", owner, {"type": "NonDeterminismError", "message": "x"}, now=3.0)
    rec = store.get_workflow("w1")
    assert rec.status is WorkflowStatus.BLOCKED and rec.owner is None
    assert claim(store, owner) == []
    assert store.resume("w1", now=4.0)
    assert not store.resume("w1", now=4.0)
    assert claim(store, owner) == ["w1"]
    store.release(owner, ["w1"])
    assert store.get_workflow("w1").owner is None


def test_inbox_and_timer_queries(store: Store) -> None:
    store.create_workflow("w1", "wf", STARTED, 1.0)
    owner = new_owner_id()
    with pytest.raises(WorkflowNotFound):
        store.enqueue("nope", "signal", now=1.0)
    store.enqueue("w1", "signal", now=1.0, name="s", payload={"a": 1})
    assert store.inbox_for_owner(owner) == []  # not owned yet
    claim(store, owner)
    [item] = store.inbox_for_owner(owner)
    assert (item.kind, item.name, item.payload) == ("signal", "s", {"a": 1})
    store.append(
        "w1",
        owner,
        1,
        [],
        now=1.0,
        timers=[TimerPut(0, "timer", 50.0), TimerPut(1, "retry", 20.0, attempt=2)],
    )
    assert store.next_timer(owner) == 20.0
    assert [t.kind for t in store.due_timers(owner, 30.0)] == ["retry"]
    store.append("w1", owner, 1, [], now=1.0, timers=[TimerDel(1, "retry")])
    assert [t.cmd for t in store.timers_for("w1")] == [0]
    assert store.next_timer("nobody") is None


def test_schema_version_and_settings_are_checked(tmp_path: Path) -> None:
    path = tmp_path / "v.db"
    Store(path).close()
    with contextlib.closing(sqlite3.connect(path)) as db, db:
        db.execute("UPDATE meta SET value='99' WHERE key='schema_version'")
    with pytest.raises(RuntimeError, match="schema version 99"):
        Store(path)
    with pytest.raises(ValueError):
        Store(tmp_path / "x.db", synchronous="BOGUS")  # type: ignore[arg-type]
    mem = Store(":memory:", synchronous="NORMAL")
    mem.close()
