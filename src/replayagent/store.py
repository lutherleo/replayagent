"""SQLite persistence: the event log plus three small operational tables.

* ``events``    - the append-only history (source of truth).
* ``workflows`` - one row per workflow: status, lease owner and history length. Every
  append is a compare-and-set on ``(history_len, owner)``, which both orders writes and
  *fences* a worker that lost its lease.
* ``timers``    - an index of due times (durable timers, retry backoff, activity and
  signal timeouts), written in the same transaction as the event that implies it.
* ``inbox``     - signals and cancellations from outside. Only the lease holder writes
  history, so external writers never race the worker; the worker moves an inbox row into
  history and deletes it in one transaction (exactly-once delivery).
"""

from __future__ import annotations

import contextlib
import os
import socket
import sqlite3
import threading
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from replayagent import serde
from replayagent.errors import LeaseLost, WorkflowAlreadyExists, WorkflowNotFound
from replayagent.events import Event, EventType, PendingEvent, WorkflowStatus

SCHEMA_VERSION = "1"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS workflows (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    history_len INTEGER NOT NULL,
    owner TEXT,
    lease_until REAL,
    result TEXT,
    error TEXT,
    code_hash TEXT
);
CREATE INDEX IF NOT EXISTS workflows_by_status ON workflows (status, owner);
CREATE TABLE IF NOT EXISTS events (
    workflow_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    type TEXT NOT NULL,
    cmd INTEGER,
    ts REAL NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (workflow_id, seq)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS timers (
    workflow_id TEXT NOT NULL,
    cmd INTEGER NOT NULL,
    kind TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    fire_at REAL NOT NULL,
    PRIMARY KEY (workflow_id, cmd, kind)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS timers_by_fire_at ON timers (fire_at);
CREATE TABLE IF NOT EXISTS inbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    name TEXT,
    payload TEXT,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS inbox_by_workflow ON inbox (workflow_id, id);
"""

TimerKind = Literal["timer", "retry", "activity_timeout", "signal_timeout"]
Synchronous = Literal["OFF", "NORMAL", "FULL", "EXTRA"]


@dataclass(frozen=True)
class TimerPut:
    cmd: int
    kind: TimerKind
    fire_at: float
    attempt: int = 0


@dataclass(frozen=True)
class TimerDel:
    cmd: int
    kind: TimerKind


@dataclass(frozen=True)
class TimerRow:
    workflow_id: str
    cmd: int
    kind: TimerKind
    attempt: int
    fire_at: float


@dataclass(frozen=True)
class InboxItem:
    id: int
    workflow_id: str
    kind: Literal["signal", "cancel"]
    name: str | None
    payload: Any


@dataclass(frozen=True)
class WorkflowRecord:
    id: str
    type: str
    status: WorkflowStatus
    created_at: float
    updated_at: float
    history_len: int
    owner: str | None
    lease_until: float | None
    result: Any
    error: dict[str, Any] | None
    code_hash: str | None


def new_owner_id() -> str:
    """Identity of a worker: ``host/pid/nonce``."""
    return f"{socket.gethostname()}/{os.getpid()}/{uuid.uuid4().hex[:8]}"


def owner_is_dead(owner: str) -> bool:
    """True if ``owner`` names a process on this host that no longer exists.

    This is what lets a restarted worker take over immediately after a crash instead of
    waiting for the lease to expire. Owners on other hosts (or unparseable ones) are
    only reclaimed through lease expiry.
    """
    host, _, rest = owner.partition("/")
    pid_text, _, _ = rest.partition("/")
    if host != socket.gethostname() or not pid_text.isdigit():
        return False
    pid = int(pid_text)
    if pid == os.getpid():
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


class Store:
    """Thread-safe handle on one SQLite database file."""

    def __init__(
        self, path: str | Path, *, synchronous: Synchronous = "FULL", busy_timeout: float = 30.0
    ) -> None:
        if synchronous not in ("OFF", "NORMAL", "FULL", "EXTRA"):
            raise ValueError(f"invalid synchronous mode {synchronous!r}")
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self.path, timeout=busy_timeout, isolation_level=None, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        try:
            self._conn.execute(f"PRAGMA synchronous={synchronous}")
            self._init_schema()
        except BaseException:
            self._conn.close()
            raise

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(_SCHEMA)
            row = self._conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
            if row is None:
                self._conn.execute(
                    "INSERT OR IGNORE INTO meta(key, value) VALUES ('schema_version', ?)",
                    (SCHEMA_VERSION,),
                )
            elif row["value"] != SCHEMA_VERSION:
                raise RuntimeError(
                    f"database schema version {row['value']} is not supported "
                    f"(expected {SCHEMA_VERSION})"
                )

    @contextlib.contextmanager
    def _tx(self) -> Iterator[sqlite3.Cursor]:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                yield cur
            except BaseException:
                cur.execute("ROLLBACK")
                raise
            cur.execute("COMMIT")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- workflows -----------------------------------------------------------------------

    def create_workflow(
        self,
        workflow_id: str,
        workflow_type: str,
        started: PendingEvent,
        now: float,
        code_hash: str | None = None,
    ) -> None:
        """Insert a workflow row plus its ``WorkflowStarted`` event atomically."""
        with self._tx() as cur:
            exists = cur.execute("SELECT 1 FROM workflows WHERE id=?", (workflow_id,)).fetchone()
            if exists:
                raise WorkflowAlreadyExists(f"workflow {workflow_id!r} already exists")
            cur.execute(
                "INSERT INTO workflows(id, type, status, created_at, updated_at, history_len,"
                " code_hash) VALUES (?, ?, ?, ?, ?, 1, ?)",
                (workflow_id, workflow_type, WorkflowStatus.RUNNING.value, now, now, code_hash),
            )
            cur.execute(
                "INSERT INTO events(workflow_id, seq, type, cmd, ts, data) "
                "VALUES (?, 1, ?, ?, ?, ?)",
                (workflow_id, started.type.value, started.cmd, now, serde.dumps(started.data)),
            )

    def get_workflow(self, workflow_id: str) -> WorkflowRecord:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM workflows WHERE id=?", (workflow_id,)
            ).fetchone()
        if row is None:
            raise WorkflowNotFound(f"no workflow {workflow_id!r}")
        return _record(row)

    def list_workflows(
        self, status: WorkflowStatus | None = None, limit: int = 100
    ) -> list[WorkflowRecord]:
        sql = "SELECT * FROM workflows"
        params: list[Any] = []
        if status is not None:
            sql += " WHERE status=?"
            params.append(status.value)
        sql += " ORDER BY created_at DESC, id LIMIT ?"
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [_record(r) for r in rows]

    def load_events(self, workflow_id: str, after_seq: int = 0) -> list[Event]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT seq, type, cmd, ts, data FROM events WHERE workflow_id=? AND seq>? "
                "ORDER BY seq",
                (workflow_id, after_seq),
            ).fetchall()
        return [
            Event(
                seq=r["seq"],
                type=EventType(r["type"]),
                ts=r["ts"],
                cmd=r["cmd"],
                data=serde.loads(r["data"]),
            )
            for r in rows
        ]

    def append(
        self,
        workflow_id: str,
        owner: str,
        expected_len: int,
        events: Sequence[PendingEvent],
        *,
        now: float,
        timers: Sequence[TimerPut | TimerDel] = (),
        status: WorkflowStatus | None = None,
        result: Any = None,
        error: dict[str, Any] | None = None,
        consume_inbox: Sequence[int] = (),
    ) -> list[Event]:
        """Append ``events`` after ``expected_len`` if ``owner`` still holds the lease.

        Everything (events, timer index, status, inbox consumption) commits atomically or
        not at all. Raises :class:`LeaseLost` when the compare-and-set fails.
        """
        new_len = expected_len + len(events)
        stored: list[Event] = []
        with self._tx() as cur:
            sets = "history_len=?, updated_at=?"
            params: list[Any] = [new_len, now]
            if status is not None:
                sets += ", status=?, result=?, error=?"
                params += [
                    status.value,
                    None if result is None else serde.dumps(result),
                    None if error is None else serde.dumps(error),
                ]
                if status.is_terminal:
                    sets += ", owner=NULL, lease_until=NULL"
            updated = cur.execute(
                f"UPDATE workflows SET {sets} WHERE id=? AND history_len=? AND owner=?",
                [*params, workflow_id, expected_len, owner],
            ).rowcount
            if updated != 1:
                raise LeaseLost(f"workflow {workflow_id!r}: lease lost or history moved on")
            for offset, pe in enumerate(events, start=1):
                seq = expected_len + offset
                cur.execute(
                    "INSERT INTO events(workflow_id, seq, type, cmd, ts, data) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (workflow_id, seq, pe.type.value, pe.cmd, now, serde.dumps(pe.data)),
                )
                stored.append(Event(seq=seq, type=pe.type, ts=now, cmd=pe.cmd, data=pe.data))
            for op in timers:
                if isinstance(op, TimerPut):
                    cur.execute(
                        "INSERT OR REPLACE INTO timers(workflow_id, cmd, kind, attempt, fire_at) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (workflow_id, op.cmd, op.kind, op.attempt, op.fire_at),
                    )
                else:
                    cur.execute(
                        "DELETE FROM timers WHERE workflow_id=? AND cmd=? AND kind=?",
                        (workflow_id, op.cmd, op.kind),
                    )
            for inbox_id in consume_inbox:
                gone = cur.execute(
                    "DELETE FROM inbox WHERE id=? AND workflow_id=?", (inbox_id, workflow_id)
                ).rowcount
                if gone != 1:
                    raise LeaseLost(f"inbox item {inbox_id} already consumed")
            if status is not None and status.is_terminal:
                # A closed workflow has no timers and accepts no more signals.
                cur.execute("DELETE FROM timers WHERE workflow_id=?", (workflow_id,))
                cur.execute("DELETE FROM inbox WHERE workflow_id=?", (workflow_id,))
        return stored

    # -- leases --------------------------------------------------------------------------

    def claim(
        self, owner: str, *, now: float, ttl: float, types: Sequence[str], limit: int = 100
    ) -> list[str]:
        """Take leases on running workflows that are unowned, expired or orphaned."""
        if not types:
            return []
        marks = ",".join("?" for _ in types)
        with self._lock:
            rows = self._conn.execute(
                f"SELECT id, owner, lease_until FROM workflows WHERE status=? AND type IN ({marks})"
                " AND (owner IS NULL OR owner != ?) ORDER BY created_at, id LIMIT ?",
                [WorkflowStatus.RUNNING.value, *types, owner, limit],
            ).fetchall()
        claimed: list[str] = []
        for row in rows:
            prev = row["owner"]
            if prev is not None and (row["lease_until"] or 0) >= now and not owner_is_dead(prev):
                continue
            with self._tx() as cur:
                ok = cur.execute(
                    "UPDATE workflows SET owner=?, lease_until=? "
                    "WHERE id=? AND status=? AND owner IS ?",
                    (owner, now + ttl, row["id"], WorkflowStatus.RUNNING.value, prev),
                ).rowcount
            if ok == 1:
                claimed.append(row["id"])
        return claimed

    def renew(self, owner: str, *, until: float) -> set[str]:
        """Extend every lease held by ``owner``; returns the ids still held."""
        with self._tx() as cur:
            cur.execute("UPDATE workflows SET lease_until=? WHERE owner=?", (until, owner))
            rows = cur.execute("SELECT id FROM workflows WHERE owner=?", (owner,)).fetchall()
        return {r["id"] for r in rows}

    def release(self, owner: str, workflow_ids: Sequence[str] | None = None) -> None:
        with self._tx() as cur:
            if workflow_ids is None:
                cur.execute(
                    "UPDATE workflows SET owner=NULL, lease_until=NULL WHERE owner=?", (owner,)
                )
            else:
                cur.executemany(
                    "UPDATE workflows SET owner=NULL, lease_until=NULL WHERE id=? AND owner=?",
                    [(wid, owner) for wid in workflow_ids],
                )

    def block(self, workflow_id: str, owner: str, error: dict[str, Any], *, now: float) -> None:
        """Park a workflow whose code no longer replays; history is left untouched."""
        with self._tx() as cur:
            cur.execute(
                "UPDATE workflows SET status=?, error=?, updated_at=?, owner=NULL, "
                "lease_until=NULL WHERE id=? AND owner=?",
                (WorkflowStatus.BLOCKED.value, serde.dumps(error), now, workflow_id, owner),
            )

    def resume(self, workflow_id: str, *, now: float) -> bool:
        """Move a blocked workflow back to running (after a code fix)."""
        with self._tx() as cur:
            changed = cur.execute(
                "UPDATE workflows SET status=?, error=NULL, updated_at=? WHERE id=? AND status=?",
                (WorkflowStatus.RUNNING.value, now, workflow_id, WorkflowStatus.BLOCKED.value),
            ).rowcount
        return changed == 1

    # -- inbox ---------------------------------------------------------------------------

    def enqueue(
        self,
        workflow_id: str,
        kind: Literal["signal", "cancel"],
        *,
        now: float,
        name: str | None = None,
        payload: Any = None,
    ) -> int:
        with self._tx() as cur:
            row = cur.execute("SELECT status FROM workflows WHERE id=?", (workflow_id,)).fetchone()
            if row is None:
                raise WorkflowNotFound(f"no workflow {workflow_id!r}")
            cur.execute(
                "INSERT INTO inbox(workflow_id, kind, name, payload, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (workflow_id, kind, name, serde.dumps(payload), now),
            )
            return int(cur.lastrowid or 0)

    def inbox_for_owner(self, owner: str) -> list[InboxItem]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT i.id, i.workflow_id, i.kind, i.name, i.payload FROM inbox i "
                "JOIN workflows w ON w.id = i.workflow_id WHERE w.owner=? ORDER BY i.id",
                (owner,),
            ).fetchall()
        return [
            InboxItem(
                id=r["id"],
                workflow_id=r["workflow_id"],
                kind=r["kind"],
                name=r["name"],
                payload=serde.loads(r["payload"]),
            )
            for r in rows
        ]

    def pending_inbox(self, workflow_id: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM inbox WHERE workflow_id=?", (workflow_id,)
            ).fetchone()
        return int(row["n"])

    # -- timers --------------------------------------------------------------------------

    def due_timers(self, owner: str, now: float, limit: int = 500) -> list[TimerRow]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT t.workflow_id, t.cmd, t.kind, t.attempt, t.fire_at FROM timers t "
                "JOIN workflows w ON w.id = t.workflow_id WHERE w.owner=? AND t.fire_at<=? "
                "ORDER BY t.fire_at, t.workflow_id, t.cmd LIMIT ?",
                (owner, now, limit),
            ).fetchall()
        return [_timer(r) for r in rows]

    def next_timer(self, owner: str) -> float | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT MIN(t.fire_at) AS at FROM timers t JOIN workflows w "
                "ON w.id = t.workflow_id WHERE w.owner=?",
                (owner,),
            ).fetchone()
        return None if row["at"] is None else float(row["at"])

    def timers_for(self, workflow_id: str) -> list[TimerRow]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT workflow_id, cmd, kind, attempt, fire_at FROM timers "
                "WHERE workflow_id=? ORDER BY fire_at",
                (workflow_id,),
            ).fetchall()
        return [_timer(r) for r in rows]


def _timer(row: sqlite3.Row) -> TimerRow:
    return TimerRow(
        workflow_id=row["workflow_id"],
        cmd=row["cmd"],
        kind=row["kind"],
        attempt=row["attempt"],
        fire_at=row["fire_at"],
    )


def _record(row: sqlite3.Row) -> WorkflowRecord:
    return WorkflowRecord(
        id=row["id"],
        type=row["type"],
        status=WorkflowStatus(row["status"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        history_len=row["history_len"],
        owner=row["owner"],
        lease_until=row["lease_until"],
        result=None if row["result"] is None else serde.loads(row["result"]),
        error=None if row["error"] is None else serde.loads(row["error"]),
        code_hash=row["code_hash"],
    )
