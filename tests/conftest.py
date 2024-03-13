from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from replayagent import App, Client, FakeClock, Store, Worker, faults


@dataclass
class Env:
    store: Store
    client: Client
    clock: FakeClock
    app: App
    path: Path

    def worker(self, app: App | None = None, **kwargs: object) -> Worker:
        return Worker(self.store, app or self.app, clock=self.clock, **kwargs)  # type: ignore[arg-type]

    def run(self, app: App | None = None, *, max_timer_wait: float = float("inf")) -> Worker:
        """Run a fresh worker until idle (following timers) and close it."""
        worker = self.worker(app)
        try:
            worker.run_until_idle(max_timer_wait=max_timer_wait)
        finally:
            worker.close()
        return worker


@pytest.fixture(autouse=True)
def _no_crash_plan() -> Iterator[None]:
    faults.install(None)
    yield
    faults.install(None)


@pytest.fixture
def make_env(tmp_path: Path) -> Iterator[object]:
    stores: list[Store] = []

    def factory(app: App, name: str = "test.db") -> Env:
        store = Store(tmp_path / name)
        stores.append(store)
        clock = FakeClock()
        return Env(store, Client(store, app, clock=clock), clock, app, tmp_path / name)

    yield factory
    for store in stores:
        store.close()
