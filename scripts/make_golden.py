"""Record a golden history of the research workflow for replay tests.

Usage: python scripts/make_golden.py <name>   ->  tests/golden/<name>.json

The run is fully deterministic apart from the workflow's random seed: fake model,
virtual clock, fixed workflow id, one revision request followed by an approval.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

from replayagent import Client, FakeClock, Store, Worker
from replayagent.examples.research import create_app
from replayagent.inspector import export_history

ROOT = Path(__file__).resolve().parent.parent
QUESTION = "How do durable execution engines avoid duplicating side effects after a crash?"


def main(name: str) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        app = create_app(out_dir=Path(tmp) / "out")
        store = Store(Path(tmp) / "golden.db")
        clock = FakeClock()
        client = Client(store, app, clock=clock)
        wid = client.start("research", {"question": QUESTION}, workflow_id=f"golden-{name}")
        worker = Worker(store, app, clock=clock)
        worker.run_until_idle()
        client.signal(wid, "review", {"approve": False, "feedback": "Add a one-line summary."})
        worker.run_until_idle()
        client.signal(wid, "review", {"approve": True})
        worker.run_until_complete(wid)
        worker.close()
        exported = export_history(store.get_workflow(wid), store.load_events(wid))
        store.close()
    out = ROOT / "tests" / "golden" / f"{name}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(exported, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {out.relative_to(ROOT)} ({len(exported['events'])} events)")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: make_golden.py <name>")
    main(sys.argv[1])
