"""Run the research agent against a local Ollama model (``make demo-ollama``).

Optional and not part of ``make check``: it needs ``ollama serve`` with the model
pulled (default ``llama3.1:8b``; override with RESEARCH_MODEL). The review step is
auto-approved. Output varies from run to run because the model is a real LLM.
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

from replayagent import Client, Store, Worker
from replayagent.events import EventType
from replayagent.examples.research import create_app
from replayagent.inspector import replay_history
from replayagent.llm import OllamaProvider

QUESTION = "How do durable execution engines avoid duplicating side effects after a crash?"


def ollama_up(host: str) -> bool:
    try:
        with urllib.request.urlopen(f"{host}/api/tags", timeout=3) as resp:
            return bool(resp.status == 200)
    except OSError:
        return False


def main() -> int:
    model = os.environ.get("RESEARCH_MODEL", "llama3.1:8b")
    provider = OllamaProvider(model=model, timeout=600)
    if not ollama_up(provider.host):
        print(f"Ollama is not reachable at {provider.host}; skipping (start `ollama serve`).")
        return 0
    with tempfile.TemporaryDirectory() as tmp:
        app = create_app(provider, Path(tmp) / "out")
        store = Store(Path(tmp) / "ollama.db")
        client = Client(store, app)
        wid = client.start("research", {"question": QUESTION, "subquestions": 2})
        client.signal(wid, "review", {"approve": True})
        t0 = time.perf_counter()
        with Worker(store, app) as worker:
            worker.run_until_complete(wid, timeout=1800)
        elapsed = time.perf_counter() - t0
        result = client.result(wid)
        events = store.load_events(wid)
        report = replay_history(app, events, wid)
        store.close()
    llm_cmds = {
        e.cmd
        for e in events
        if e.type is EventType.ACTIVITY_SCHEDULED and e.data["name"] == "llm.chat"
    }
    usage = [
        e.data["result"]["usage"]
        for e in events
        if e.type is EventType.ACTIVITY_COMPLETED and e.cmd in llm_cmds
    ]
    prompt = sum(u["prompt_tokens"] for u in usage)
    completion = sum(u["completion_tokens"] for u in usage)
    print(
        f"model {model}: {elapsed:.1f}s wall, {len(usage)} model calls, "
        f"{prompt} prompt + {completion} completion tokens"
    )
    print(f"status {result['status']}, sub-questions: {result['subquestions']}")
    print(f"tool calls per sub-question: {[n['tool_calls'] for n in result['notes']]}")
    print(f"citations: {result['citations']}  check: {result.get('citation_check')}")
    print(report.summary())
    print("-" * 72)
    print(result["report"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
