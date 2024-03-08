"""Deterministic stand-in for the research agent's model.

It reads the role tag in the system prompt and produces extractive, citation-bearing
answers from the tool results, so the offline demo exercises the full control flow
(planning JSON, tool calls, parallel research, drafting, revision) with no model.
"""

from __future__ import annotations

import json
import re

from replayagent.llm.types import LLMRequest, Message, ToolCall

#: (focus, search keywords) pairs the fake planner uses to fan a question out.
ASPECTS = (
    ("core mechanism", "event history recorded replay"),
    ("failure handling", "retries backoff idempotency keys"),
    ("practical safeguards", "versioning determinism tests"),
    ("human oversight", "signals approval review"),
    ("storage", "sqlite durability transactions"),
)


def _field(text: str, name: str) -> str:
    match = re.search(rf"^{name}:\s*(.*)$", text, flags=re.MULTILINE)
    return match.group(1).strip() if match else ""


def _section(text: str, name: str) -> str:
    _, _, rest = text.partition(f"{name}:\n")
    return rest.split("\n\nFEEDBACK:", 1)[0].strip()


def research_responder(request: LLMRequest) -> Message | str:
    sys_prompt = request.messages[0].content if request.messages else ""
    last = request.messages[-1]
    if "[planner]" in sys_prompt:
        question = _field(last.content, "QUESTION")
        count = int(_field(last.content, "COUNT") or 3)
        subs = [f"{question} Focus: {name} ({words})." for name, words in ASPECTS[:count]]
        return json.dumps({"subquestions": subs})
    if "[researcher]" in sys_prompt:
        tool_msgs = [m for m in request.messages if m.role == "tool"]
        if not tool_msgs and request.tools:
            subq = _field(request.messages[1].content, "SUBQUESTION")
            call = ToolCall("call_0", "search_corpus", {"query": subq, "k": 3})
            return Message("assistant", "", tool_calls=(call,))
        if not tool_msgs:
            return "- No evidence was gathered for this sub-question."
        try:
            hits = json.loads(tool_msgs[-1].content)
        except json.JSONDecodeError:
            hits = []
        bullets = [f"- {h['snippet']} [{h['doc_id']}]" for h in hits[:2] if isinstance(h, dict)]
        return "\n".join(bullets) or "- The corpus has nothing on this sub-question."
    if "[writer]" in sys_prompt:
        question = _field(last.content, "QUESTION")
        notes = _section(last.content, "NOTES")
        bullets = [ln for ln in notes.splitlines() if ln.startswith("- ")]
        sources = sorted(set(re.findall(r"\[([a-z0-9-]+)\]", notes)))
        summary = bullets[0][2:] if bullets else "The corpus does not answer this question."
        lines = [f"# {question}", "", "## Summary", summary, "", "## Findings", notes, ""]
        lines += ["## Sources", *[f"- [{s}]" for s in sources]]
        return "\n".join(lines)
    if "[reviser]" in sys_prompt:
        draft = _section(last.content, "DRAFT")
        feedback = last.content.split("FEEDBACK:\n", 1)[-1].strip()
        return f"{draft}\n\n## Reviewer feedback addressed\n- {feedback}"
    return "I can only help with the research workflow."
