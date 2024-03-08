"""The research agent workflow.

plan sub-questions -> research each one with a tool-calling loop (in parallel) ->
draft a report -> wait for a human review signal (revise on feedback) -> publish.

Every model call, tool call and the publish step are activities, so the workflow can be
killed at any point and resumed without repeating finished work.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from replayagent.agent import Tool, run_tool_loops, tool
from replayagent.errors import SignalTimeout
from replayagent.examples.research.corpus import default_corpus
from replayagent.llm import chat, system, user
from replayagent.replay import DEFAULT_VERSION, WorkflowContext

PLANNER_PROMPT = (
    "[planner] You plan research. Break the QUESTION into COUNT focused, non-overlapping "
    'sub-questions. Reply with JSON only: {"subquestions": ["...", "..."]}.'
)
RESEARCHER_PROMPT = (
    "[researcher] Answer the SUBQUESTION using only the document corpus. Call the "
    "search_corpus tool first. Then answer in 2-4 short bullet points; end every bullet with "
    "the id of the document it comes from in square brackets, for example [event-sourcing]."
)
WRITER_PROMPT = (
    "[writer] Write a concise markdown report that answers the QUESTION using only the NOTES. "
    "Use the sections '## Summary', '## Findings' and '## Sources'. Keep the [doc-id] "
    "citations from the notes."
)
REVISER_PROMPT = (
    "[reviser] Revise the DRAFT report to address the reviewer FEEDBACK. Keep the structure "
    "and the [doc-id] citations. Return the complete revised report in markdown."
)

REVIEW_SIGNAL = "review"

#: ``get_version`` change id for the citation check added after the first release.
CITATION_CHECK = "citation-check"


@tool
def search_corpus(query: str, k: int = 3) -> list[dict[str, Any]]:
    """Search the research corpus and return the best matching passages with their
    document ids."""
    return default_corpus().search(query, k=min(max(int(k), 1), 5))


@tool
def read_document(doc_id: str) -> str:
    """Return the full text of one corpus document by its id."""
    doc = default_corpus().get(doc_id)
    return doc.text if doc else f"no document with id {doc_id!r}"


RESEARCH_TOOLS: list[Tool] = [search_corpus, read_document]


@dataclass(frozen=True)
class ResearchParams:
    question: str
    subquestions: int = 3
    review_timeout: float = 24 * 3600.0
    max_revisions: int = 2
    model: str | None = None

    @classmethod
    def from_input(cls, raw: Any) -> ResearchParams:
        if isinstance(raw, str):
            raw = {"question": raw}
        if not isinstance(raw, dict) or not str(raw.get("question", "")).strip():
            raise ValueError("research input needs a non-empty 'question'")
        return cls(
            question=str(raw["question"]).strip(),
            subquestions=max(1, min(int(raw.get("subquestions", 3)), 5)),
            review_timeout=float(raw.get("review_timeout", 24 * 3600.0)),
            max_revisions=max(0, int(raw.get("max_revisions", 2))),
            model=raw.get("model"),
        )


def parse_subquestions(text: str, question: str, count: int) -> list[str]:
    """Extract sub-questions from a planner reply (JSON, or one per line as a fallback)."""
    items: list[str] = []
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            data = data.get("subquestions", [])
        if isinstance(data, list):
            items = [str(x) for x in data if isinstance(x, str | int | float)]
    except json.JSONDecodeError:
        items = [re.sub(r"^\s*(?:[-*]|\d+[.)])\s*", "", line) for line in text.splitlines()]
    seen: list[str] = []
    for item in (i.strip() for i in items):
        if item and item.lower() not in (s.lower() for s in seen):
            seen.append(item)
    return seen[:count] or [question]


CITATION = re.compile(r"\[([a-z0-9][a-z0-9-]*)\]")


def citations(text: str) -> list[str]:
    """Document ids cited as ``[doc-id]``, in first-seen order."""
    out: list[str] = []
    for doc_id in CITATION.findall(text):
        if doc_id not in out:
            out.append(doc_id)
    return out


def check_citations(report: str) -> dict[str, Any]:
    """Activity: verify every ``[doc-id]`` citation names a real corpus document."""
    corpus = default_corpus()
    cited = citations(report)
    return {
        "cited": cited,
        "unknown": [c for c in cited if corpus.get(c) is None],
    }


def _strip_unknown(report: str, unknown: list[str]) -> str:
    for doc_id in unknown:
        report = report.replace(f"[{doc_id}]", "[citation removed]")
    return report


def research(ctx: WorkflowContext, raw: Any) -> dict[str, Any]:
    """The research workflow. v1 added a citation check behind ``get_version`` so
    workflows started by v0 code still replay."""
    return _research(
        ctx, raw, citation_check=lambda: ctx.get_version(CITATION_CHECK, DEFAULT_VERSION, 1) >= 1
    )


def research_unguarded(ctx: WorkflowContext, raw: Any) -> dict[str, Any]:
    """What the v1 change looks like *without* the version guard. Replaying a v0
    history against it raises NonDeterminismError (used by tests and the demo)."""
    return _research(ctx, raw, citation_check=lambda: True)


def _research(
    ctx: WorkflowContext, raw: Any, *, citation_check: Callable[[], bool]
) -> dict[str, Any]:
    params = ResearchParams.from_input(raw)
    q = params.question
    plan = chat(
        ctx,
        [system(PLANNER_PROMPT), user(f"QUESTION: {q}\nCOUNT: {params.subquestions}")],
        json_mode=True,
        model=params.model,
    )
    subquestions = parse_subquestions(plan.text, q, params.subquestions)
    ctx.logger.info("planned %d sub-questions", len(subquestions))

    conversations = [[system(RESEARCHER_PROMPT), user(f"SUBQUESTION: {s}")] for s in subquestions]
    results = run_tool_loops(ctx, conversations, RESEARCH_TOOLS, max_turns=3, model=params.model)
    notes = [
        {"subquestion": s, "notes": r.final.strip(), "tool_calls": r.tool_calls}
        for s, r in zip(subquestions, results, strict=True)
    ]
    notes_text = "\n\n".join(f"### {n['subquestion']}\n{n['notes']}" for n in notes)
    draft = chat(
        ctx,
        [system(WRITER_PROMPT), user(f"QUESTION: {q}\n\nNOTES:\n{notes_text}")],
        model=params.model,
    ).text

    checked: dict[str, Any] | None = None
    if citation_check():
        checked = ctx.activity("research.check_citations", draft)
        draft = _strip_unknown(draft, checked["unknown"])

    reviews: list[dict[str, Any]] = []
    while True:
        try:
            review = ctx.wait_signal(REVIEW_SIGNAL, timeout=params.review_timeout)
        except SignalTimeout:
            return _result(params, notes, draft, reviews, checked, "review_timed_out", None)
        review = review if isinstance(review, dict) else {"approve": bool(review)}
        reviews.append(review)
        if review.get("approve"):
            break
        if len(reviews) > params.max_revisions:
            return _result(params, notes, draft, reviews, checked, "rejected", None)
        feedback = str(review.get("feedback", "")).strip() or "Improve clarity."
        draft = chat(
            ctx,
            [system(REVISER_PROMPT), user(f"DRAFT:\n{draft}\n\nFEEDBACK:\n{feedback}")],
            model=params.model,
        ).text

    published = ctx.activity("research.publish", {"question": q, "report": draft})
    return _result(params, notes, draft, reviews, checked, "published", published)


def _result(
    params: ResearchParams,
    notes: list[dict[str, Any]],
    report: str,
    reviews: list[dict[str, Any]],
    citation_check: dict[str, Any] | None,
    status: str,
    published: Any,
) -> dict[str, Any]:
    out = {
        "status": status,
        "question": params.question,
        "subquestions": [n["subquestion"] for n in notes],
        "notes": notes,
        "report": report,
        "citations": citations(report),
        "reviews": reviews,
        "published": published,
    }
    if citation_check is not None:
        out["citation_check"] = citation_check
    return out
