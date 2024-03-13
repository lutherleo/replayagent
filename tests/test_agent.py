from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from replayagent import App, ApplicationError, WorkflowContext, workflow
from replayagent.agent import register_tools, run_tool_loop, run_tool_loops, tool
from replayagent.llm import FakeProvider, LLMRequest, Message, ToolCall, register_llm, user
from tests.conftest import Env

EnvFactory = Callable[..., Env]


@tool
def add(a: int, b: int = 0) -> int:
    """Add two integers.

    Extra paragraphs are not part of the description."""
    return a + b


@tool(name="lookup", description="Look a key up")
def lookup_tool(key: str, tags: list[str] | None = None, meta: dict[str, Any] | None = None) -> str:
    if key == "boom":
        raise ApplicationError("lookup failed", non_retryable=True)
    return f"value-of-{key}"


TOOLS = [add, lookup_tool]


def test_schema_from_signature() -> None:
    assert add.description == "Add two integers."
    assert add.parameters == {
        "type": "object",
        "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
        "required": ["a"],
    }
    props = lookup_tool.parameters["properties"]
    assert props["tags"] == {"type": "array", "items": {"type": "string"}}
    assert props["meta"] == {"type": "object"}
    assert lookup_tool.activity_name == "tool.lookup"
    assert lookup_tool.spec.description == "Look a key up"


def test_invoke_validates_arguments() -> None:
    assert add.invoke({"a": 2, "b": 3}) == 5
    for bad in ({"b": 1}, {"a": 1, "c": 2}):
        with pytest.raises(ApplicationError) as info:
            add.invoke(bad)
        assert info.value.non_retryable


def scripted(request: LLMRequest) -> Message | str:
    """Calls every tool kind once, then answers with what it saw."""
    question = request.messages[0].content
    tool_msgs = [m for m in request.messages if m.role == "tool"]
    if tool_msgs:
        return f"{question} -> " + " | ".join(m.content for m in tool_msgs)
    if not request.tools:
        return f"{question} -> no tools offered"
    calls = (
        ToolCall("c0", "add", {"a": 2, "b": 40}),
        ToolCall("c1", "lookup", {"key": question}),
        ToolCall("c2", "lookup", {"key": "boom"}),
        ToolCall("c3", "nope", {}),
    )
    return Message("assistant", "", tool_calls=calls)


@workflow
def agent_wf(ctx: WorkflowContext, questions: list[str]) -> list[dict[str, Any]]:
    results = run_tool_loops(ctx, [[user(q)] for q in questions], TOOLS, max_turns=3)
    return [{"final": r.final, "turns": r.turns, "calls": r.tool_calls} for r in results]


@workflow
def one_turn(ctx: WorkflowContext, q: str) -> dict[str, Any]:
    r = run_tool_loop(ctx, [user(q)], TOOLS, max_turns=1)
    return {"final": r.final, "finished": r.finished}


@workflow
def always_tools(ctx: WorkflowContext, q: str) -> dict[str, Any]:
    r = run_tool_loop(ctx, [user(q)], TOOLS, max_turns=2, llm_activity="loop.chat")
    return {"final": r.final, "finished": r.finished, "log": r.tool_log}


def make_app(responder: Callable[[LLMRequest], Message | str], name: str = "llm.chat") -> App:
    app = App(workflows=[agent_wf, one_turn, always_tools])
    register_llm(app, FakeProvider(responder), name=name)
    register_tools(app, TOOLS)
    return app


def test_lockstep_tool_loops(make_env: EnvFactory) -> None:
    env = make_env(make_app(scripted))
    wid = env.client.start(agent_wf, ["k1", "k2"])
    env.run()
    out = env.client.result(wid)
    assert out[0] == {
        "final": "k1 -> 42 | value-of-k1 | error: ApplicationError: lookup failed | "
        "error: unknown tool 'nope'; available: ['add', 'lookup']",
        "turns": 2,
        "calls": 4,
    }
    assert out[1]["final"].startswith("k2 -> 42 | value-of-k2")
    # Both conversations' model calls of a turn were scheduled together (fan-out).
    names = [e.data["name"] for e in env.client.history(wid) if e.type.value == "ActivityScheduled"]
    assert names[:2] == ["llm.chat", "llm.chat"]


def test_last_turn_forces_answer(make_env: EnvFactory) -> None:
    env = make_env(make_app(scripted))
    wid = env.client.start(one_turn, "q")
    env.run()
    assert env.client.result(wid) == {"final": "q -> no tools offered", "finished": True}


def test_unfinished_loop_reports_last_assistant_text(make_env: EnvFactory) -> None:
    def stubborn(request: LLMRequest) -> Message:
        return Message("assistant", "thinking", tool_calls=(ToolCall("c", "add", {"a": 1}),))

    env = make_env(make_app(stubborn, name="loop.chat"))
    wid = env.client.start(always_tools, "q")
    env.run()
    out = env.client.result(wid)
    assert out["finished"] is False
    assert out["final"] == "thinking"
    assert out["log"] == [{"tool": "add", "arguments": {"a": 1}}] * 2


def test_max_turns_must_be_positive(make_env: EnvFactory) -> None:
    @workflow
    def bad(ctx: WorkflowContext, _: Any) -> Any:
        return run_tool_loop(ctx, [user("x")], TOOLS, max_turns=0)

    app = make_app(scripted)
    app.register_workflow(bad)
    env = make_env(app)
    wid = env.client.start(bad)
    env.run()
    assert env.client.get(wid).status.value == "failed"
