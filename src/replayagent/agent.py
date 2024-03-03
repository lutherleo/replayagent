"""Tool-calling agent loops built from durable activities.

Each model turn is one ``llm.chat`` activity and each tool call is one ``tool.<name>``
activity, so a crash in the middle of an agent loop resumes at the exact turn and tool
call where it stopped. :func:`run_tool_loops` advances several conversations in lockstep:
the model calls of one turn run in parallel, then all the tool calls of that turn run
in parallel.
"""

from __future__ import annotations

import inspect
import types
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Union, get_args, get_origin, get_type_hints, overload

from replayagent import serde
from replayagent.errors import ActivityError, ApplicationError
from replayagent.llm import LLM_ACTIVITY, response_of, start_chat
from replayagent.llm.types import Message, ToolCall, ToolSpec
from replayagent.registry import ActivityOptions, App
from replayagent.replay import ActivityHandle, WorkflowContext
from replayagent.retry import RetryPolicy

_JSON_TYPES: dict[Any, str] = {str: "string", int: "integer", float: "number", bool: "boolean"}


def _schema_for(annotation: Any) -> dict[str, Any]:
    if annotation in _JSON_TYPES:
        return {"type": _JSON_TYPES[annotation]}
    origin = get_origin(annotation)
    if origin in (list, tuple, Sequence):
        args = get_args(annotation)
        return {"type": "array", "items": _schema_for(args[0]) if args else {}}
    if origin is dict:
        return {"type": "object"}
    if origin in (Union, types.UnionType):
        options = [a for a in get_args(annotation) if a is not type(None)]
        if len(options) == 1:
            return _schema_for(options[0])
    return {}


@dataclass(frozen=True)
class Tool:
    """A function the model can call, executed as activity ``tool.<name>``."""

    name: str
    description: str
    parameters: dict[str, Any]
    fn: Callable[..., Any]

    @property
    def activity_name(self) -> str:
        return f"tool.{self.name}"

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(self.name, self.description, self.parameters)

    def invoke(self, arguments: dict[str, Any]) -> Any:
        """Validate model-supplied arguments against the signature and call the tool."""
        props = self.parameters.get("properties", {})
        unknown = sorted(set(arguments) - set(props))
        missing = sorted(set(self.parameters.get("required", [])) - set(arguments))
        if unknown or missing:
            raise ApplicationError(
                f"bad arguments for tool {self.name!r}: unknown={unknown} missing={missing}",
                non_retryable=True,
                type="ToolArgumentError",
            )
        return self.fn(**arguments)


@overload
def tool(fn: Callable[..., Any], /) -> Tool: ...
@overload
def tool(
    *, name: str | None = None, description: str | None = None
) -> Callable[[Callable[..., Any]], Tool]: ...
def tool(
    fn: Callable[..., Any] | None = None,
    /,
    *,
    name: str | None = None,
    description: str | None = None,
) -> Tool | Callable[[Callable[..., Any]], Tool]:
    """Build a :class:`Tool` from a typed function; the docstring's first paragraph is
    the description shown to the model."""

    def build(func: Callable[..., Any]) -> Tool:
        hints = get_type_hints(func)
        props: dict[str, Any] = {}
        required: list[str] = []
        for pname, param in inspect.signature(func).parameters.items():
            props[pname] = _schema_for(hints.get(pname, Any))
            if param.default is inspect.Parameter.empty:
                required.append(pname)
        doc = inspect.getdoc(func) or ""
        return Tool(
            name=name or func.__name__,
            description=description or doc.split("\n\n")[0].replace("\n", " ").strip(),
            parameters={"type": "object", "properties": props, "required": required},
            fn=func,
        )

    return build(fn) if fn is not None else build


def register_tools(
    app: App,
    tools: Sequence[Tool],
    *,
    retry: RetryPolicy | None = None,
    timeout: float | None = None,
) -> None:
    """Register each tool as activity ``tool.<name>``."""
    for t in tools:

        def run(arguments: dict[str, Any], _tool: Tool = t) -> Any:
            return _tool.invoke(arguments)

        run.__name__ = f"tool_{t.name}"
        app.register_activity(
            run, name=t.activity_name, options=ActivityOptions(retry=retry, timeout=timeout)
        )


@dataclass
class AgentResult:
    messages: list[Message]
    final: str = ""
    turns: int = 0
    tool_calls: int = 0
    finished: bool = False
    tool_log: list[dict[str, Any]] = field(default_factory=list)


def _tool_message(call: ToolCall, content: str) -> Message:
    return Message("tool", content, tool_call_id=call.id, name=call.name)


def run_tool_loops(
    ctx: WorkflowContext,
    conversations: Sequence[Sequence[Message]],
    tools: Sequence[Tool],
    *,
    max_turns: int = 4,
    model: str | None = None,
    temperature: float = 0.0,
    llm_activity: str = LLM_ACTIVITY,
) -> list[AgentResult]:
    """Run tool-calling loops for several conversations in lockstep.

    The last turn offers no tools, which forces a final answer. A tool failure (after
    its retries) is reported back to the model as the tool's output instead of failing
    the workflow; unknown tool names are answered the same way without an activity.
    """
    if max_turns < 1:
        raise ValueError("max_turns must be >= 1")
    by_name = {t.name: t for t in tools}
    specs = [t.spec for t in tools]
    states = [AgentResult(messages=list(conv)) for conv in conversations]
    for turn in range(max_turns):
        active = [s for s in states if not s.finished]
        if not active:
            break
        offer = specs if turn < max_turns - 1 else []
        chats = [
            start_chat(
                ctx,
                s.messages,
                tools=offer,
                model=model,
                temperature=temperature,
                activity=llm_activity,
            )
            for s in active
        ]
        calls: list[tuple[AgentResult, ToolCall, ActivityHandle | None]] = []
        for state, handle in zip(active, chats, strict=True):
            reply = response_of(handle).message
            state.messages.append(reply)
            state.turns += 1
            if not reply.tool_calls:
                state.finished = True
                state.final = reply.content
                continue
            for call in reply.tool_calls:
                known = by_name.get(call.name)
                act = (
                    None
                    if known is None
                    else ctx.start_activity(known.activity_name, call.arguments)
                )
                calls.append((state, call, act))
        for state, call, act in calls:
            state.tool_calls += 1
            if act is None:
                content = f"error: unknown tool {call.name!r}; available: {sorted(by_name)}"
            else:
                try:
                    out = act.result()
                    content = out if isinstance(out, str) else serde.dumps(out)
                except ActivityError as exc:
                    content = f"error: {exc.error_type}: {exc.error_message}"
            state.tool_log.append({"tool": call.name, "arguments": call.arguments})
            state.messages.append(_tool_message(call, content))
    for state in states:
        if not state.finished:
            last = next((m for m in reversed(state.messages) if m.role == "assistant"), None)
            state.final = last.content if last else ""
    return states


def run_tool_loop(
    ctx: WorkflowContext,
    messages: Sequence[Message],
    tools: Sequence[Tool],
    **kwargs: Any,
) -> AgentResult:
    """Single-conversation form of :func:`run_tool_loops`."""
    return run_tool_loops(ctx, [messages], tools, **kwargs)[0]
