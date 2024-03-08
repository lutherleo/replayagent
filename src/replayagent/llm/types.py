"""Provider-neutral chat types. Everything converts to and from plain JSON because
requests and responses are stored in workflow history."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

Role = Literal["system", "user", "assistant", "tool"]


@dataclass(frozen=True)
class ToolCall:
    id: str
    name: str
    arguments: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "name": self.name, "arguments": self.arguments}

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> ToolCall:
        return cls(id=str(raw["id"]), name=str(raw["name"]), arguments=dict(raw["arguments"]))


@dataclass(frozen=True)
class Message:
    role: Role
    content: str = ""
    tool_calls: tuple[ToolCall, ...] = ()
    tool_call_id: str | None = None
    name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"role": self.role, "content": self.content}
        if self.tool_calls:
            out["tool_calls"] = [c.to_dict() for c in self.tool_calls]
        if self.tool_call_id is not None:
            out["tool_call_id"] = self.tool_call_id
        if self.name is not None:
            out["name"] = self.name
        return out

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> Message:
        role = raw["role"]
        if role not in ("system", "user", "assistant", "tool"):
            raise ValueError(f"unknown message role {role!r}")
        return cls(
            role=role,
            content=str(raw.get("content") or ""),
            tool_calls=tuple(ToolCall.from_dict(c) for c in raw.get("tool_calls") or ()),
            tool_call_id=raw.get("tool_call_id"),
            name=raw.get("name"),
        )


def system(content: str) -> Message:
    return Message("system", content)


def user(content: str) -> Message:
    return Message("user", content)


@dataclass(frozen=True)
class ToolSpec:
    """A tool the model may call; ``parameters`` is a JSON Schema object."""

    name: str
    description: str
    parameters: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "description": self.description, "parameters": self.parameters}

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> ToolSpec:
        return cls(
            name=str(raw["name"]),
            description=str(raw.get("description", "")),
            parameters=dict(raw.get("parameters") or {"type": "object", "properties": {}}),
        )


@dataclass(frozen=True)
class LLMRequest:
    messages: tuple[Message, ...]
    model: str | None = None
    tools: tuple[ToolSpec, ...] = ()
    temperature: float = 0.0
    max_tokens: int | None = None
    json_mode: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "messages": [m.to_dict() for m in self.messages],
            "model": self.model,
            "tools": [t.to_dict() for t in self.tools],
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "json_mode": self.json_mode,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> LLMRequest:
        return cls(
            messages=tuple(Message.from_dict(m) for m in raw["messages"]),
            model=raw.get("model"),
            tools=tuple(ToolSpec.from_dict(t) for t in raw.get("tools") or ()),
            temperature=float(raw.get("temperature", 0.0)),
            max_tokens=raw.get("max_tokens"),
            json_mode=bool(raw.get("json_mode", False)),
        )


@dataclass(frozen=True)
class Usage:
    prompt_tokens: int = 0
    completion_tokens: int = 0


@dataclass(frozen=True)
class LLMResponse:
    message: Message
    model: str
    usage: Usage = field(default_factory=Usage)
    finish_reason: str = "stop"

    @property
    def text(self) -> str:
        return self.message.content

    def to_dict(self) -> dict[str, Any]:
        return {
            "message": self.message.to_dict(),
            "model": self.model,
            "usage": {
                "prompt_tokens": self.usage.prompt_tokens,
                "completion_tokens": self.usage.completion_tokens,
            },
            "finish_reason": self.finish_reason,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> LLMResponse:
        usage = raw.get("usage") or {}
        return cls(
            message=Message.from_dict(raw["message"]),
            model=str(raw.get("model", "")),
            usage=Usage(int(usage.get("prompt_tokens", 0)), int(usage.get("completion_tokens", 0))),
            finish_reason=str(raw.get("finish_reason", "stop")),
        )


class LLMProvider(Protocol):
    """Anything that can complete a chat request."""

    def complete(self, request: LLMRequest) -> LLMResponse: ...


class LLMTransientError(Exception):
    """Retryable provider failure (connection refused, timeout, 5xx)."""
