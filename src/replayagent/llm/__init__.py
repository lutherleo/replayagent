"""LLM calls as durable activities.

:func:`register_llm` installs a provider as the ``llm.chat`` activity; :func:`chat`
calls it from workflow code. The request and the full response are recorded in history,
so a replay after a crash never calls (or pays for) the model again.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from replayagent.llm.fake import FakeProvider
from replayagent.llm.ollama import OllamaProvider
from replayagent.llm.types import (
    LLMProvider,
    LLMRequest,
    LLMResponse,
    LLMTransientError,
    Message,
    ToolCall,
    ToolSpec,
    Usage,
    system,
    user,
)
from replayagent.registry import ActivityOptions, App
from replayagent.replay import ActivityHandle, WorkflowContext
from replayagent.retry import RetryPolicy

LLM_ACTIVITY = "llm.chat"

#: Default policy for model calls: transient errors back off 1s, 2s, 4s; bad requests
#: (``LLMRequestError``) are not retried.
LLM_RETRY = RetryPolicy(
    initial_interval=1.0,
    backoff_coefficient=2.0,
    maximum_interval=30.0,
    maximum_attempts=4,
    non_retryable_error_types=("LLMRequestError",),
)


def register_llm(
    app: App,
    provider: LLMProvider,
    *,
    name: str = LLM_ACTIVITY,
    retry: RetryPolicy = LLM_RETRY,
    timeout: float | None = None,
) -> str:
    """Register ``provider`` as an activity called ``name``."""

    def llm_chat(request: dict[str, Any]) -> dict[str, Any]:
        return provider.complete(LLMRequest.from_dict(request)).to_dict()

    llm_chat.__name__ = name.replace(".", "_")
    return app.register_activity(
        llm_chat, name=name, options=ActivityOptions(retry=retry, timeout=timeout)
    )


def start_chat(
    ctx: WorkflowContext,
    messages: Sequence[Message],
    *,
    tools: Sequence[ToolSpec] = (),
    model: str | None = None,
    temperature: float = 0.0,
    max_tokens: int | None = None,
    json_mode: bool = False,
    activity: str = LLM_ACTIVITY,
) -> ActivityHandle:
    """Schedule a model call without waiting (fan-out); decode with :func:`response_of`."""
    request = LLMRequest(
        messages=tuple(messages),
        model=model,
        tools=tuple(tools),
        temperature=temperature,
        max_tokens=max_tokens,
        json_mode=json_mode,
    )
    return ctx.start_activity(activity, request.to_dict())


def response_of(handle: ActivityHandle) -> LLMResponse:
    return LLMResponse.from_dict(handle.result())


def chat(
    ctx: WorkflowContext,
    messages: Sequence[Message],
    *,
    tools: Sequence[ToolSpec] = (),
    model: str | None = None,
    temperature: float = 0.0,
    max_tokens: int | None = None,
    json_mode: bool = False,
    activity: str = LLM_ACTIVITY,
) -> LLMResponse:
    """Call the model durably from workflow code."""
    handle = start_chat(
        ctx,
        messages,
        tools=tools,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        json_mode=json_mode,
        activity=activity,
    )
    return response_of(handle)


__all__ = [
    "LLM_ACTIVITY",
    "LLM_RETRY",
    "FakeProvider",
    "LLMProvider",
    "LLMRequest",
    "LLMResponse",
    "LLMTransientError",
    "Message",
    "OllamaProvider",
    "ToolCall",
    "ToolSpec",
    "Usage",
    "chat",
    "register_llm",
    "response_of",
    "start_chat",
    "system",
    "user",
]
