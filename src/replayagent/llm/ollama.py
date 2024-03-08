"""Ollama chat provider (``POST /api/chat``) using only the standard library."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

from replayagent.errors import ApplicationError
from replayagent.llm.types import (
    LLMRequest,
    LLMResponse,
    LLMTransientError,
    Message,
    ToolCall,
    Usage,
)

DEFAULT_HOST = "http://127.0.0.1:11434"
DEFAULT_MODEL = "llama3.1:8b"


class OllamaProvider:
    """Talks to a local Ollama server.

    Connection errors, timeouts and 5xx responses raise :class:`LLMTransientError`
    (retried by the activity's policy); 4xx responses such as an unknown model raise a
    non-retryable :class:`~replayagent.errors.ApplicationError`.
    """

    def __init__(
        self,
        *,
        host: str | None = None,
        model: str = DEFAULT_MODEL,
        timeout: float = 300.0,
        keep_alive: str = "5m",
    ) -> None:
        self.host = (host or os.environ.get("OLLAMA_HOST") or DEFAULT_HOST).rstrip("/")
        if not self.host.startswith(("http://", "https://")):
            self.host = "http://" + self.host
        self.model = model
        self.timeout = timeout
        self.keep_alive = keep_alive

    def build_payload(self, request: LLMRequest) -> dict[str, Any]:
        messages: list[dict[str, Any]] = []
        for m in request.messages:
            item: dict[str, Any] = {"role": m.role, "content": m.content}
            if m.tool_calls:
                item["tool_calls"] = [
                    {"function": {"name": c.name, "arguments": c.arguments}} for c in m.tool_calls
                ]
            if m.role == "tool" and m.name:
                item["tool_name"] = m.name
            messages.append(item)
        options: dict[str, Any] = {"temperature": request.temperature}
        if request.max_tokens is not None:
            options["num_predict"] = request.max_tokens
        payload: dict[str, Any] = {
            "model": request.model or self.model,
            "messages": messages,
            "stream": False,
            "options": options,
            "keep_alive": self.keep_alive,
        }
        if request.tools:
            payload["tools"] = [
                {
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.parameters,
                    },
                }
                for t in request.tools
            ]
        if request.json_mode:
            payload["format"] = "json"
        return payload

    def complete(self, request: LLMRequest) -> LLMResponse:
        body = json.dumps(self.build_payload(request)).encode("utf-8")
        req = urllib.request.Request(
            f"{self.host}/api/chat",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")[:500]
            if exc.code >= 500:
                raise LLMTransientError(f"ollama HTTP {exc.code}: {detail}") from exc
            raise ApplicationError(
                f"ollama HTTP {exc.code}: {detail}", non_retryable=True, type="LLMRequestError"
            ) from exc
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            raise LLMTransientError(f"ollama unreachable at {self.host}: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise LLMTransientError(f"ollama returned invalid JSON: {exc}") from exc
        return self.parse_response(raw, request)

    def parse_response(self, raw: dict[str, Any], request: LLMRequest) -> LLMResponse:
        msg = raw.get("message") or {}
        calls = []
        for i, call in enumerate(msg.get("tool_calls") or ()):
            fn = call.get("function") or {}
            args = fn.get("arguments") or {}
            if isinstance(args, str):
                try:
                    args = json.loads(args)
                except json.JSONDecodeError:
                    args = {"_raw": args}
            calls.append(ToolCall(id=f"call_{i}", name=str(fn.get("name", "")), arguments=args))
        return LLMResponse(
            message=Message("assistant", str(msg.get("content") or ""), tool_calls=tuple(calls)),
            model=str(raw.get("model") or request.model or self.model),
            usage=Usage(int(raw.get("prompt_eval_count", 0)), int(raw.get("eval_count", 0))),
            finish_reason=str(raw.get("done_reason") or "stop"),
        )
