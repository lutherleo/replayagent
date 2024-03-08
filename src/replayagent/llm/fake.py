"""Deterministic in-process provider for tests and offline demos."""

from __future__ import annotations

import json
import os
import threading
from collections.abc import Callable
from pathlib import Path

from replayagent.llm.types import LLMRequest, LLMResponse, LLMTransientError, Message, Usage

Responder = Callable[[LLMRequest], LLMResponse | Message | str]


def _estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)


class FakeProvider:
    """Answers with a pure function of the request.

    ``fail_first`` makes the first *n* calls raise :class:`LLMTransientError` so retry
    paths can be exercised. ``log_path`` appends one JSON line per call, which lets
    crash tests count real provider calls across processes.
    """

    def __init__(
        self,
        responder: Responder,
        *,
        model: str = "fake-1",
        fail_first: int = 0,
        log_path: str | Path | None = None,
    ) -> None:
        self._responder = responder
        self.model = model
        self._fail_left = fail_first
        self._log_path = None if log_path is None else Path(log_path)
        self._lock = threading.Lock()
        self.calls: list[LLMRequest] = []

    def complete(self, request: LLMRequest) -> LLMResponse:
        with self._lock:
            self.calls.append(request)
            fail = self._fail_left > 0
            if fail:
                self._fail_left -= 1
        self._log(request, failed=fail)
        if fail:
            raise LLMTransientError("fake provider: injected transient failure")
        out = self._responder(request)
        if isinstance(out, LLMResponse):
            return out
        message = Message("assistant", out) if isinstance(out, str) else out
        prompt = sum(_estimate_tokens(m.content) for m in request.messages)
        return LLMResponse(
            message=message,
            model=request.model or self.model,
            usage=Usage(prompt, _estimate_tokens(message.content)),
        )

    def _log(self, request: LLMRequest, *, failed: bool) -> None:
        if self._log_path is None:
            return
        last = request.messages[-1].content if request.messages else ""
        line = json.dumps({"pid": os.getpid(), "failed": failed, "last": last[:120]})
        with self._lock, self._log_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
            fh.flush()
            os.fsync(fh.fileno())
