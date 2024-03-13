from __future__ import annotations

import json
import socket
import threading
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, ClassVar

import pytest

from replayagent import App, ApplicationError, RetryPolicy, WorkflowContext, workflow
from replayagent.llm import (
    FakeProvider,
    LLMRequest,
    LLMResponse,
    LLMTransientError,
    Message,
    OllamaProvider,
    ToolCall,
    ToolSpec,
    chat,
    register_llm,
    system,
    user,
)
from tests.conftest import Env

EnvFactory = Callable[..., Env]


def test_types_roundtrip() -> None:
    req = LLMRequest(
        messages=(
            system("s"),
            Message("assistant", "", tool_calls=(ToolCall("c1", "t", {"a": 1}),)),
            Message("tool", "out", tool_call_id="c1", name="t"),
        ),
        model="m",
        tools=(ToolSpec("t", "does t", {"type": "object", "properties": {}}),),
        temperature=0.2,
        max_tokens=10,
        json_mode=True,
    )
    assert LLMRequest.from_dict(json.loads(json.dumps(req.to_dict()))) == req
    resp = LLMResponse(Message("assistant", "hi"), "m")
    assert LLMResponse.from_dict(resp.to_dict()) == resp
    assert resp.text == "hi"
    with pytest.raises(ValueError):
        Message.from_dict({"role": "robot"})


def test_fake_provider_fails_then_answers_and_logs(tmp_path: Path) -> None:
    log = tmp_path / "calls.jsonl"
    provider = FakeProvider(lambda r: "pong", fail_first=1, log_path=log)
    req = LLMRequest((user("ping"),))
    with pytest.raises(LLMTransientError):
        provider.complete(req)
    out = provider.complete(req)
    assert out.text == "pong" and out.usage.completion_tokens >= 1
    assert [json.loads(line)["failed"] for line in log.read_text().splitlines()] == [True, False]
    assert len(provider.calls) == 2
    full = LLMResponse(Message("assistant", "x"), "custom")
    assert FakeProvider(lambda r: full).complete(req) is full


@workflow
def ask(ctx: WorkflowContext, question: str) -> dict[str, Any]:
    first = chat(ctx, [user(question)])
    second = chat(ctx, [user(question), first.message, user("again")])
    return {"first": first.text, "second": second.text, "model": second.model}


def test_chat_is_durable_and_retried(make_env: EnvFactory) -> None:
    provider = FakeProvider(lambda r: f"answer#{len(r.messages)}", fail_first=2)
    app = App(workflows=[ask])
    register_llm(app, provider, retry=RetryPolicy(initial_interval=0.1, maximum_attempts=5))
    env = make_env(app)
    wid = env.client.start(ask, "why?")
    env.run()
    assert env.client.result(wid) == {"first": "answer#1", "second": "answer#3", "model": "fake-1"}
    assert len(provider.calls) == 4  # 2 transient failures + 2 answers
    env.client.resume(wid)  # no-op for a completed workflow
    env.run()
    assert len(provider.calls) == 4  # replay/inspection never calls the model


# -- Ollama provider against a local stub server -----------------------------------------------


class _Stub(BaseHTTPRequestHandler):
    queue: ClassVar[list[tuple[int, bytes]]] = []
    received: ClassVar[list[dict[str, Any]]] = []

    def do_POST(self) -> None:
        length = int(self.headers["Content-Length"])
        _Stub.received.append(json.loads(self.rfile.read(length)))
        status, body = _Stub.queue.pop(0)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:
        pass


@pytest.fixture
def stub() -> Iterator[str]:
    _Stub.queue, _Stub.received = [], []
    server = HTTPServer(("127.0.0.1", 0), _Stub)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()
    thread.join()


def test_ollama_success_with_tool_calls(stub: str) -> None:
    body = {
        "model": "llama3.1:8b",
        "message": {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {"function": {"name": "search", "arguments": {"q": "x"}}},
                {"function": {"name": "read", "arguments": '{"id": "d1"}'}},
                {"function": {"name": "odd", "arguments": "not json"}},
            ],
        },
        "prompt_eval_count": 12,
        "eval_count": 3,
        "done_reason": "stop",
    }
    _Stub.queue.append((200, json.dumps(body).encode()))
    provider = OllamaProvider(host=stub, model="llama3.1:8b")
    req = LLMRequest(
        (
            system("s"),
            Message("assistant", "", tool_calls=(ToolCall("c0", "search", {"q": "x"}),)),
            Message("tool", "result", tool_call_id="c0", name="search"),
        ),
        tools=(ToolSpec("search", "find", {"type": "object"}),),
        max_tokens=50,
        json_mode=True,
    )
    resp = provider.complete(req)
    calls = resp.message.tool_calls
    assert [c.id for c in calls] == ["call_0", "call_1", "call_2"]
    assert calls[1].arguments == {"id": "d1"} and calls[2].arguments == {"_raw": "not json"}
    assert (resp.usage.prompt_tokens, resp.usage.completion_tokens) == (12, 3)
    sent = _Stub.received[0]
    assert sent["format"] == "json" and sent["options"]["num_predict"] == 50
    assert sent["messages"][2]["tool_name"] == "search"
    assert sent["tools"][0]["function"]["name"] == "search"
    assert sent["stream"] is False


def test_ollama_error_mapping(stub: str) -> None:
    provider = OllamaProvider(host=f"http://{stub}", timeout=5)
    req = LLMRequest((user("hi"),))
    _Stub.queue.append((404, b'{"error":"model not found"}'))
    with pytest.raises(ApplicationError) as info:
        provider.complete(req)
    assert info.value.non_retryable and info.value.type == "LLMRequestError"
    _Stub.queue.append((503, b"busy"))
    with pytest.raises(LLMTransientError, match="503"):
        provider.complete(req)
    _Stub.queue.append((200, b"{not json"))
    with pytest.raises(LLMTransientError, match="invalid JSON"):
        provider.complete(req)


def test_ollama_unreachable_is_transient() -> None:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    provider = OllamaProvider(host=f"127.0.0.1:{port}", timeout=2)
    with pytest.raises(LLMTransientError, match="unreachable"):
        provider.complete(LLMRequest((user("hi"),)))


def test_ollama_host_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OLLAMA_HOST", "localhost:9999")
    assert OllamaProvider().host == "http://localhost:9999"
