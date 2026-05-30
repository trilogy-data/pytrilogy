from dataclasses import dataclass

import pytest

from trilogy.ai.models import LLMMessage, LLMRequestOptions, LLMToolDefinition
from trilogy.ai.providers.anthropic import AnthropicProvider
from trilogy.ai.providers.base import parse_tool_arguments
from trilogy.ai.providers.deepseek import DeepSeekProvider
from trilogy.ai.providers.google import GoogleProvider
from trilogy.ai.providers.openai import OpenAIProvider
from trilogy.ai.providers.openrouter import OpenRouterProvider
from trilogy.ai.providers.utils import RetryOptions


@dataclass
class _FakeResponse:
    payload: dict
    status_code: int = 200
    text: str = ""

    def raise_for_status(self):
        if self.status_code >= 400:
            import httpx

            response = httpx.Response(
                status_code=self.status_code,
                request=httpx.Request("POST", "http://test.local"),
                content=self.text.encode(),
            )
            raise httpx.HTTPStatusError(
                "error", request=response.request, response=response
            )

    def json(self):
        return self.payload


class _FakeClient:
    def __init__(self, response_payload: dict, sink: dict):
        self._response_payload = response_payload
        self._sink = sink

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, url, headers, json):
        self._sink["url"] = url
        self._sink["headers"] = headers
        self._sink["json"] = json
        return _FakeResponse(self._response_payload)


def _tool_def() -> LLMToolDefinition:
    return LLMToolDefinition(
        name="submit_query",
        description="Return final query",
        input_schema={
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "meta": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {"x": {"type": "number"}},
                },
            },
            "required": ["query"],
            "additionalProperties": False,
        },
    )


def test_parse_tool_arguments_variants():
    assert parse_tool_arguments(None) == {}
    assert parse_tool_arguments({"a": 1}) == {"a": 1}
    assert parse_tool_arguments("  ") == {}
    assert parse_tool_arguments('{"query":"select 1"}') == {"query": "select 1"}
    with pytest.raises(ValueError):
        parse_tool_arguments("[]")


def test_parse_tool_arguments_tolerates_raw_newlines_in_strings():
    """Models emit multi-line `--content` bodies with real newlines instead of
    escaped `\\n`; strict JSON rejects that, but the tool call is otherwise
    valid, so we parse with strict=False."""
    raw = (
        '{"args": ["file", "write", "q.preql", "--content", "import x;\n\nselect 1;"]}'
    )
    parsed = parse_tool_arguments(raw)
    assert parsed["args"][-1] == "import x;\n\nselect 1;"


def test_build_tool_call_parses_valid_arguments():
    from trilogy.ai.providers.base import build_tool_call

    call = build_tool_call("todo", '{"action":"add"}')
    assert call.arguments == {"action": "add"}
    assert call.parse_error is None


def test_build_tool_call_tolerates_malformed_json():
    from trilogy.ai.providers.base import build_tool_call

    call = build_tool_call("todo", '{"action":"add"')  # truncated JSON
    assert call.arguments == {}
    assert call.parse_error is not None
    assert "invalid tool arguments" in call.parse_error


def _tool_history():
    from trilogy.ai.models import LLMMessage

    return [
        LLMMessage(role="system", content="sys"),
        LLMMessage(role="user", content="do it"),
        LLMMessage(
            role="assistant",
            content="",
            model_info={
                "tool_calls": [{"name": "todo", "arguments": {"action": "list"}}]
            },
        ),
        LLMMessage(role="user", content="todo result"),
    ]


def test_to_openai_messages_threads_tool_calls():
    from trilogy.ai.providers.base import to_openai_messages

    msgs = to_openai_messages(_tool_history())
    assert [m["role"] for m in msgs] == ["system", "user", "assistant", "tool"]
    call = msgs[2]["tool_calls"][0]
    assert call["function"]["name"] == "todo"
    assert call["function"]["arguments"] == '{"action": "list"}'
    assert msgs[3]["tool_call_id"] == call["id"]
    assert msgs[3]["content"] == "todo result"


def test_to_openai_messages_plain_when_no_tool_calls():
    from trilogy.ai.models import LLMMessage
    from trilogy.ai.providers.base import to_openai_messages

    history = [
        LLMMessage(role="user", content="hi"),
        LLMMessage(role="assistant", content="hello"),
    ]
    assert to_openai_messages(history) == [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "hello"},
    ]


def test_to_anthropic_messages_threads_tool_calls():
    from trilogy.ai.providers.anthropic import _to_anthropic_messages

    msgs = _to_anthropic_messages(_tool_history())
    assert [m["role"] for m in msgs] == ["user", "assistant", "user"]
    use = msgs[1]["content"][0]
    assert use["type"] == "tool_use" and use["name"] == "todo"
    result = msgs[2]["content"][0]
    assert result["type"] == "tool_result"
    assert result["tool_use_id"] == use["id"]
    assert result["content"] == "todo result"


def test_gemini_history_threads_tool_calls():
    from trilogy.ai.providers.google import GoogleProvider

    provider = GoogleProvider(name="t", model="m", api_key="fake-key")
    contents = provider._convert_to_gemini_history(_tool_history())
    assert [c["role"] for c in contents] == ["user", "model", "user"]
    fc = contents[1]["parts"][0]["functionCall"]
    assert fc["name"] == "todo" and fc["args"] == {"action": "list"}
    fr = contents[2]["parts"][0]["functionResponse"]
    assert fr["name"] == "todo" and fr["response"] == {"result": "todo result"}


def test_openai_provider_builds_required_tool_payload(monkeypatch):
    import httpx

    sink: dict = {}
    response_payload = {
        "choices": [
            {
                "message": {
                    "content": "",
                    "tool_calls": [
                        {
                            "function": {
                                "name": "submit_query",
                                "arguments": '{"query":"select 1"}',
                            }
                        }
                    ],
                }
            }
        ],
        "usage": {
            "prompt_tokens": 1,
            "completion_tokens": 2,
            "total_tokens": 3,
        },
    }

    monkeypatch.setattr(
        httpx,
        "Client",
        lambda timeout: _FakeClient(response_payload=response_payload, sink=sink),
    )
    provider = OpenAIProvider(name="openai", model="gpt-test", api_key="x")
    result = provider.generate_completion(
        LLMRequestOptions(tools=[_tool_def()], require_tool=True),
        [LLMMessage(role="user", content="hi")],
    )

    assert sink["json"]["tool_choice"] == "required"
    assert result.tool_calls[0].arguments["query"] == "select 1"


def test_openai_provider_prefers_named_tool_choice(monkeypatch):
    import httpx

    sink: dict = {}
    response_payload = {
        "choices": [{"message": {"content": "ok", "tool_calls": []}}],
        "usage": {
            "prompt_tokens": 1,
            "completion_tokens": 1,
            "total_tokens": 2,
        },
    }
    monkeypatch.setattr(
        httpx,
        "Client",
        lambda timeout: _FakeClient(response_payload=response_payload, sink=sink),
    )
    provider = OpenAIProvider(name="openai", model="gpt-test", api_key="x")
    provider.generate_completion(
        LLMRequestOptions(tools=[_tool_def()], tool_choice="submit_query"),
        [LLMMessage(role="user", content="hi")],
    )

    assert sink["json"]["tool_choice"]["function"]["name"] == "submit_query"


def test_to_anthropic_messages_includes_assistant_text_with_tool_calls():
    """Assistant content alongside tool_calls produces a leading text block."""
    from trilogy.ai.providers.anthropic import _to_anthropic_messages

    history = [
        LLMMessage(role="user", content="hi"),
        LLMMessage(
            role="assistant",
            content="reasoning",
            model_info={"tool_calls": [{"name": "todo", "arguments": {}}]},
        ),
        LLMMessage(role="user", content="todo result"),
    ]
    msgs = _to_anthropic_messages(history)
    blocks = msgs[1]["content"]
    assert blocks[0] == {"type": "text", "text": "reasoning"}
    assert blocks[1]["type"] == "tool_use"


def test_load_openrouter_provider_routing_from_env(monkeypatch):
    from trilogy.ai.providers.openrouter import _load_provider_routing

    monkeypatch.delenv("OPENROUTER_PROVIDER", raising=False)
    assert _load_provider_routing() is None

    monkeypatch.setenv("OPENROUTER_PROVIDER", '{"ignore": ["AtlasCloud"]}')
    assert _load_provider_routing() == {"ignore": ["AtlasCloud"]}

    monkeypatch.setenv("OPENROUTER_PROVIDER", "not-json")
    with pytest.raises(ValueError, match="valid JSON"):
        _load_provider_routing()

    monkeypatch.setenv("OPENROUTER_PROVIDER", '["array"]')
    with pytest.raises(ValueError, match="JSON object"):
        _load_provider_routing()


def test_openrouter_provider_forwards_provider_routing(monkeypatch):
    import httpx

    sink: dict = {}
    response_payload = {
        "choices": [{"message": {"content": "ok", "tool_calls": []}}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }
    monkeypatch.setattr(
        httpx,
        "Client",
        lambda timeout: _FakeClient(response_payload=response_payload, sink=sink),
    )
    provider = OpenRouterProvider(
        name="openrouter",
        model="r1",
        api_key="x",
        provider_routing={"ignore": ["AtlasCloud"]},
    )
    provider.generate_completion(
        LLMRequestOptions(),
        [LLMMessage(role="user", content="hi")],
    )
    assert sink["json"]["provider"] == {"ignore": ["AtlasCloud"]}


def _openrouter_response_with_tool_args(args_json: str) -> dict:
    """Build an OpenRouter response payload that ships one tool call with
    the given (JSON-string) arguments."""
    return {
        "choices": [
            {
                "message": {
                    "content": "",
                    "tool_calls": [
                        {
                            "function": {
                                "name": "write_file",
                                "arguments": args_json,
                            }
                        }
                    ],
                }
            }
        ],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }


def test_openrouter_sanitize_html_escapes_decodes_tool_args(monkeypatch):
    """Flag on → HTML entities in tool arguments decode to literals."""
    import httpx

    sink: dict = {}
    payload = _openrouter_response_with_tool_args(
        '{"path": "q.preql", "content": "auto x &lt;- count(id);"}'
    )
    monkeypatch.setattr(
        httpx,
        "Client",
        lambda timeout: _FakeClient(response_payload=payload, sink=sink),
    )
    provider = OpenRouterProvider(
        name="openrouter", model="r1", api_key="x", sanitize_html_escapes=True
    )
    response = provider.generate_completion(
        LLMRequestOptions(),
        [LLMMessage(role="user", content="hi")],
    )
    assert response.tool_calls[0].arguments["content"] == "auto x <- count(id);"


def test_openrouter_sanitize_html_escapes_off_keeps_raw_entities(monkeypatch):
    """Flag off (default) → arguments ship verbatim, entities and all."""
    import httpx

    sink: dict = {}
    payload = _openrouter_response_with_tool_args(
        '{"path": "q.preql", "content": "auto x &lt;- count(id);"}'
    )
    monkeypatch.setattr(
        httpx,
        "Client",
        lambda timeout: _FakeClient(response_payload=payload, sink=sink),
    )
    provider = OpenRouterProvider(
        name="openrouter", model="r1", api_key="x", sanitize_html_escapes=False
    )
    response = provider.generate_completion(
        LLMRequestOptions(),
        [LLMMessage(role="user", content="hi")],
    )
    assert response.tool_calls[0].arguments["content"] == "auto x &lt;- count(id);"


def test_openrouter_sanitize_html_escapes_env_var_opt_in(monkeypatch):
    """Setting OPENROUTER_SANITIZE_HTML_ESCAPES=true flips the default on."""
    import httpx

    monkeypatch.setenv("OPENROUTER_SANITIZE_HTML_ESCAPES", "true")
    sink: dict = {}
    payload = _openrouter_response_with_tool_args('{"content": "x &amp;&lt;-"}')
    monkeypatch.setattr(
        httpx,
        "Client",
        lambda timeout: _FakeClient(response_payload=payload, sink=sink),
    )
    provider = OpenRouterProvider(name="openrouter", model="r1", api_key="x")
    response = provider.generate_completion(
        LLMRequestOptions(),
        [LLMMessage(role="user", content="hi")],
    )
    # `&amp;&lt;-` → `&<-`, not `<-`: html.unescape preserves the ampersand
    # the model intended to escape (vs. naïve replace order).
    assert response.tool_calls[0].arguments["content"] == "x &<-"


def test_openrouter_provider_prefers_named_tool_choice(monkeypatch):
    import httpx

    sink: dict = {}
    response_payload = {
        "choices": [{"message": {"content": "ok", "tool_calls": []}}],
        "usage": {
            "prompt_tokens": 1,
            "completion_tokens": 1,
            "total_tokens": 2,
        },
    }
    monkeypatch.setattr(
        httpx,
        "Client",
        lambda timeout: _FakeClient(response_payload=response_payload, sink=sink),
    )
    provider = OpenRouterProvider(name="openrouter", model="r1", api_key="x")
    provider.generate_completion(
        LLMRequestOptions(tools=[_tool_def()], tool_choice="submit_query"),
        [LLMMessage(role="user", content="hi")],
    )

    assert sink["json"]["tool_choice"]["function"]["name"] == "submit_query"


@pytest.mark.parametrize(
    "provider_cls, env_name",
    [
        (OpenAIProvider, "OPENAI_API_KEY"),
        (OpenRouterProvider, "OPENROUTER_API_KEY"),
        (AnthropicProvider, "ANTHROPIC_API_KEY"),
        (GoogleProvider, "GOOGLE_API_KEY"),
        (DeepSeekProvider, "DEEPSEEK_API_KEY"),
    ],
)
def test_provider_requires_api_key(monkeypatch, provider_cls, env_name):
    monkeypatch.delenv(env_name, raising=False)
    with pytest.raises(ValueError):
        provider_cls(name="p", model="m")


def test_deepseek_points_at_deepseek_endpoint(monkeypatch):
    """DeepSeek provider hits api.deepseek.com directly, not OpenAI's URL —
    inheriting from OpenAIProvider but overriding base_completion_url."""
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-test")
    p = DeepSeekProvider(name="ds", model="deepseek-v4-flash")
    assert p.base_completion_url == "https://api.deepseek.com/v1/chat/completions"
    assert "openai.com" not in p.base_completion_url
    from trilogy.ai.enums import Provider

    assert p.type == Provider.DEEPSEEK


def test_deepseek_posts_to_deepseek_url(monkeypatch):
    """Round-trip: a generate_completion call lands at the DeepSeek URL with
    the DEEPSEEK_API_KEY in the Authorization header, not OpenAI's URL/key."""
    import httpx

    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-deepseek-test")
    sink: dict = {}
    response_payload = {
        "choices": [{"message": {"content": "ok", "tool_calls": []}}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }
    monkeypatch.setattr(
        httpx,
        "Client",
        lambda timeout: _FakeClient(response_payload=response_payload, sink=sink),
    )
    provider = DeepSeekProvider(name="ds", model="deepseek-v4-flash")
    provider.generate_completion(
        LLMRequestOptions(), [LLMMessage(role="user", content="hi")]
    )
    assert sink["url"] == "https://api.deepseek.com/v1/chat/completions"
    assert sink["headers"]["Authorization"] == "Bearer sk-deepseek-test"
    assert sink["json"]["model"] == "deepseek-v4-flash"


@pytest.mark.parametrize(
    "provider_cls, model",
    [
        (OpenAIProvider, "gpt-test"),
        (OpenRouterProvider, "r-test"),
        (AnthropicProvider, "claude-test"),
        (GoogleProvider, "gemini-test"),
    ],
)
def test_provider_http_status_error_is_wrapped(monkeypatch, provider_cls, model):
    import httpx

    class _ErrorClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, url, headers, json):
            return _FakeResponse(payload={}, status_code=429, text='{"error":"quota"}')

    monkeypatch.setattr(httpx, "Client", lambda timeout: _ErrorClient())

    provider = provider_cls(
        name="p",
        model=model,
        api_key="x",
        retry_options=RetryOptions(max_retries=0, initial_delay_ms=1),
    )
    with pytest.raises(Exception, match="API error"):
        provider.generate_completion(
            LLMRequestOptions(),
            [LLMMessage(role="user", content="hi")],
        )


def test_anthropic_provider_sets_tool_choice_any_and_parses_tool_use(monkeypatch):
    import httpx

    sink: dict = {}
    response_payload = {
        "content": [
            {"type": "text", "text": "thinking"},
            {
                "type": "tool_use",
                "name": "submit_query",
                "input": {"query": "select 2"},
            },
        ],
        "usage": {"input_tokens": 3, "output_tokens": 4},
    }
    monkeypatch.setattr(
        httpx,
        "Client",
        lambda timeout: _FakeClient(response_payload=response_payload, sink=sink),
    )
    provider = AnthropicProvider(name="anthropic", model="claude", api_key="x")
    result = provider.generate_completion(
        LLMRequestOptions(tools=[_tool_def()], require_tool=True),
        [
            LLMMessage(role="system", content="sys"),
            LLMMessage(role="user", content="q"),
        ],
    )

    assert sink["json"]["tool_choice"] == {"type": "any"}
    assert sink["json"]["system"] == "sys"
    assert result.tool_calls[0].name == "submit_query"


def test_google_provider_converts_schema_and_parses_function_call(monkeypatch):
    import httpx

    sink: dict = {}
    response_payload = {
        "candidates": [
            {
                "content": {
                    "parts": [
                        {"text": "done"},
                        {
                            "functionCall": {
                                "name": "submit_query",
                                "args": {"query": "select 3"},
                            }
                        },
                    ]
                }
            }
        ],
        "usageMetadata": {"promptTokenCount": 5, "candidatesTokenCount": 6},
    }
    monkeypatch.setattr(
        httpx,
        "Client",
        lambda timeout: _FakeClient(response_payload=response_payload, sink=sink),
    )
    provider = GoogleProvider(name="google", model="gemini-test", api_key="x")
    result = provider.generate_completion(
        LLMRequestOptions(tools=[_tool_def()], require_tool=True),
        [
            LLMMessage(role="system", content="sys"),
            LLMMessage(role="user", content="q"),
        ],
    )

    schema = sink["json"]["tools"][0]["functionDeclarations"][0]["parameters"]
    assert schema["type"] == "OBJECT"
    assert "additionalProperties" not in schema
    assert sink["json"]["toolConfig"]["functionCallingConfig"]["mode"] == "ANY"
    assert result.tool_calls[0].arguments["query"] == "select 3"


def test_google_provider_named_tool_choice(monkeypatch):
    import httpx

    sink: dict = {}
    response_payload = {
        "candidates": [{"content": {"parts": [{"text": "ok"}]}}],
        "usageMetadata": {"promptTokenCount": 1, "candidatesTokenCount": 1},
    }
    monkeypatch.setattr(
        httpx,
        "Client",
        lambda timeout: _FakeClient(response_payload=response_payload, sink=sink),
    )
    provider = GoogleProvider(name="google", model="gemini-test", api_key="x")
    provider.generate_completion(
        LLMRequestOptions(tools=[_tool_def()], tool_choice="submit_query"),
        [LLMMessage(role="user", content="q")],
    )

    cfg = sink["json"]["toolConfig"]["functionCallingConfig"]
    assert cfg["allowedFunctionNames"] == ["submit_query"]


# --- OpenRouter defensive body handling ---


def test_interpret_body_accepts_usable_response():
    from trilogy.ai.providers.openrouter import _interpret_body

    data, problem = _interpret_body(
        {"choices": [{"message": {"content": "hi"}}], "usage": {}}
    )
    assert problem is None
    assert data == {"choices": [{"message": {"content": "hi"}}], "usage": {}}


def test_interpret_body_flags_error_envelope():
    from trilogy.ai.providers.openrouter import _interpret_body

    data, problem = _interpret_body(
        {"error": {"code": 503, "message": "upstream provider unavailable"}}
    )
    assert data is None
    assert "code=503" in problem
    assert "upstream provider unavailable" in problem


def test_interpret_body_flags_missing_choices():
    from trilogy.ai.providers.openrouter import _interpret_body

    data, problem = _interpret_body({"id": "x"})
    assert data is None
    assert "missing 'choices'" in problem


def test_interpret_body_flags_non_dict_body():
    from trilogy.ai.providers.openrouter import _interpret_body

    data, problem = _interpret_body([])
    assert data is None
    assert "non-dict" in problem


def test_openrouter_missing_usage_falls_back_to_zero(monkeypatch):
    """A 200-OK response without ``usage`` is still usable; the agent loop
    must not die just because token counts are missing."""
    import httpx

    sink: dict = {}
    payload = {
        "choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}],
        # No "usage" — the q02 enriched crash repro.
    }
    monkeypatch.setattr(
        httpx,
        "Client",
        lambda timeout: _FakeClient(response_payload=payload, sink=sink),
    )
    provider = OpenRouterProvider(
        name="openrouter", model="r1", api_key="x", sanitize_html_escapes=False
    )
    response = provider.generate_completion(
        LLMRequestOptions(),
        [LLMMessage(role="user", content="hi")],
    )
    assert response.text == "ok"
    assert response.usage.prompt_tokens == 0
    assert response.usage.completion_tokens == 0
    assert response.usage.total_tokens == 0


class _SequenceClient:
    """``_FakeClient`` variant that returns a different payload on each call,
    so a test can simulate "first response is an error envelope, retry
    succeeds"."""

    def __init__(self, payloads):
        self._payloads = list(payloads)
        self.calls = 0

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def post(self, url, headers, json):
        self.calls += 1
        return _FakeResponse(self._payloads.pop(0))


def test_openrouter_retries_through_error_envelope(monkeypatch):
    """An upstream error envelope is retried, not immediately raised — and
    once we get a usable body the call returns normally."""
    import httpx

    monkeypatch.setattr("trilogy.ai.providers.openrouter.time.sleep", lambda _s: None)
    seq = _SequenceClient(
        [
            {"error": {"code": 502, "message": "upstream hiccup"}},
            {
                "choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}],
                "usage": {
                    "prompt_tokens": 5,
                    "completion_tokens": 1,
                    "total_tokens": 6,
                },
            },
        ]
    )
    monkeypatch.setattr(httpx, "Client", lambda timeout: seq)
    provider = OpenRouterProvider(
        name="openrouter", model="r1", api_key="x", sanitize_html_escapes=False
    )
    response = provider.generate_completion(
        LLMRequestOptions(), [LLMMessage(role="user", content="hi")]
    )
    assert response.text == "ok"
    assert seq.calls == 2


class _TransportErrorThenOkClient:
    """Raise the configured transport error on the first N calls, then return
    a usable payload — proves the retry layer absorbs socket-level failures
    rather than letting them escape the provider."""

    def __init__(self, error_factory, fail_times: int, ok_payload: dict):
        self._error_factory = error_factory
        self._fail_times = fail_times
        self._ok_payload = ok_payload
        self.calls = 0

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def post(self, url, headers, json):
        self.calls += 1
        if self.calls <= self._fail_times:
            raise self._error_factory()
        return _FakeResponse(self._ok_payload)


@pytest.mark.parametrize(
    "error_factory_name",
    ["ReadError", "ConnectError", "RemoteProtocolError", "ReadTimeout", "PoolTimeout"],
)
def test_openrouter_retries_transport_errors(monkeypatch, error_factory_name):
    """Transient socket-level failures (connection closed mid-stream, DNS
    blip, remote protocol error) must be retried, not bubbled. The eval
    agent died on a real ``ReadError`` mid-run before this fix."""
    import httpx

    monkeypatch.setattr("trilogy.ai.providers.openrouter.time.sleep", lambda _s: None)
    monkeypatch.setattr("trilogy.ai.providers.utils.time.sleep", lambda _s: None)
    err_cls = getattr(httpx, error_factory_name)
    ok_payload = {
        "choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }
    client = _TransportErrorThenOkClient(
        error_factory=lambda: err_cls("forcibly closed"),
        fail_times=2,
        ok_payload=ok_payload,
    )
    monkeypatch.setattr(httpx, "Client", lambda timeout: client)
    provider = OpenRouterProvider(
        name="openrouter",
        model="r1",
        api_key="x",
        sanitize_html_escapes=False,
        retry_options=RetryOptions(max_retries=3, initial_delay_ms=1),
    )
    response = provider.generate_completion(
        LLMRequestOptions(), [LLMMessage(role="user", content="hi")]
    )
    assert response.text == "ok"
    assert client.calls == 3


def test_openrouter_gives_up_on_persistent_transport_error(monkeypatch):
    """If the transport error never clears, the provider still raises — but
    only after exhausting the retry budget."""
    import httpx

    monkeypatch.setattr("trilogy.ai.providers.openrouter.time.sleep", lambda _s: None)
    monkeypatch.setattr("trilogy.ai.providers.utils.time.sleep", lambda _s: None)
    client = _TransportErrorThenOkClient(
        error_factory=lambda: httpx.ReadError("forcibly closed"),
        fail_times=99,
        ok_payload={},
    )
    monkeypatch.setattr(httpx, "Client", lambda timeout: client)
    provider = OpenRouterProvider(
        name="openrouter",
        model="r1",
        api_key="x",
        sanitize_html_escapes=False,
        retry_options=RetryOptions(max_retries=2, initial_delay_ms=1),
    )
    with pytest.raises(Exception, match="OpenRouter API error"):
        provider.generate_completion(
            LLMRequestOptions(), [LLMMessage(role="user", content="hi")]
        )
    assert client.calls == 3  # initial attempt + 2 retries


def test_openrouter_gives_up_with_structured_error(monkeypatch):
    """After exhausting body-retry attempts on a persistent error envelope,
    the exception message preserves the upstream code + message so the
    operator sees the real cause in logs (vs. ``'usage'``)."""
    import httpx

    monkeypatch.setattr("trilogy.ai.providers.openrouter.time.sleep", lambda _s: None)
    err_payload = {"error": {"code": 503, "message": "rate limit on upstream provider"}}
    seq = _SequenceClient([err_payload, err_payload, err_payload])
    monkeypatch.setattr(httpx, "Client", lambda timeout: seq)
    provider = OpenRouterProvider(
        name="openrouter", model="r1", api_key="x", sanitize_html_escapes=False
    )
    with pytest.raises(Exception) as excinfo:
        provider.generate_completion(
            LLMRequestOptions(), [LLMMessage(role="user", content="hi")]
        )
    msg = str(excinfo.value)
    assert "code=503" in msg
    assert "rate limit on upstream provider" in msg
