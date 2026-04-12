from dataclasses import dataclass

import pytest

from trilogy.ai.models import LLMMessage, LLMRequestOptions, LLMToolDefinition
from trilogy.ai.providers.anthropic import AnthropicProvider
from trilogy.ai.providers.base import parse_tool_arguments
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
    ],
)
def test_provider_requires_api_key(monkeypatch, provider_cls, env_name):
    monkeypatch.delenv(env_name, raising=False)
    with pytest.raises(ValueError):
        provider_cls(name="p", model="m")


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
