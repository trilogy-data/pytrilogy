import json
from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any

from trilogy.ai.enums import Provider
from trilogy.ai.models import (
    LLMMessage,
    LLMRequestOptions,
    LLMResponse,
    LLMToolCall,
)

RETRYABLE_CODES = [429, 500, 502, 503, 504]


class LLMProvider(ABC):
    def __init__(self, name: str, api_key: str, model: str, provider: Provider):
        self.api_key = api_key
        self.models: list[str] = []
        self.name = name
        self.model = model
        self.type = provider
        self.error: str | None = None

    # Abstract method to be implemented by specific providers
    @abstractmethod
    def generate_completion(
        self, options: LLMRequestOptions, history: list[LLMMessage]
    ) -> LLMResponse:
        pass


def parse_tool_arguments(arguments: str | dict[str, Any] | None) -> dict[str, Any]:
    if arguments is None:
        return {}
    if isinstance(arguments, dict):
        return arguments
    if not arguments.strip():
        return {}
    # strict=False permits literal control characters (raw newlines/tabs) inside
    # strings — and relaxes nothing else. Models routinely emit a multi-line
    # `--content` file body with real newlines rather than escaped `\n`, which
    # strict JSON rejects ("Invalid control character"); that otherwise-valid
    # tool call was getting bounced back as a parse error and wasting a turn.
    parsed = json.loads(arguments, strict=False)
    if not isinstance(parsed, dict):
        raise ValueError(f"Tool arguments must decode to an object, got {type(parsed)}")
    return parsed


def build_tool_call(
    name: str, raw_arguments: str | dict[str, Any] | None
) -> LLMToolCall:
    """Build an LLMToolCall, tolerating malformed argument JSON. A model that
    emits invalid JSON yields a tool call carrying ``parse_error`` rather than
    raising; the agent loop surfaces that to the model so it can retry."""
    try:
        return LLMToolCall(name=name, arguments=parse_tool_arguments(raw_arguments))
    except (json.JSONDecodeError, ValueError) as exc:
        return LLMToolCall(name=name, parse_error=f"invalid tool arguments: {exc}")


def iter_history_turns(
    history: list[LLMMessage],
) -> Iterator[tuple[LLMMessage, list[dict] | None, list[LLMMessage]]]:
    """Walk conversation history yielding ``(message, tool_calls, results)``.

    An assistant turn that made tool calls (recorded in ``model_info``) yields
    its ``tool_calls`` (a list of ``{name, arguments}`` dicts) plus ``results``
    — the messages that answered them (the next ``len(tool_calls)`` messages).
    Any other message yields ``(message, None, [])``. This is the shared spine
    every provider uses to thread tool calls back into its own request format.
    """
    i = 0
    while i < len(history):
        msg = history[i]
        tool_calls = None
        if msg.role == "assistant":
            tool_calls = (msg.model_info or {}).get("tool_calls")
        if tool_calls:
            results: list[LLMMessage] = []
            for _ in range(len(tool_calls)):
                i += 1
                if i < len(history):
                    results.append(history[i])
            yield msg, tool_calls, results
            i += 1
        else:
            yield msg, None, []
            i += 1


def to_openai_messages(history: list[LLMMessage]) -> list[dict]:
    """OpenAI / OpenRouter chat-format message list, threading assistant tool
    calls back as real ``tool_calls`` and their results as ``role:"tool"``
    replies. Without this the model never sees its own prior tool calls."""
    messages: list[dict] = []
    for idx, (msg, tool_calls, results) in enumerate(iter_history_turns(history)):
        if tool_calls:
            ids = [f"call_{idx}_{j}" for j in range(len(tool_calls))]
            messages.append(
                {
                    "role": "assistant",
                    "content": msg.content or None,
                    "tool_calls": [
                        {
                            "id": ids[j],
                            "type": "function",
                            "function": {
                                "name": tc.get("name", ""),
                                "arguments": json.dumps(tc.get("arguments") or {}),
                            },
                        }
                        for j, tc in enumerate(tool_calls)
                    ],
                }
            )
            for j, res in enumerate(results):
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": ids[j],
                        "content": res.content or "",
                    }
                )
        else:
            messages.append({"role": msg.role, "content": msg.content})
    return messages
