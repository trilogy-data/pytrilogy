import json
from os import environ
from typing import Any

from trilogy.ai.enums import Provider
from trilogy.ai.models import LLMMessage, LLMResponse, UsageDict
from trilogy.constants import logger

from .base import (
    RETRYABLE_CODES,
    LLMProvider,
    LLMRequestOptions,
    ProviderError,
    build_tool_call,
    iter_history_turns,
    to_openai_messages,
)
from .utils import RetryOptions, fetch_with_retry


def to_openai_response_input(history: list[LLMMessage]) -> list[dict[str, Any]]:
    input_items: list[dict[str, Any]] = []
    for idx, (message, tool_calls, results) in enumerate(iter_history_turns(history)):
        if not tool_calls:
            input_items.append({"role": message.role, "content": message.content})
            continue

        if message.content:
            input_items.append({"role": "assistant", "content": message.content})
        for tool_idx, tool_call in enumerate(tool_calls):
            call_id = f"call_{idx}_{tool_idx}"
            input_items.append(
                {
                    "type": "function_call",
                    "call_id": call_id,
                    "name": tool_call.get("name", ""),
                    "arguments": json.dumps(tool_call.get("arguments") or {}),
                }
            )
            if tool_idx < len(results):
                input_items.append(
                    {
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": results[tool_idx].content or "",
                    }
                )
    return input_items


def _parse_response(data: dict[str, Any]) -> LLMResponse:
    text_parts: list[str] = []
    reasoning_parts: list[str] = []
    tool_calls = []
    for item in data.get("output", []):
        if item.get("type") == "function_call" and item.get("name"):
            tool_calls.append(build_tool_call(item["name"], item.get("arguments")))
        elif item.get("type") == "message":
            text_parts.extend(
                part.get("text", "")
                for part in item.get("content", [])
                if part.get("type") == "output_text"
            )
        elif item.get("type") == "reasoning":
            reasoning_parts.extend(
                part.get("text", "") for part in item.get("summary", [])
            )

    usage = data.get("usage") or {}
    output_details = usage.get("output_tokens_details") or {}
    incomplete = data.get("incomplete_details") or {}
    return LLMResponse(
        text="".join(text_parts),
        reasoning="\n".join(reasoning_parts) or None,
        tool_calls=tool_calls,
        finish_reason=(
            incomplete.get("reason") if data.get("status") == "incomplete" else None
        ),
        usage=UsageDict(
            prompt_tokens=usage.get("input_tokens", 0),
            completion_tokens=usage.get("output_tokens", 0),
            total_tokens=usage.get("total_tokens", 0),
            reasoning_tokens=output_details.get("reasoning_tokens", 0),
        ),
    )


class OpenAIProvider(LLMProvider):
    def __init__(
        self,
        name: str,
        model: str,
        api_key: str | None = None,
        retry_options: RetryOptions | None = None,
        request_timeout: float = 30.0,
    ):
        api_key = api_key or environ.get("OPENAI_API_KEY")
        if not api_key:
            raise ValueError(
                "API key argument or environment variable OPENAI_API_KEY is required"
            )
        super().__init__(name, api_key, model, Provider.OPENAI)
        self.base_completion_url = "https://api.openai.com/v1/responses"
        self.base_model_url = "https://api.openai.com/v1/models"
        self.models: list[str] = []
        self.type = Provider.OPENAI
        self.use_responses_api = True
        self.request_timeout = request_timeout

        self.retry_options = retry_options or RetryOptions(
            max_retries=3,
            initial_delay_ms=1000,
            retry_status_codes=RETRYABLE_CODES,
            on_retry=lambda attempt, delay_ms, error: logger.info(
                f"Retry attempt {attempt} after {delay_ms}ms delay due to error: {error!s}"
            ),
        )

    def generate_completion(
        self, options: LLMRequestOptions, history: list[LLMMessage]
    ) -> LLMResponse:
        if not self.use_responses_api:
            return self._generate_chat_completion(options, history)
        payload: dict[str, Any] = {
            "model": self.model,
            "input": to_openai_response_input(history),
        }
        if options.max_tokens is not None:
            payload["max_output_tokens"] = options.max_tokens
        if options.temperature is not None:
            payload["temperature"] = options.temperature
        if options.top_p is not None:
            payload["top_p"] = options.top_p
        if options.tools:
            payload["tools"] = [
                {
                    "type": "function",
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.input_schema,
                }
                for tool in options.tools
            ]
        if options.tool_choice:
            payload["tool_choice"] = {
                "type": "function",
                "name": options.tool_choice,
            }
        elif options.require_tool:
            payload["tool_choice"] = "required"
        return _parse_response(self._post(payload))

    def _post(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            import httpx
        except ImportError:
            raise ImportError(
                "Missing httpx. Install pytrilogy[ai] to use OpenAIProvider."
            )

        try:

            def make_request() -> dict[str, Any]:
                with httpx.Client(timeout=self.request_timeout) as client:
                    response = client.post(
                        url=self.base_completion_url,
                        headers={
                            "Content-Type": "application/json",
                            "Authorization": f"Bearer {self.api_key}",
                        },
                        json=payload,
                    )
                    response.raise_for_status()
                    return response.json()

            return fetch_with_retry(make_request, self.retry_options)
        except httpx.HTTPStatusError as error:
            raise ProviderError(
                f"{self._api_label} API error "
                f"({error.response.status_code}): {error.response.text}"
            ) from error
        except Exception as error:
            raise ProviderError(f"{self._api_label} API error: {error!s}") from error

    @property
    def _api_label(self) -> str:
        if self.type == Provider.DEEPSEEK:
            return "DeepSeek"
        return "OpenAI"

    def _generate_chat_completion(
        self, options: LLMRequestOptions, history: list[LLMMessage]
    ) -> LLMResponse:
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": to_openai_messages(history),
        }
        if options.max_tokens is not None:
            payload["max_tokens"] = options.max_tokens
        if options.temperature is not None:
            payload["temperature"] = options.temperature
        if options.top_p is not None:
            payload["top_p"] = options.top_p
        if options.tools:
            payload["tools"] = [
                {
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": tool.input_schema,
                    },
                }
                for tool in options.tools
            ]
        if options.tool_choice:
            payload["tool_choice"] = {
                "type": "function",
                "function": {"name": options.tool_choice},
            }
        elif options.require_tool:
            payload["tool_choice"] = "required"

        data = self._post(payload)
        message = data["choices"][0]["message"]
        usage = data["usage"]
        return LLMResponse(
            text=message.get("content") or "",
            reasoning_content=message.get("reasoning_content"),
            tool_calls=[
                build_tool_call(tc["function"]["name"], tc["function"].get("arguments"))
                for tc in message.get("tool_calls", [])
                if tc.get("function", {}).get("name")
            ],
            usage=UsageDict(
                prompt_tokens=usage["prompt_tokens"],
                completion_tokens=usage["completion_tokens"],
                total_tokens=usage["total_tokens"],
            ),
        )
