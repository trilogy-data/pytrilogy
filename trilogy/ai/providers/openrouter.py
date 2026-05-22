import json
from os import environ
from typing import List, Optional

from trilogy.ai.enums import Provider
from trilogy.ai.models import LLMMessage, LLMResponse, UsageDict
from trilogy.constants import logger

from .base import (
    RETRYABLE_CODES,
    LLMProvider,
    LLMRequestOptions,
    build_tool_call,
    to_openai_messages,
)
from .utils import RetryOptions, fetch_with_retry


def _load_provider_routing() -> Optional[dict]:
    """OpenRouter provider-selection preferences from the OPENROUTER_PROVIDER
    env var (a JSON object, e.g. {"ignore": ["AtlasCloud"]}). OpenRouter
    multiplexes a model across providers and some reject otherwise-valid tool
    requests, so this allows pinning/suppressing routes.

    See https://openrouter.ai/docs/features/provider-routing.
    """
    raw = environ.get("OPENROUTER_PROVIDER")
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"OPENROUTER_PROVIDER must be valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError("OPENROUTER_PROVIDER must be a JSON object")
    return parsed


class OpenRouterProvider(LLMProvider):
    def __init__(
        self,
        name: str,
        model: str,
        api_key: str | None = None,
        retry_options: Optional[RetryOptions] = None,
        provider_routing: Optional[dict] = None,
    ):
        api_key = api_key or environ.get("OPENROUTER_API_KEY")
        if not api_key:
            raise ValueError(
                "API key argument or environment variable OPENROUTER_API_KEY is required"
            )
        super().__init__(name, api_key, model, Provider.OPENROUTER)
        self.base_completion_url = "https://openrouter.ai/api/v1/chat/completions"
        self.type = Provider.OPENROUTER
        self.provider_routing = (
            provider_routing
            if provider_routing is not None
            else _load_provider_routing()
        )

        self.retry_options = retry_options or RetryOptions(
            max_retries=3,
            initial_delay_ms=1000,
            retry_status_codes=RETRYABLE_CODES,
            on_retry=lambda attempt, delay_ms, error: logger.info(
                f"Retry attempt {attempt} after {delay_ms}ms delay due to error: {str(error)}"
            ),
        )

    def generate_completion(
        self, options: LLMRequestOptions, history: List[LLMMessage]
    ) -> LLMResponse:
        try:
            import httpx
        except ImportError:
            raise ImportError(
                "Missing httpx. Install pytrilogy[ai] to use OpenRouterProvider."
            )

        messages = to_openai_messages(history)
        try:

            def make_request():
                with httpx.Client(timeout=30) as client:
                    payload = {
                        "model": self.model,
                        "messages": messages,
                    }
                    if self.provider_routing:
                        payload["provider"] = self.provider_routing
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

            data = fetch_with_retry(make_request, self.retry_options)
            message = data["choices"][0]["message"]
            return LLMResponse(
                text=message.get("content") or "",
                tool_calls=[
                    build_tool_call(
                        tc["function"]["name"], tc["function"].get("arguments")
                    )
                    for tc in message.get("tool_calls", [])
                    if tc.get("function", {}).get("name")
                ],
                usage=UsageDict(
                    prompt_tokens=data["usage"]["prompt_tokens"],
                    completion_tokens=data["usage"]["completion_tokens"],
                    total_tokens=data["usage"]["total_tokens"],
                ),
            )
        except httpx.HTTPStatusError as error:
            error_detail = error.response.text
            raise Exception(
                f"OpenRouter API error ({error.response.status_code}): {error_detail}"
            )
        except Exception as error:
            raise Exception(f"OpenRouter API error: {str(error)}")
