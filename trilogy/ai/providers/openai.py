from os import environ
from typing import List, Optional

from trilogy.ai.enums import Provider
from trilogy.ai.models import LLMMessage, LLMResponse, LLMToolCall, UsageDict
from trilogy.constants import logger

from .base import RETRYABLE_CODES, LLMProvider, LLMRequestOptions, parse_tool_arguments
from .utils import RetryOptions, fetch_with_retry


class OpenAIProvider(LLMProvider):
    def __init__(
        self,
        name: str,
        model: str,
        api_key: str | None = None,
        retry_options: Optional[RetryOptions] = None,
    ):
        api_key = api_key or environ.get("OPENAI_API_KEY")
        if not api_key:
            raise ValueError(
                "API key argument or environment variable OPENAI_API_KEY is required"
            )
        super().__init__(name, api_key, model, Provider.OPENAI)
        self.base_completion_url = "https://api.openai.com/v1/chat/completions"
        self.base_model_url = "https://api.openai.com/v1/models"
        self.models: List[str] = []
        self.type = Provider.OPENAI

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
                "Missing httpx. Install pytrilogy[ai] to use OpenAIProvider."
            )

        messages: List[dict] = []
        messages = [{"role": msg.role, "content": msg.content} for msg in history]
        try:

            def make_request():
                with httpx.Client(timeout=30) as client:
                    payload = {
                        "model": self.model,
                        "messages": messages,
                    }
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
                    LLMToolCall(
                        name=tool_call["function"]["name"],
                        arguments=parse_tool_arguments(
                            tool_call["function"].get("arguments")
                        ),
                    )
                    for tool_call in message.get("tool_calls", [])
                    if tool_call.get("function", {}).get("name")
                ],
                usage=UsageDict(
                    prompt_tokens=data["usage"]["prompt_tokens"],
                    completion_tokens=data["usage"]["completion_tokens"],
                    total_tokens=data["usage"]["total_tokens"],
                ),
            )
        except httpx.HTTPStatusError as error:
            # Capture the response body text
            error_detail = error.response.text
            raise Exception(
                f"OpenAI API error ({error.response.status_code}): {error_detail}"
            )

        except Exception as error:
            raise Exception(f"OpenAI API error: {str(error)}")
