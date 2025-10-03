from os import environ
from typing import List, Optional

from trilogy.ai.enums import Provider
from trilogy.ai.models import LLMMessage, LLMResponse, UsageDict

from .base import LLMProvider, LLMRequestOptions
from .utils import RetryOptions, fetch_with_retry

DEFAULT_MAX_TOKENS = 10000


class AnthropicProvider(LLMProvider):
    def __init__(
        self,
        name: str,
        model: str,
        api_key: str | None = None,
        retry_options: Optional[RetryOptions] = None,
    ):
        api_key = api_key or environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError(
                "API key argument or environment variable ANTHROPIC_API_KEY is required"
            )
        super().__init__(name, api_key, model, Provider.ANTHROPIC)
        self.base_completion_url = "https://api.anthropic.com/v1/messages"
        self.base_model_url = "https://api.anthropic.com/v1/models"
        self.models: List[str] = []
        self.type = Provider.ANTHROPIC
        self.retry_options = retry_options or RetryOptions(
            max_retries=5,
            initial_delay_ms=5000,
            retry_status_codes=[429, 500, 502, 503, 504],
            on_retry=lambda attempt, delay_ms, error: print(
                f"Anthropic API retry attempt {attempt} after {delay_ms}ms delay due to error: {str(error)}"
            ),
        )

    def generate_completion(
        self, options: LLMRequestOptions, history: List[LLMMessage]
    ) -> LLMResponse:
        try:
            import httpx
        except ImportError:
            raise ImportError(
                "Missing httpx. Install pytrilogy[ai] to use AnthropicProvider."
            )

        # Separate system messages from user/assistant messages
        system_messages = [msg.content for msg in history if msg.role == "system"]
        conversation_messages = [
            {"role": msg.role, "content": msg.content}
            for msg in history
            if msg.role != "system"
        ]

        try:

            def make_request():
                with httpx.Client(timeout=60) as client:
                    payload = {
                        "model": self.model,
                        "messages": conversation_messages,
                        "max_tokens": options.max_tokens or 10000,
                        # "temperature": options.temperature or 0.7,
                        # "top_p": options.top_p if hasattr(options, "top_p") else 1.0,
                    }

                    # Add system parameter if there are system messages
                    if system_messages:
                        # Combine multiple system messages with newlines
                        payload["system"] = "\n\n".join(system_messages)

                    response = client.post(
                        url=self.base_completion_url,
                        headers={
                            "Content-Type": "application/json",
                            "x-api-key": self.api_key,
                            "anthropic-version": "2023-06-01",
                        },
                        json=payload,
                    )
                    response.raise_for_status()
                    return response.json()

            data = fetch_with_retry(make_request, self.retry_options)

            return LLMResponse(
                text=data["content"][0]["text"],
                usage=UsageDict(
                    prompt_tokens=data["usage"]["input_tokens"],
                    completion_tokens=data["usage"]["output_tokens"],
                    total_tokens=data["usage"]["input_tokens"]
                    + data["usage"]["output_tokens"],
                ),
            )

        except httpx.HTTPStatusError as error:
            error_detail = error.response.text
            raise Exception(
                f"Anthropic API error ({error.response.status_code}): {error_detail}"
            )
        except Exception as error:
            raise Exception(f"Anthropic API error: {str(error)}")
