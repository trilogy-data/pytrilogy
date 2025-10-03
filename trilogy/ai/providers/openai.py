from os import environ
from typing import List, Optional

from trilogy.ai.enums import Provider
from trilogy.ai.models import LLMMessage, LLMResponse, UsageDict

from .base import LLMProvider, LLMRequestOptions
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
            retry_status_codes=[429, 500, 502, 503, 504],  # Add common API error codes
            on_retry=lambda attempt, delay_ms, error: print(
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
            return LLMResponse(
                text=data["choices"][0]["message"]["content"],
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
