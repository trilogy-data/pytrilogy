from typing import List, Optional

from trilogy.ai.models import LLMMessage, LLMResponse, UsageDict

from .base import LLMProvider, LLMRequestOptions
from .utils import RetryOptions, fetch_with_retry


class OpenAIProvider(LLMProvider):
    def __init__(
        self,
        name: str,
        api_key: str,
        model: str,
        save_credential: bool = False,
        retry_options: Optional[RetryOptions] = None,
    ):
        super().__init__(name, api_key, model, save_credential)
        self.base_completion_url = "https://api.openai.com/v1/chat/completions"
        self.base_model_url = "https://api.openai.com/v1/models"
        self.models: List[str] = []
        self.type = "openai"

        self.retry_options = retry_options or RetryOptions(
            max_retries=3,
            initial_delay_ms=1000,
            retry_status_codes=[429, 500, 502, 503, 504],  # Add common API error codes
            on_retry=lambda attempt, delay_ms, error: print(
                f"Retry attempt {attempt} after {delay_ms}ms delay due to error: {str(error)}"
            ),
        )

    def reset(self) -> None:
        import httpx

        self.error = None
        try:

            def make_request():
                with httpx.Client() as client:
                    response = client.get(
                        self.base_model_url,
                        headers={"Authorization": f"Bearer {self.api_key}"},
                    )
                    response.raise_for_status()
                    return response.json()

            model_data = fetch_with_retry(make_request, self.retry_options)
            self.models = sorted([model["id"] for model in model_data["data"]])
            self.connected = True
        except Exception as e:
            self.error = str(e)
            self.connected = False
            raise

    def generate_completion(
        self, options: LLMRequestOptions, history: List[LLMMessage]
    ) -> LLMResponse:
        import httpx

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
