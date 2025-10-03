from os import environ
from typing import Any, Dict, List, Optional

from trilogy.ai.enums import Provider
from trilogy.ai.models import LLMMessage, LLMResponse, UsageDict

from .base import LLMProvider, LLMRequestOptions
from .utils import RetryOptions, fetch_with_retry


class GoogleProvider(LLMProvider):
    def __init__(
        self,
        name: str,
        model: str,
        api_key: str | None = None,
        retry_options: Optional[RetryOptions] = None,
    ):
        api_key = api_key or environ.get("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError(
                "API key argument or environment variable GOOGLE_API_KEY is required"
            )
        super().__init__(name, api_key, model, Provider.GOOGLE)
        self.base_model_url = "https://generativelanguage.googleapis.com/v1/models"
        self.base_completion_url = "https://generativelanguage.googleapis.com/v1beta"
        self.models: List[str] = []
        self.type = Provider.GOOGLE
        self.retry_options = retry_options or RetryOptions(
            max_retries=3,
            initial_delay_ms=30000,  # 30s default for Google's 429 rate limits
            retry_status_codes=[429, 500, 502, 503, 504],
            on_retry=lambda attempt, delay_ms, error: print(
                f"Google API retry attempt {attempt} after {delay_ms}ms delay due to error: {str(error)}"
            ),
        )

    def _convert_to_gemini_history(
        self, messages: List[LLMMessage]
    ) -> List[Dict[str, Any]]:
        """Convert standard message format to Gemini format."""
        return [
            {
                "role": "model" if msg.role == "assistant" else "user",
                "parts": [{"text": msg.content}],
            }
            for msg in messages
        ]

    def generate_completion(
        self, options: LLMRequestOptions, history: List[LLMMessage]
    ) -> LLMResponse:
        try:
            import httpx
        except ImportError:
            raise ImportError(
                "Missing httpx. Install pytrilogy[ai] to use GoogleProvider."
            )

        # Convert messages to Gemini format
        gemini_history = self._convert_to_gemini_history(history)

        # Separate system message if present
        system_instruction = None
        contents = gemini_history

        # Check if first message is a system message
        if history and history[0].role == "system":
            system_instruction = {"parts": [{"text": history[0].content}]}
            contents = gemini_history[1:]  # Remove system message from history

        # Build the request URL
        url = f"{self.base_completion_url}/models/{self.model}:generateContent"

        # Build request body
        request_body: Dict[str, Any] = {"contents": contents, "generationConfig": {}}

        # Add system instruction if present
        if system_instruction:
            request_body["systemInstruction"] = system_instruction

        # Add generation config options
        if options.temperature is not None:
            request_body["generationConfig"]["temperature"] = options.temperature

        if options.max_tokens is not None:
            request_body["generationConfig"]["maxOutputTokens"] = options.max_tokens

        if options.top_p is not None:
            request_body["generationConfig"]["topP"] = options.top_p

        try:
            # Make the API request with retry logic using a lambda
            response = fetch_with_retry(
                fetch_fn=lambda: httpx.post(
                    url,
                    headers={
                        "Content-Type": "application/json",
                        "x-goog-api-key": self.api_key,
                    },
                    json=request_body,
                    timeout=60.0,
                ),
                options=self.retry_options,
            )

            response.raise_for_status()
            data = response.json()

            # Extract text from response
            candidates = data.get("candidates", [])
            if not candidates:
                raise Exception("No candidates returned from Google API")

            content = candidates[0].get("content", {})
            parts = content.get("parts", [])

            if not parts:
                raise Exception("No parts in response content")

            text = parts[0].get("text", "")

            # Extract usage metadata
            usage_metadata = data.get("usageMetadata", {})
            prompt_tokens = usage_metadata.get("promptTokenCount", 0)
            completion_tokens = usage_metadata.get("candidatesTokenCount", 0)

            return LLMResponse(
                text=text,
                usage=UsageDict(
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    total_tokens=prompt_tokens + completion_tokens,
                ),
            )
        except httpx.HTTPStatusError as error:
            error_detail = error.response.text
            raise Exception(
                f"Google API error ({error.response.status_code}): {error_detail}"
            )
        except Exception as error:
            raise Exception(f"Google API error: {str(error)}")
