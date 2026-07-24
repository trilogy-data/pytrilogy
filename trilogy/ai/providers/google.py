import json
from os import environ
from typing import Any

from trilogy.ai.enums import Provider
from trilogy.ai.models import LLMMessage, LLMResponse, LLMToolCall, UsageDict
from trilogy.constants import logger

from .base import RETRYABLE_CODES, LLMProvider, LLMRequestOptions, iter_history_turns
from .utils import RetryOptions, fetch_with_retry


def _extract_google_retry_delay_ms(error: Exception) -> int | None:
    """Parse retryDelay from Google's error body (proto Duration string like '1.5s')."""
    try:
        from httpx import HTTPStatusError

        if not isinstance(error, HTTPStatusError):
            return None
        body = json.loads(error.response.text)
        for detail in body.get("error", {}).get("details", []):
            if "RetryInfo" in detail.get("@type", "") and "retryDelay" in detail:
                delay_str = detail["retryDelay"].rstrip("s")
                return int(float(delay_str) * 1000)
    except Exception:
        pass
    return None


def _convert_to_google_schema(value: Any) -> Any:
    if isinstance(value, dict):
        converted: dict[str, Any] = {}
        for key, nested_value in value.items():
            if key == "additionalProperties":
                continue
            if key == "type" and isinstance(nested_value, str):
                converted[key] = nested_value.upper()
            else:
                converted[key] = _convert_to_google_schema(nested_value)
        return converted
    if isinstance(value, list):
        return [_convert_to_google_schema(item) for item in value]
    return value


class GoogleProvider(LLMProvider):
    def __init__(
        self,
        name: str,
        model: str,
        api_key: str | None = None,
        retry_options: RetryOptions | None = None,
    ):
        api_key = api_key or environ.get("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError(
                "API key argument or environment variable GOOGLE_API_KEY is required"
            )
        super().__init__(name, api_key, model, Provider.GOOGLE)
        self.base_model_url = "https://generativelanguage.googleapis.com/v1/models"
        self.base_completion_url = "https://generativelanguage.googleapis.com/v1beta"
        self.models: list[str] = []
        self.type = Provider.GOOGLE
        self.retry_options = retry_options or RetryOptions(
            max_retries=3,
            initial_delay_ms=30000,
            retry_status_codes=RETRYABLE_CODES,
            on_retry=lambda attempt, delay_ms, error: logger.info(
                f"Google API retry attempt {attempt} after {delay_ms}ms delay due to error: {error!s}"
            ),
            extract_retry_delay_fn=_extract_google_retry_delay_ms,
        )

    def _convert_to_gemini_history(
        self, messages: list[LLMMessage]
    ) -> list[dict[str, Any]]:
        """Convert message history to Gemini `contents`, threading assistant
        tool calls as `functionCall` parts and their results as `functionResponse`
        parts. System messages are skipped (handled separately by the caller)."""
        contents: list[dict[str, Any]] = []
        for msg, tool_calls, results in iter_history_turns(messages):
            if msg.role == "system":
                continue
            if tool_calls:
                contents.append(
                    {
                        "role": "model",
                        "parts": [
                            {
                                "functionCall": {
                                    "name": tc.get("name", ""),
                                    "args": tc.get("arguments") or {},
                                }
                            }
                            for tc in tool_calls
                        ],
                    }
                )
                contents.append(
                    {
                        "role": "user",
                        "parts": [
                            {
                                "functionResponse": {
                                    "name": tool_calls[j].get("name", ""),
                                    "response": {"result": res.content or ""},
                                }
                            }
                            for j, res in enumerate(results)
                        ],
                    }
                )
            else:
                contents.append(
                    {
                        "role": "model" if msg.role == "assistant" else "user",
                        "parts": [{"text": msg.content}],
                    }
                )
        return contents

    def generate_completion(
        self, options: LLMRequestOptions, history: list[LLMMessage]
    ) -> LLMResponse:
        try:
            import httpx
        except ImportError:
            raise ImportError(
                "Missing httpx. Install pytrilogy[ai] to use GoogleProvider."
            )

        # Convert messages to Gemini format (system messages are skipped here).
        contents = self._convert_to_gemini_history(history)

        # The system message, if present, becomes a top-level instruction.
        system_instruction = None
        if history and history[0].role == "system":
            system_instruction = {"parts": [{"text": history[0].content}]}

        # Build the request URL
        url = f"{self.base_completion_url}/models/{self.model}:generateContent"

        # Build request body. Request thought summaries so reasoning shows up in
        # the trace (Gemini 2.5 models think by default but only return the
        # thought parts when includeThoughts is set).
        request_body: dict[str, Any] = {
            "contents": contents,
            "generationConfig": {"thinkingConfig": {"includeThoughts": True}},
        }
        if options.tools:
            request_body["tools"] = [
                {
                    "functionDeclarations": [
                        {
                            "name": tool.name,
                            "description": tool.description,
                            "parameters": _convert_to_google_schema(tool.input_schema),
                        }
                        for tool in options.tools
                    ]
                }
            ]
        if options.tool_choice:
            request_body["toolConfig"] = {
                "functionCallingConfig": {
                    "mode": "ANY",
                    "allowedFunctionNames": [options.tool_choice],
                }
            }
        elif options.require_tool:
            request_body["toolConfig"] = {
                "functionCallingConfig": {
                    "mode": "ANY",
                }
            }

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

            def fetch_function() -> dict[str, Any]:
                with httpx.Client(timeout=60) as client:
                    resp = client.post(
                        url,
                        headers={
                            "Content-Type": "application/json",
                            "x-goog-api-key": self.api_key,
                        },
                        json=request_body,
                    )
                    resp.raise_for_status()
                    return resp.json()

            data = fetch_with_retry(
                fetch_fn=fetch_function,
                options=self.retry_options,
            )

            # Extract text from response
            candidates = data.get("candidates", [])
            if not candidates:
                raise Exception("No candidates returned from Google API")

            content = candidates[0].get("content", {})
            parts = content.get("parts", [])

            if not parts:
                raise Exception("No parts in response content")

            # Parts flagged `thought: true` are reasoning summaries, not answer
            # text — keep them out of the response text and surface separately.
            text_parts = [
                part["text"]
                for part in parts
                if part.get("text") and not part.get("thought")
            ]
            thought_parts = [
                part["text"]
                for part in parts
                if part.get("text") and part.get("thought")
            ]
            tool_calls = [
                LLMToolCall(
                    name=part["functionCall"]["name"],
                    arguments=part["functionCall"].get("args", {}),
                )
                for part in parts
                if part.get("functionCall", {}).get("name")
            ]

            # Extract usage metadata
            usage_metadata = data.get("usageMetadata", {})
            prompt_tokens = usage_metadata.get("promptTokenCount", 0)
            completion_tokens = usage_metadata.get("candidatesTokenCount", 0)
            reasoning_tokens = usage_metadata.get("thoughtsTokenCount", 0)

            return LLMResponse(
                text="\n".join(text_parts).strip(),
                tool_calls=tool_calls,
                reasoning="\n".join(thought_parts).strip() or None,
                usage=UsageDict(
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    total_tokens=prompt_tokens + completion_tokens + reasoning_tokens,
                    reasoning_tokens=reasoning_tokens,
                ),
            )
        except httpx.HTTPStatusError as error:
            error_detail = error.response.text
            raise Exception(
                f"Google API error ({error.response.status_code}): {error_detail}"
            )
        except Exception as error:
            raise Exception(f"Google API error: {error!s}")
