from os import environ
from typing import List, Optional

from trilogy.ai.enums import Provider
from trilogy.ai.models import LLMMessage, LLMResponse, LLMToolCall, UsageDict
from trilogy.constants import logger

from .base import RETRYABLE_CODES, LLMProvider, LLMRequestOptions, iter_history_turns
from .utils import RetryOptions, fetch_with_retry

DEFAULT_MAX_TOKENS = 10000


def _to_anthropic_messages(history: List[LLMMessage]) -> list[dict]:
    """Anthropic Messages-format conversation (system messages are handled
    separately by the caller). Assistant tool calls become ``tool_use`` content
    blocks and their results become a following user message of ``tool_result``
    blocks — so the model sees its own prior tool calls."""
    messages: list[dict] = []
    for idx, (msg, tool_calls, results) in enumerate(iter_history_turns(history)):
        if msg.role == "system":
            continue
        if tool_calls:
            ids = [f"call_{idx}_{j}" for j in range(len(tool_calls))]
            blocks: list[dict] = []
            if msg.content:
                blocks.append({"type": "text", "text": msg.content})
            for j, tc in enumerate(tool_calls):
                blocks.append(
                    {
                        "type": "tool_use",
                        "id": ids[j],
                        "name": tc.get("name", ""),
                        "input": tc.get("arguments") or {},
                    }
                )
            messages.append({"role": "assistant", "content": blocks})
            messages.append(
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": ids[j],
                            "content": res.content or "",
                        }
                        for j, res in enumerate(results)
                    ],
                }
            )
        else:
            messages.append({"role": msg.role, "content": msg.content})
    return messages


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
            retry_status_codes=RETRYABLE_CODES,
            on_retry=lambda attempt, delay_ms, error: logger.info(
                f"Anthropic API retry attempt {attempt} after {delay_ms}ms delay due to error: {str(error)}"
            ),
        )

    @staticmethod
    def _build_usage(usage: dict) -> UsageDict:
        # With caching enabled, input_tokens covers only the uncached portion;
        # cached tokens are reported in the cache_* fields.
        input_tokens = (
            usage["input_tokens"]
            + usage.get("cache_creation_input_tokens", 0)
            + usage.get("cache_read_input_tokens", 0)
        )
        return UsageDict(
            prompt_tokens=input_tokens,
            completion_tokens=usage["output_tokens"],
            total_tokens=input_tokens + usage["output_tokens"],
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

        # System messages go to the top-level `system` param; the rest are
        # threaded (tool calls included) into Anthropic Messages format.
        system_messages = [msg.content for msg in history if msg.role == "system"]
        conversation_messages = _to_anthropic_messages(history)

        try:

            def make_request():
                with httpx.Client(timeout=60) as client:
                    payload = {
                        "model": self.model,
                        "messages": conversation_messages,
                        "max_tokens": options.max_tokens or DEFAULT_MAX_TOKENS,
                        # Automatic prompt caching: caches the last cacheable
                        # block so repeated prefixes (system + tools + history)
                        # are read from cache on subsequent turns.
                        "cache_control": {"type": "ephemeral"},
                        # "temperature": options.temperature or 0.7,
                        # "top_p": options.top_p if hasattr(options, "top_p") else 1.0,
                    }
                    if options.tools:
                        payload["tools"] = [
                            {
                                "name": tool.name,
                                "description": tool.description,
                                "input_schema": tool.input_schema,
                            }
                            for tool in options.tools
                        ]
                    if options.tool_choice:
                        payload["tool_choice"] = {
                            "type": "tool",
                            "name": options.tool_choice,
                        }
                    elif options.require_tool:
                        payload["tool_choice"] = {"type": "any"}

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
            content = data.get("content", [])

            return LLMResponse(
                text="\n".join(
                    block.get("text", "")
                    for block in content
                    if block.get("type") == "text"
                ).strip(),
                tool_calls=[
                    LLMToolCall(
                        name=block["name"],
                        arguments=block.get("input", {}),
                    )
                    for block in content
                    if block.get("type") == "tool_use" and block.get("name")
                ],
                usage=self._build_usage(data["usage"]),
            )

        except httpx.HTTPStatusError as error:
            error_detail = error.response.text
            raise Exception(
                f"Anthropic API error ({error.response.status_code}): {error_detail}"
            )
        except Exception as error:
            raise Exception(f"Anthropic API error: {str(error)}")
