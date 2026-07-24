"""Direct DeepSeek API client. DeepSeek exposes an OpenAI-compatible
chat-completions endpoint, so this provider extends :class:`OpenAIProvider`
and only overrides the bits that differ: base URL, API-key env var, and
the timeout/retry posture (DeepSeek upstreams are direct, no OpenRouter
keep-alive masking, so the read timeout actually fires)."""

from os import environ

from trilogy.ai.enums import Provider
from trilogy.constants import logger

from .base import RETRYABLE_CODES
from .openai import OpenAIProvider
from .utils import RetryOptions


class DeepSeekProvider(OpenAIProvider):
    """OpenAI-compatible client pointed at DeepSeek's API.

    Use this instead of going through OpenRouter when you only need DeepSeek
    models — direct connection avoids the OpenRouter routing layer (and the
    keep-alive comments it injects on long-running calls, which can mask
    upstream hangs from httpx's read timeout). Cheaper, too, since there's
    no OpenRouter markup."""

    def __init__(
        self,
        name: str,
        model: str,
        api_key: str | None = None,
        retry_options: RetryOptions | None = None,
    ):
        api_key = api_key or environ.get("DEEPSEEK_API_KEY")
        if not api_key:
            raise ValueError(
                "API key argument or environment variable DEEPSEEK_API_KEY is required"
            )
        # Bypass OpenAIProvider's __init__ env-var lookup by passing the resolved
        # key. We still want OpenAIProvider's request-building behavior; only the
        # base URL + retry posture change.
        super().__init__(name=name, model=model, api_key=api_key)
        self.base_completion_url = "https://api.deepseek.com/v1/chat/completions"
        self.base_model_url = "https://api.deepseek.com/v1/models"
        self.type = Provider.DEEPSEEK

        self.retry_options = retry_options or RetryOptions(
            max_retries=5,
            initial_delay_ms=2000,
            retry_status_codes=RETRYABLE_CODES,
            on_retry=lambda attempt, delay_ms, error: logger.warning(
                "DeepSeek retry %d/5 after %dms backoff (error: %s)",
                attempt,
                delay_ms,
                str(error),
            ),
        )
