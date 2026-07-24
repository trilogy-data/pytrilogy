import dataclasses
import json
import time
from os import environ
from typing import Any

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
from .utils import RetryOptions, fetch_with_retry, sanitize_html_escapes

# Number of times to re-issue the underlying HTTP call when the response is
# 200-OK but the body itself is unusable — error envelope from an upstream
# provider, missing `choices`, etc. These are not HTTPStatusErrors so
# ``fetch_with_retry`` (which retries on httpx error codes only) doesn't see
# them; we retry at the body-interpretation layer.
_BODY_RETRY_ATTEMPTS = 3
_BODY_RETRY_INITIAL_DELAY_S = 2.0


def _interpret_body(data: Any) -> tuple[dict | None, str | None]:
    """Inspect a 200-OK response body. Returns ``(data, None)`` when usable,
    ``(None, reason)`` when the response should be retried.

    Two failure shapes the OpenRouter pass-through can produce as 200-OK:
      - **error envelope**: ``{"error": {...}}`` with no ``choices`` — upstream
        provider failed but OpenRouter answered. Retry tends to succeed when
        the failure was transient (upstream node hiccup).
      - **missing-choices**: ``{}`` or ``{"id": "…"}`` with no ``choices`` at
        all — the response body is structurally incomplete.

    A genuinely fatal envelope is raised by the caller with the parsed
    detail; we only mark these as retryable here."""
    if not isinstance(data, dict):
        return None, f"non-dict response body: {type(data).__name__}"
    if "error" in data and not data.get("choices"):
        err = data["error"]
        if isinstance(err, dict):
            code = err.get("code")
            msg = err.get("message", "(no message)")
            return None, f"upstream error envelope (code={code}): {msg}"
        return None, f"upstream error envelope: {err}"
    if not data.get("choices"):
        return None, "response missing 'choices'"
    return data, None


def _env_flag(name: str) -> bool:
    """Truthy parser for an env-var feature flag (1/true/yes/on, any case)."""
    raw = environ.get(name)
    if raw is None:
        return False
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _load_provider_routing() -> dict | None:
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
        retry_options: RetryOptions | None = None,
        provider_routing: dict | None = None,
        sanitize_html_escapes: bool | None = None,
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
        # When True, HTML entities in tool-call arguments are decoded before
        # the agent loop sees them (``&lt;-`` → ``<-``). Some upstreams escape
        # operators inside tool args; sanitizing here avoids teaching the
        # agent to undo the escapes itself. Off by default to preserve raw
        # payloads in production runs; eval harnesses opt in.
        self.sanitize_html_escapes = (
            sanitize_html_escapes
            if sanitize_html_escapes is not None
            else _env_flag("OPENROUTER_SANITIZE_HTML_ESCAPES")
        )

        # 5 retries at 2s initial w/ exp backoff = ~62s of total wait time
        # (2+4+8+16+32). DeepSeek upstreams on OpenRouter (DeepInfra, etc.)
        # rate-limit aggressively under burst; shorter budgets surfaced the
        # 429 mid-eval instead of riding it out.
        self.retry_options = retry_options or RetryOptions(
            max_retries=5,
            initial_delay_ms=2000,
            retry_status_codes=RETRYABLE_CODES,
            on_retry=lambda attempt, delay_ms, error: logger.warning(
                "OpenRouter retry %d/5 after %dms backoff (error: %s)",
                attempt,
                delay_ms,
                str(error),
            ),
        )

    def generate_completion(
        self, options: LLMRequestOptions, history: list[LLMMessage]
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
                # httpx `read` is per-chunk (between-bytes), not total elapsed —
                # so a slow-drip response that trickles a byte every few seconds
                # can hang forever at 60s. 30s is generous for any healthy
                # generation; longer means the upstream is misbehaving and we
                # should bail to the retry layer rather than wait it out.
                timeout = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)
                with httpx.Client(timeout=timeout) as client:
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

            data: dict | None = None
            last_problem: str | None = None
            for attempt in range(_BODY_RETRY_ATTEMPTS):
                raw = fetch_with_retry(make_request, self.retry_options)
                data, problem = _interpret_body(raw)
                if data is not None:
                    break
                last_problem = problem
                if attempt < _BODY_RETRY_ATTEMPTS - 1:
                    delay = _BODY_RETRY_INITIAL_DELAY_S * (2**attempt)
                    logger.warning(
                        "OpenRouter body retry %d/%d in %.1fs: %s",
                        attempt + 1,
                        _BODY_RETRY_ATTEMPTS,
                        delay,
                        problem,
                    )
                    time.sleep(delay)
            if data is None:
                # Fatal — log the structured problem and bubble up so the
                # caller sees the real reason instead of a downstream KeyError.
                logger.error(
                    "OpenRouter API error (gave up after %d body retries): %s",
                    _BODY_RETRY_ATTEMPTS,
                    last_problem,
                )
                raise Exception(f"OpenRouter API error: {last_problem}")

            choice = data["choices"][0]
            message = choice.get("message") or {}
            finish_reason = choice.get("finish_reason") or choice.get(
                "native_finish_reason"
            )
            # `usage` is optional in upstream responses. Default each field
            # to 0 so a missing block doesn't blow up the agent loop — better
            # to under-report token counts than to crash mid-conversation.
            usage = data.get("usage") or {}
            if finish_reason == "length":
                logger.warning(
                    "OpenRouter response truncated by max_tokens (finish_reason=length). "
                    "completion_tokens=%s, model=%s. tool_call arguments may be partial.",
                    usage.get("completion_tokens", 0),
                    self.model,
                )
            tool_calls = [
                build_tool_call(tc["function"]["name"], tc["function"].get("arguments"))
                for tc in message.get("tool_calls", [])
                if tc.get("function", {}).get("name")
            ]
            if self.sanitize_html_escapes:
                tool_calls = [
                    (
                        tc
                        if tc.parse_error
                        else dataclasses.replace(
                            tc, arguments=sanitize_html_escapes(tc.arguments)
                        )
                    )
                    for tc in tool_calls
                ]
            return LLMResponse(
                text=message.get("content") or "",
                tool_calls=tool_calls,
                usage=UsageDict(
                    prompt_tokens=usage.get("prompt_tokens", 0) or 0,
                    completion_tokens=usage.get("completion_tokens", 0) or 0,
                    total_tokens=usage.get("total_tokens", 0) or 0,
                ),
                finish_reason=finish_reason,
            )
        except httpx.HTTPStatusError as error:
            error_detail = error.response.text
            raise Exception(
                f"OpenRouter API error ({error.response.status_code}): {error_detail}"
            )
        except Exception as error:
            raise Exception(f"OpenRouter API error: {error!s}")
