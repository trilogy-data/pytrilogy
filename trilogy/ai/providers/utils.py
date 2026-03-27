import time
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
from typing import Callable, List, Optional, TypeVar

T = TypeVar("T")


@dataclass
class RetryOptions:
    max_retries: int = 3
    initial_delay_ms: int = 1000
    retry_status_codes: List[int] = field(
        default_factory=lambda: [429, 500, 502, 503, 504, 525]
    )
    on_retry: Callable[[int, int, Exception], None] | None = None
    retry_after_padding_ms: int = 500
    # Optional: parse a suggested retry delay (ms) from the error body
    extract_retry_delay_fn: Callable[[Exception], Optional[int]] | None = None


def _parse_retry_after_ms(value: str) -> Optional[int]:
    """Return ms from a Retry-After header (seconds int/float or HTTP-date), or None."""
    try:
        return int(float(value) * 1000)
    except ValueError:
        pass
    try:
        from datetime import datetime, timezone

        dt = parsedate_to_datetime(value)
        delta_ms = int((dt - datetime.now(timezone.utc)).total_seconds() * 1000)
        return max(0, delta_ms)
    except Exception:
        return None


def fetch_with_retry(fetch_fn: Callable[[], T], options: RetryOptions) -> T:
    from httpx import HTTPError, HTTPStatusError, TimeoutException

    """
    Retry a fetch operation with exponential backoff.

    Respects Retry-After headers and an optional body-level delay extractor.
    Falls back to exponential backoff when no server hint is available.
    """
    last_error = None
    delay_ms = options.initial_delay_ms

    for attempt in range(options.max_retries + 1):
        try:
            return fetch_fn()
        except (HTTPError, TimeoutException, Exception) as error:
            last_error = error
            should_retry = False
            suggested_ms: Optional[int] = None

            if isinstance(error, HTTPStatusError):
                if (
                    options.retry_status_codes
                    and error.response.status_code in options.retry_status_codes
                ):
                    should_retry = True
                    retry_after = error.response.headers.get("Retry-After")
                    if retry_after:
                        suggested_ms = _parse_retry_after_ms(retry_after)
                    if suggested_ms is None and options.extract_retry_delay_fn:
                        suggested_ms = options.extract_retry_delay_fn(error)
            elif isinstance(error, TimeoutException):
                should_retry = True

            if not should_retry or attempt >= options.max_retries:
                raise

            if suggested_ms is not None:
                actual_delay_ms = suggested_ms + options.retry_after_padding_ms
            else:
                actual_delay_ms = delay_ms

            if options.on_retry:
                options.on_retry(attempt + 1, actual_delay_ms, error)

            time.sleep(actual_delay_ms / 1000.0)
            delay_ms *= 2

    if last_error:
        raise last_error
    raise Exception("Retry logic failed unexpectedly")
