import time
from dataclasses import dataclass
from typing import Callable, List, Optional, TypeVar

T = TypeVar("T")


@dataclass
class RetryOptions:
    max_retries: int = 3
    initial_delay_ms: int = 1000
    retry_status_codes: List[int] = None
    on_retry: Optional[Callable[[int, int, Exception], None]] = None

    def __post_init__(self):
        if self.retry_status_codes is None:
            self.retry_status_codes = [429, 500, 502, 503, 504]


def fetch_with_retry(fetch_fn: Callable[[], T], options: RetryOptions) -> T:
    """
    Retry a fetch operation with exponential backoff.

    Args:
        fetch_fn: Function that performs the fetch operation
        options: Retry configuration options

    Returns:
        The result from the successful fetch operation

    Raises:
        The last exception encountered if all retries fail
    """
    last_error = None
    delay_ms = options.initial_delay_ms

    for attempt in range(options.max_retries + 1):
        try:
            return fetch_fn()
        except Exception as error:
            last_error = error

            # Check if we should retry based on status code (if available)
            should_retry = False
            if hasattr(error, "response") and hasattr(error.response, "status_code"):
                if error.response.status_code in options.retry_status_codes:
                    should_retry = True
            elif attempt < options.max_retries:
                # Retry for other errors too, unless we're out of retries
                should_retry = True

            if not should_retry or attempt >= options.max_retries:
                raise

            # Call the retry callback if provided
            if options.on_retry:
                options.on_retry(attempt + 1, delay_ms, error)

            # Wait before retrying with exponential backoff
            time.sleep(delay_ms / 1000.0)
            delay_ms *= 2  # Exponential backoff

    # This should never be reached, but just in case
    if last_error:
        raise last_error
    raise Exception("Retry logic failed unexpectedly")
