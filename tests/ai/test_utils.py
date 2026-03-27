import json
import time
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from unittest.mock import Mock, call

import pytest
from httpx import HTTPStatusError, Request, Response, TimeoutException

from trilogy.ai.providers.google import _extract_google_retry_delay_ms
from trilogy.ai.providers.utils import (
    RetryOptions,
    _parse_retry_after_ms,
    fetch_with_retry,
)


class TestParseRetryAfterMs:
    def test_integer_seconds(self):
        assert _parse_retry_after_ms("2") == 2000

    def test_float_seconds(self):
        assert _parse_retry_after_ms("0.165") == 165

    def test_http_date_future(self):
        future = datetime.now(timezone.utc) + timedelta(seconds=1)
        value = format_datetime(future, usegmt=True)
        result = _parse_retry_after_ms(value)
        assert result is not None
        assert 0 < result <= 1500

    def test_unparseable_returns_none(self):
        assert _parse_retry_after_ms("not-a-date-or-number") is None


class TestExtractGoogleRetryDelayMs:
    def _make_error(self, body: dict) -> HTTPStatusError:
        response = Response(
            status_code=429,
            content=json.dumps(body).encode(),
            request=Request("GET", "http://test.com"),
        )
        return HTTPStatusError(
            "Rate limited", request=response.request, response=response
        )

    def test_parses_retry_delay(self):
        error = self._make_error(
            {
                "error": {
                    "details": [
                        {
                            "@type": "type.googleapis.com/google.rpc.RetryInfo",
                            "retryDelay": "1.5s",
                        }
                    ]
                }
            }
        )
        assert _extract_google_retry_delay_ms(error) == 1500

    def test_zero_delay(self):
        error = self._make_error(
            {
                "error": {
                    "details": [
                        {
                            "@type": "type.googleapis.com/google.rpc.RetryInfo",
                            "retryDelay": "0s",
                        }
                    ]
                }
            }
        )
        assert _extract_google_retry_delay_ms(error) == 0

    def test_no_retry_info_returns_none(self):
        error = self._make_error({"error": {"details": []}})
        assert _extract_google_retry_delay_ms(error) is None

    def test_non_http_error_returns_none(self):
        assert _extract_google_retry_delay_ms(ValueError("oops")) is None


class TestFetchWithRetry:
    """Test suite for fetch_with_retry function."""

    def test_successful_first_attempt(self):
        """Test that successful fetch on first attempt returns immediately."""
        fetch_fn = Mock(return_value="success")
        options = RetryOptions()

        result = fetch_with_retry(fetch_fn, options)

        assert result == "success"
        assert fetch_fn.call_count == 1

    def test_retry_on_retryable_status_code(self):
        """Test that function retries on configured status codes."""
        response = Response(status_code=503, request=Request("GET", "http://test.com"))
        error = HTTPStatusError(
            "Service unavailable", request=response.request, response=response
        )

        fetch_fn = Mock(side_effect=[error, error, "success"])
        options = RetryOptions(max_retries=3, initial_delay_ms=10)

        result = fetch_with_retry(fetch_fn, options)

        assert result == "success"
        assert fetch_fn.call_count == 3

    def test_retry_on_timeout(self):
        """Test that function retries on timeout exceptions."""
        fetch_fn = Mock(
            side_effect=[
                TimeoutException("Timeout"),
                TimeoutException("Timeout"),
                "success",
            ]
        )
        options = RetryOptions(max_retries=3, initial_delay_ms=10)

        result = fetch_with_retry(fetch_fn, options)

        assert result == "success"
        assert fetch_fn.call_count == 3

    def test_max_retries_exceeded(self):
        """Test that exception is raised when max retries exceeded."""
        response = Response(status_code=503, request=Request("GET", "http://test.com"))
        error = HTTPStatusError(
            "Service unavailable", request=response.request, response=response
        )

        fetch_fn = Mock(side_effect=error)
        options = RetryOptions(max_retries=2, initial_delay_ms=10)

        with pytest.raises(HTTPStatusError):
            fetch_with_retry(fetch_fn, options)

        assert fetch_fn.call_count == 3  # Initial attempt + 2 retries

    def test_non_retryable_status_code(self):
        """Test that non-retryable status codes raise immediately."""
        response = Response(status_code=404, request=Request("GET", "http://test.com"))
        error = HTTPStatusError(
            "Not found", request=response.request, response=response
        )

        fetch_fn = Mock(side_effect=error)
        options = RetryOptions(max_retries=3, initial_delay_ms=10)

        with pytest.raises(HTTPStatusError):
            fetch_with_retry(fetch_fn, options)

        assert fetch_fn.call_count == 1  # No retries

    def test_exponential_backoff(self):
        """Test that delays follow exponential backoff pattern."""
        response = Response(status_code=429, request=Request("GET", "http://test.com"))
        error = HTTPStatusError(
            "Rate limited", request=response.request, response=response
        )

        fetch_fn = Mock(side_effect=[error, error, error, "success"])
        options = RetryOptions(max_retries=3, initial_delay_ms=100)

        start_time = time.time()
        result = fetch_with_retry(fetch_fn, options)
        elapsed_time = time.time() - start_time

        # Expected delays: 100ms, 200ms, 400ms = 700ms total
        assert result == "success"
        assert elapsed_time >= 0.7  # At least 700ms
        assert elapsed_time < 1.0  # But not too much more

    def test_on_retry_callback(self):
        """Test that on_retry callback is called with correct parameters."""
        response = Response(status_code=503, request=Request("GET", "http://test.com"))
        error = HTTPStatusError(
            "Service unavailable", request=response.request, response=response
        )

        fetch_fn = Mock(side_effect=[error, error, "success"])
        on_retry_mock = Mock()
        options = RetryOptions(
            max_retries=3, initial_delay_ms=10, on_retry=on_retry_mock
        )

        fetch_with_retry(fetch_fn, options)

        assert on_retry_mock.call_count == 2
        # Check that callback was called with increasing attempt numbers and delays
        on_retry_mock.assert_has_calls([call(1, 10, error), call(2, 20, error)])

    def test_custom_retry_status_codes(self):
        """Test that custom retry status codes are respected."""
        response_retryable = Response(
            status_code=418, request=Request("GET", "http://test.com")
        )
        response_non_retryable = Response(
            status_code=503, request=Request("GET", "http://test.com")
        )

        error_retryable = HTTPStatusError(
            "I'm a teapot",
            request=response_retryable.request,
            response=response_retryable,
        )
        error_non_retryable = HTTPStatusError(
            "Service unavailable",
            request=response_non_retryable.request,
            response=response_non_retryable,
        )

        # Test custom code is retried
        fetch_fn = Mock(side_effect=[error_retryable, "success"])
        options = RetryOptions(
            max_retries=2, initial_delay_ms=10, retry_status_codes=[418]
        )

        result = fetch_with_retry(fetch_fn, options)
        assert result == "success"
        assert fetch_fn.call_count == 2

        # Test default code is not retried when not in custom list
        fetch_fn = Mock(side_effect=error_non_retryable)
        options = RetryOptions(
            max_retries=2, initial_delay_ms=10, retry_status_codes=[418]
        )

        with pytest.raises(HTTPStatusError):
            fetch_with_retry(fetch_fn, options)

        assert fetch_fn.call_count == 1

    def test_empty_retry_status_codes(self):
        """Test behavior when retry_status_codes is empty."""
        response = Response(status_code=503, request=Request("GET", "http://test.com"))
        error = HTTPStatusError(
            "Service unavailable", request=response.request, response=response
        )

        fetch_fn = Mock(side_effect=error)
        options = RetryOptions(
            max_retries=2, initial_delay_ms=10, retry_status_codes=[]
        )

        with pytest.raises(HTTPStatusError):
            fetch_with_retry(fetch_fn, options)

        assert fetch_fn.call_count == 1  # No retries

    def test_zero_retries(self):
        """Test that max_retries=0 means only one attempt."""
        response = Response(status_code=503, request=Request("GET", "http://test.com"))
        error = HTTPStatusError(
            "Service unavailable", request=response.request, response=response
        )

        fetch_fn = Mock(side_effect=error)
        options = RetryOptions(max_retries=0, initial_delay_ms=10)

        with pytest.raises(HTTPStatusError):
            fetch_with_retry(fetch_fn, options)

        assert fetch_fn.call_count == 1

    def test_retry_after_header_overrides_backoff(self):
        """Retry-After header delay is used instead of exponential backoff."""
        response = Response(
            status_code=429,
            headers={"Retry-After": "0.05"},  # 50ms
            request=Request("GET", "http://test.com"),
        )
        error = HTTPStatusError(
            "Rate limited", request=response.request, response=response
        )

        fetch_fn = Mock(side_effect=[error, "success"])
        on_retry_mock = Mock()
        options = RetryOptions(
            max_retries=2,
            initial_delay_ms=10000,  # would be 10s without header
            retry_after_padding_ms=10,
            on_retry=on_retry_mock,
        )

        start = time.time()
        result = fetch_with_retry(fetch_fn, options)
        elapsed = time.time() - start

        assert result == "success"
        assert elapsed < 1.0  # used header (50ms + 10ms), not 10s backoff
        on_retry_mock.assert_called_once_with(1, 60, error)  # 50ms + 10ms padding

    def test_extract_retry_delay_fn_used_when_no_header(self):
        """extract_retry_delay_fn is used when no Retry-After header is present."""
        response = Response(
            status_code=429,
            request=Request("GET", "http://test.com"),
        )
        error = HTTPStatusError(
            "Rate limited", request=response.request, response=response
        )

        fetch_fn = Mock(side_effect=[error, "success"])
        on_retry_mock = Mock()
        options = RetryOptions(
            max_retries=2,
            initial_delay_ms=10000,
            retry_after_padding_ms=50,
            on_retry=on_retry_mock,
            extract_retry_delay_fn=lambda _: 100,  # 100ms from body
        )

        start = time.time()
        result = fetch_with_retry(fetch_fn, options)
        elapsed = time.time() - start

        assert result == "success"
        assert elapsed < 1.0
        on_retry_mock.assert_called_once_with(1, 150, error)  # 100ms + 50ms padding

    def test_retry_after_header_takes_precedence_over_extract_fn(self):
        """Retry-After header takes precedence over extract_retry_delay_fn."""
        response = Response(
            status_code=429,
            headers={"Retry-After": "0.02"},  # 20ms
            request=Request("GET", "http://test.com"),
        )
        error = HTTPStatusError(
            "Rate limited", request=response.request, response=response
        )

        fetch_fn = Mock(side_effect=[error, "success"])
        on_retry_mock = Mock()
        options = RetryOptions(
            max_retries=2,
            initial_delay_ms=10000,
            retry_after_padding_ms=0,
            on_retry=on_retry_mock,
            extract_retry_delay_fn=lambda _: 9000,  # should be ignored
        )

        fetch_with_retry(fetch_fn, options)
        on_retry_mock.assert_called_once_with(1, 20, error)

    def test_type_preservation(self):
        """Test that return type is preserved correctly."""
        # Test with dict
        fetch_fn = Mock(return_value={"key": "value"})
        result = fetch_with_retry(fetch_fn, RetryOptions())
        assert result == {"key": "value"}

        # Test with list
        fetch_fn = Mock(return_value=[1, 2, 3])
        result = fetch_with_retry(fetch_fn, RetryOptions())
        assert result == [1, 2, 3]

        # Test with custom object
        class CustomObject:
            def __init__(self, value):
                self.value = value

        obj = CustomObject(42)
        fetch_fn = Mock(return_value=obj)
        result = fetch_with_retry(fetch_fn, RetryOptions())
        assert result.value == 42
