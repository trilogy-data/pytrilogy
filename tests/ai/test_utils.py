import time
from unittest.mock import Mock, call

import pytest
from httpx import HTTPStatusError, Request, Response, TimeoutException

from trilogy.ai.providers.utils import RetryOptions, fetch_with_retry


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
