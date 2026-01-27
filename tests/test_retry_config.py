from unittest.mock import patch

import pytest

from trilogy import Dialects
from trilogy.dialect.config import DuckDBConfig, RetryConfig, RetryPolicy


def test_retry_policy_delay_calculation():
    policy = RetryPolicy(max_attempts=3, base_delay_seconds=1.0, exponential_base=2.0)
    assert policy.get_delay(1) == 1.0
    assert policy.get_delay(2) == 2.0
    assert policy.get_delay(3) == 4.0


def test_retry_policy_max_delay():
    policy = RetryPolicy(
        max_attempts=5,
        base_delay_seconds=10.0,
        exponential_base=2.0,
        max_delay_seconds=30.0,
    )
    assert policy.get_delay(1) == 10.0
    assert policy.get_delay(2) == 20.0
    assert policy.get_delay(3) == 30.0  # capped at max


def test_retry_config_pattern_matching():
    policy = RetryPolicy(max_attempts=2)
    config = RetryConfig(patterns={"access_mode": policy})

    assert config.get_policy_for_error("error: access_mode issue") is policy
    assert config.get_policy_for_error("ACCESS_MODE problem") is policy
    assert config.get_policy_for_error("unrelated error") is None


def test_executor_retry_on_error():
    retry_policy = RetryPolicy(max_attempts=3, base_delay_seconds=0.01)
    retry_config = RetryConfig(patterns={r"test_retry_error": retry_policy})
    config = DuckDBConfig(retry_config=retry_config)

    executor = Dialects.DUCK_DB.default_executor(conf=config)

    with patch("time.sleep") as mock_sleep:
        with pytest.raises(Exception) as exc_info:
            executor.execute_raw_sql("SELECT error('test_retry_error')")

        assert "test_retry_error" in str(exc_info.value)
        assert mock_sleep.call_count == 2  # retried twice before final failure


def test_executor_no_retry_on_unmatched_error():
    retry_policy = RetryPolicy(max_attempts=3, base_delay_seconds=0.01)
    retry_config = RetryConfig(patterns={r"specific_error": retry_policy})
    config = DuckDBConfig(retry_config=retry_config)

    executor = Dialects.DUCK_DB.default_executor(conf=config)

    with patch("time.sleep") as mock_sleep:
        with pytest.raises(Exception) as exc_info:
            executor.execute_raw_sql("SELECT error('different_error')")

        assert "different_error" in str(exc_info.value)
        assert mock_sleep.call_count == 0  # no retry for unmatched error


def test_executor_no_retry_without_config():
    executor = Dialects.DUCK_DB.default_executor()

    with patch("time.sleep") as mock_sleep:
        with pytest.raises(Exception):
            executor.execute_raw_sql("SELECT error('any_error')")

        assert mock_sleep.call_count == 0
