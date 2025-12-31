import os
import sys
from pathlib import Path

import pytest

from trilogy import Dialects, Environment
from trilogy.execution import DuckDBConfig


def test_arrow_source():
    script = """
key fib_index int;
property fib_index.value int;

datasource fib_numbers(
    index:fib_index,
    fibonacci: value
)
grain (fib_index)
file `./fib.py`;


select
    sum(value) as total_fib;
"""

    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent),
        conf=DuckDBConfig(enable_python_datasources=True),
    )

    results = executor.execute_text(script)

    assert results[-1].fetchone()[0] > 100


def test_arrow_source_not_enabled_error():
    script = """
key fib_index int;
property fib_index.value int;

datasource fib_numbers(
    index:fib_index,
    fibonacci: value
)
grain (fib_index)
file `./fib.py`;


select
    sum(value) as total_fib;
"""

    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent),
    )

    with pytest.raises(Exception):
        executor.execute_text(script)


def test_uv_run_macro_error_message():
    """Test that uv_run macro gives helpful error when not configured."""
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent),
    )

    with pytest.raises(Exception) as exc_info:
        executor.execute_raw_sql("SELECT * FROM uv_run('test.py')")

    assert "enable_python_datasources" in str(exc_info.value)


def test_telemetry_optout_env_var():
    """Test that QUERY_FARM_TELEMETRY_OPTOUT is set when shellfs is used."""
    # Clear the env var first to ensure clean state
    os.environ.pop("QUERY_FARM_TELEMETRY_OPTOUT", None)

    if sys.platform == "win32":
        # On Windows, shellfs is not used, so env var should not be set
        executor = Dialects.DUCK_DB.default_executor(
            environment=Environment(working_path=Path(__file__).parent),
            conf=DuckDBConfig(enable_python_datasources=True),
        )
        # Env var may or may not be set on Windows (we don't use shellfs)
        # Just verify executor was created successfully
        assert executor is not None
    else:
        # On Unix/Linux/Mac, shellfs is used and env var should be set
        executor = Dialects.DUCK_DB.default_executor(
            environment=Environment(working_path=Path(__file__).parent),
            conf=DuckDBConfig(enable_python_datasources=True),
        )

        # Verify the environment variable was set
        assert (
            os.environ.get("QUERY_FARM_TELEMETRY_OPTOUT") == "1"
        ), "QUERY_FARM_TELEMETRY_OPTOUT should be set to '1' when shellfs is loaded"
