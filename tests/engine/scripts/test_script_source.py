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


def test_uv_run_error_passing():
    """Test that uv_run macro gives helpful error when not configured."""
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent),
        conf=DuckDBConfig(enable_python_datasources=True),
    )
    script_path = Path(__file__).parent / "error.py"
    with pytest.raises(Exception) as exc_info:
        executor.execute_raw_sql(f"SELECT * FROM uv_run('{script_path}')")

        assert "A helpful error describing what went wrong." in str(exc_info.value)
