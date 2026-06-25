from pathlib import Path

import pytest

from trilogy import Dialects, Environment
from trilogy.core.enums import ValidationScope
from trilogy.core.validation.environment import validate_environment
from trilogy.dialect import duckdb_uv
from trilogy.dialect.duckdb import get_python_datasource_setup_sql
from trilogy.dialect.duckdb_uv import is_retryable_uv_error, run_with_retry
from trilogy.execution import DuckDBConfig


class FakeRetryableUvRun:
    def __init__(self) -> None:
        self.attempts = 0

    def __call__(
        self, script: str, args: str, output_path: Path, error_path: Path
    ) -> int:
        self.attempts += 1
        if self.attempts == 1:
            error_path.write_text("failed to acquire file lock", encoding="utf-8")
            return 1
        output_path.write_text("ok", encoding="utf-8")
        return 0


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
    """Test that uv_run macro surfaces script stderr."""
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent),
        conf=DuckDBConfig(enable_python_datasources=True),
    )
    script_path = Path(__file__).parent / "error.py"
    with pytest.raises(Exception) as exc_info:
        executor.execute_raw_sql(f"SELECT * FROM uv_run('{script_path}')")

    assert "Pipe process exited" in str(exc_info.value)


def test_windows_uv_run_uses_retry_wrapper():
    sql = get_python_datasource_setup_sql(enabled=True, is_windows=True)

    assert "trilogy.dialect.duckdb_uv" in sql


def test_uv_retryable_error_detection():
    assert is_retryable_uv_error(
        "error: failed to acquire file lock: The process cannot access the file"
    )
    assert not is_retryable_uv_error("SyntaxError: A helpful error")


def test_uv_wrapper_retries_retryable_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    output_path = tmp_path / "out.arrow"
    error_path = tmp_path / "out.err"
    fake_run_uv = FakeRetryableUvRun()

    monkeypatch.setattr(duckdb_uv, "_run_uv", fake_run_uv)

    assert run_with_retry("script.py", "", output_path, error_path) == 0
    assert fake_run_uv.attempts == 2
    assert output_path.read_text(encoding="utf-8").strip() == "ok"
    assert capsys.readouterr().out.strip() == '{"name": "done"}'


def test_validation_caches_python_datasource_per_run():
    script = """
key fib_index int;
property fib_index.value int;

datasource fib_numbers(
    index:fib_index,
    fibonacci: value
)
grain (fib_index)
file `./fib.py`;
"""

    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent),
        conf=DuckDBConfig(enable_python_datasources=True),
    )
    executor.parse_text(script)

    recorded_sql: list[str] = []
    original_execute_raw_sql = executor.execute_raw_sql

    def tracking_execute_raw_sql(command, variables=None, local_concepts=None):
        recorded_sql.append(str(command))
        return original_execute_raw_sql(
            command,
            variables=variables,
            local_concepts=local_concepts,
        )

    executor.execute_raw_sql = tracking_execute_raw_sql  # type: ignore[method-assign]

    validate_environment(
        executor.environment,
        scope=ValidationScope.DATASOURCES,
        exec=executor,
    )

    uv_run_calls = [sql for sql in recorded_sql if "uv_run(" in sql]
    assert len(uv_run_calls) == 1
    assert any("CREATE TEMP TABLE" in sql for sql in recorded_sql)
