from pathlib import Path

import pytest

from trilogy import Dialects, Environment, Executor
from trilogy.core.enums import ValidationScope
from trilogy.core.validation.environment import validate_environment
from trilogy.dialect import duckdb_uv
from trilogy.dialect.duckdb import (
    get_python_datasource_setup_sql,
    python_datasource_failure,
)
from trilogy.dialect.duckdb_uv import is_retryable_uv_error, run_with_retry
from trilogy.dialect.python_source import PythonDatasourceError
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


PYTHON_SOURCE_MODEL = """
key fib_index int;
property fib_index.value int;

datasource fib_numbers(
    index:fib_index,
    fibonacci: value
)
grain (fib_index)
file `./fib.py`;
"""


class RecordingPrepare:
    """Stands in for dialect.prepare_sources, recording script basenames."""

    def __init__(self) -> None:
        self.seen: list[str] = []

    def __call__(self, addresses, run_sql) -> None:
        self.seen.extend(Path(a.location).name for a in addresses)


def python_source_executor() -> Executor:
    return Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent),
        conf=DuckDBConfig(enable_python_datasources=True),
    )


def prepared_sources(executor: Executor, script: str) -> list[str]:
    """Script basenames the dialect was offered while running `script`."""
    spy = RecordingPrepare()
    executor.generator.prepare_sources = spy  # type: ignore[method-assign]
    executor.execute_text(PYTHON_SOURCE_MODEL + script)
    return spy.seen


def test_lazy_dialects_skip_source_preparation_entirely():
    executor = python_source_executor()
    assert prepared_sources(executor, "select sum(value) as total_fib;") == []


@pytest.mark.parametrize(
    "statement",
    [
        "select sum(value) as total_fib;",
        "persist fib_out into fib_summary from select fib_index, value;",
        "copy into csv 'source_prep.csv' from select fib_index, value;",
        "chart layer bar (x_axis <- fib_index, y_axis <- value);",
    ],
)
def test_every_execution_path_offers_python_sources_to_the_dialect(
    statement: str, tmp_path: Path
):
    """Dialects that must materialize a script (e.g. BigQuery) get first refusal.

    Regression: chart layers and copy targets compiled their SQL directly off
    the generator, so a script source was never staged for them.
    """
    executor = python_source_executor()
    executor.generator.REQUIRES_SOURCE_PREPARATION = True

    statement = statement.replace(
        "'source_prep.csv'", f"'{(tmp_path / 'source_prep.csv').as_posix()}'"
    )
    assert prepared_sources(executor, statement) == ["fib.py"]


def test_rendering_sql_does_not_prepare_sources():
    """generate_sql must stay side-effect free — no script runs, nothing staged."""
    executor = python_source_executor()
    executor.generator.REQUIRES_SOURCE_PREPARATION = True
    spy = RecordingPrepare()
    executor.generator.prepare_sources = spy  # type: ignore[method-assign]

    executor.parse_text(PYTHON_SOURCE_MODEL)
    executor.generate_sql("select sum(value) as total_fib;")

    assert spy.seen == []


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

    with pytest.raises(ValueError, match="enable_python_datasources"):
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
    with pytest.raises(PythonDatasourceError) as exc_info:
        executor.execute_raw_sql(f"SELECT * FROM uv_run('{script_path}')")

    message = str(exc_info.value)
    assert message.startswith(f"Python datasource script '{script_path}' failed")
    headline = message.splitlines()[0]
    assert headline.endswith("SyntaxError: A helpful error describing what went wrong.")
    assert "Traceback (most recent call last)" in message
    assert "Pipe process exited" not in message


def test_uv_run_script_stderr_stays_off_the_terminal(capfd: pytest.CaptureFixture[str]):
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent),
        conf=DuckDBConfig(enable_python_datasources=True),
    )
    script_path = Path(__file__).parent / "error.py"
    with pytest.raises(PythonDatasourceError):
        executor.execute_raw_sql(f"SELECT * FROM uv_run('{script_path}')")

    assert "Traceback" not in capfd.readouterr().err


def test_posix_pipe_failure_reads_the_stderr_sidecar(tmp_path: Path):
    sidecar = tmp_path / "abc.err"
    sidecar.write_text(
        "Traceback (most recent call last):\n"
        '  File "/w/raw/a.py", line 3, in <module>\n'
        "    fetch()\n"
        "requests.exceptions.HTTPError: 403 Client Error: Forbidden\n",
        encoding="utf-8",
    )
    error = python_datasource_failure(
        Exception(
            "(_duckdb.IOException) IO Error: Pipe process exited abnormally code=1: "
            f"uv run --no-project --quiet /w/raw/a.py --limit 5 2>'{sidecar.as_posix()}' |"
            "\n\nLINE 7:     uv_run('/w/raw/a.py')"
        )
    )

    assert error is not None
    assert (error.script, error.return_code) == ("/w/raw/a.py", 1)
    assert str(error).splitlines()[0] == (
        "Python datasource script '/w/raw/a.py' failed (exit code 1): "
        "requests.exceptions.HTTPError: 403 Client Error: Forbidden"
    )
    assert 'File "/w/raw/a.py", line 3' in str(error)


def test_windows_pipe_failure_names_the_script_not_the_wrapper(tmp_path: Path):
    sidecar = tmp_path / "abc.err"
    sidecar.write_text("ValueError: bad\n", encoding="utf-8")
    error = python_datasource_failure(
        Exception(
            "IO Error: Pipe process exited abnormally code=1: call "
            '"C:/py.exe" -m trilogy.dialect.duckdb_uv "C:/t/abc.arrow" '
            f'"{sidecar.as_posix()}" "C:\\raw\\a.py" "" |'
        )
    )

    assert error is not None
    assert error.script == "C:\\raw\\a.py"
    assert str(error) == (
        "Python datasource script 'C:\\raw\\a.py' failed (exit code 1): ValueError: bad"
    )


def test_non_pipe_errors_are_not_script_failures():
    assert python_datasource_failure(Exception("Binder Error: no column")) is None


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
