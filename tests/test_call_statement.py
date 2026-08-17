import datetime
from pathlib import Path

import pytest

from trilogy import Dialects, Environment
from trilogy.core.statements.execute import ProcessedCallStatement
from trilogy.dialect.python_source import UV_RUN_PREFIX, build_script_command
from trilogy.executor import serialize_call_arg
from trilogy.parsing.render import Renderer


def test_call_parse_bare():
    exec = Dialects.DUCK_DB.default_executor()
    results = exec.parse_text("call `./notify.py`;")
    assert len(results) == 1
    statement = results[0]
    assert isinstance(statement, ProcessedCallStatement)
    assert statement.target == "./notify.py"
    assert statement.query is None


def test_call_parse_with_args():
    exec = Dialects.DUCK_DB.default_executor()
    results = exec.parse_text(
        "call `./send.py` from select 'daily.html' -> file, 'Daily' -> subject;"
    )
    statement = results[0]
    assert isinstance(statement, ProcessedCallStatement)
    assert statement.query is not None
    visible = [
        c.address.rsplit(".", 1)[-1]
        for c in statement.query.output_columns
        if c.address not in statement.query.hidden_columns
    ]
    assert visible == ["file", "subject"]


def test_call_execution(tmp_path: Path):
    out_file = tmp_path / "args.txt"
    script = tmp_path / "echo_args.py"
    script.write_text(
        "import sys, pathlib\n"
        f"pathlib.Path(r'{out_file}').write_text(' '.join(sys.argv[1:]))\n",
        newline="\n",
    )
    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    results = exec.execute_text(
        "call `./echo_args.py` from select 'daily.html' -> file, 42 -> count_val;"
    )
    assert results[-1].as_dict() == [{"target": "./echo_args.py", "status": "success"}]
    assert out_file.read_text() == "--file daily.html --count_val 42"


def test_call_execution_failure_surfaces_stderr(tmp_path: Path):
    script = tmp_path / "boom.py"
    script.write_text(
        "import sys\nprint('it broke', file=sys.stderr)\nsys.exit(3)\n",
        newline="\n",
    )
    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    with pytest.raises(RuntimeError, match=r"exit 3.*it broke"):
        exec.execute_text("call `./boom.py`;")


def test_call_multi_row_select_rejected_at_parse():
    exec = Dialects.DUCK_DB.default_executor()
    with pytest.raises(Exception, match="exactly one row"):
        exec.parse_text("""const arr <- [1,2,3];
auto x <- unnest(arr);
call `./x.py` from select x;""")


def test_call_multi_row_select_allowed_with_limit_one():
    exec = Dialects.DUCK_DB.default_executor()
    results = exec.parse_text("""const arr <- [1,2,3];
auto x <- unnest(arr);
call `./x.py` from select x limit 1;""")
    assert isinstance(results[-1], ProcessedCallStatement)


def test_call_zero_rows_rejected_at_runtime(tmp_path: Path):
    script = tmp_path / "never_runs.py"
    script.write_text("raise SystemExit(1)\n", newline="\n")
    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    with pytest.raises(ValueError, match="exactly one row, got 0"):
        exec.execute_text("""const arr <- [1,2,3];
auto x <- unnest(arr);
call `./never_runs.py` from select x where x = 99 limit 1;""")


def test_call_fmt_round_trip():
    exec = Dialects.DUCK_DB.default_executor()
    text = "call `./send.py` from select 'daily.html' -> file, 'Daily' -> subject;"
    _, statements = exec.environment.parse(text)
    rendered = Renderer(environment=exec.environment).to_string(statements[-1])
    exec2 = Dialects.DUCK_DB.default_executor()
    _, statements2 = exec2.environment.parse(rendered)
    rendered2 = Renderer(environment=exec2.environment).to_string(statements2[-1])
    assert rendered == rendered2


def test_call_fmt_round_trip_bare():
    exec = Dialects.DUCK_DB.default_executor()
    _, statements = exec.environment.parse("call `./notify.py`;")
    rendered = Renderer(environment=exec.environment).to_string(statements[-1])
    assert rendered == "call `./notify.py`;"
    exec2 = Dialects.DUCK_DB.default_executor()
    _, statements2 = exec2.environment.parse(rendered)
    rendered2 = Renderer(environment=exec2.environment).to_string(statements2[-1])
    assert rendered == rendered2


def test_call_compile_is_comment_sql():
    exec = Dialects.DUCK_DB.default_executor()
    results = exec.parse_text("call `./notify.py`;")
    sql = exec.generator.compile_statement(results[0])
    assert sql.startswith("--Trilogy call statements")


def test_serialize_call_arg():
    assert serialize_call_arg(None) is None
    assert serialize_call_arg(True) == "true"
    assert serialize_call_arg(False) == "false"
    assert serialize_call_arg("x y") == "x y"
    assert serialize_call_arg(42) == "42"
    assert serialize_call_arg(1.5) == "1.5"
    assert serialize_call_arg(datetime.date(2026, 8, 17)) == "2026-08-17"
    assert (
        serialize_call_arg(datetime.datetime(2026, 8, 17, 5, 10))
        == "2026-08-17T05:10:00"
    )
    assert serialize_call_arg([1, "a"]) == '[1, "a"]'


def test_build_script_command():
    assert build_script_command("./foo.py") == [*UV_RUN_PREFIX, "./foo.py"]
    assert build_script_command("./foo.exe") == ["./foo.exe"]
    assert build_script_command("./foo") == ["./foo"]
