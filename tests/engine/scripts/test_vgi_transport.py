import json
from pathlib import Path

import pytest

from trilogy import Dialects, Environment
from trilogy.dialect.config import DuckDBConfig, PythonDatasourceTransport
from trilogy.dialect.duckdb import (
    get_vgi_setup_sql,
    python_datasource_failure,
    vgi_worker_command,
)
from trilogy.dialect.python_source import VGI_FAILURE_MARKER, PythonDatasourceError

SCRIPTS = Path(__file__).parent

MODEL = """
key id int;
property id.state string;
property id.label string;
property id.score float;

datasource src (id: id, state: state, label: label, score: score)
grain (id)
file `./pushdown_source.py`;
"""


def test_transport_defaults_to_shell():
    assert DuckDBConfig().python_datasource_transport == PythonDatasourceTransport.SHELL
    assert DuckDBConfig().connection_string() == "duckdb:///:memory:"


def test_unknown_transport_is_rejected():
    with pytest.raises(ValueError):
        DuckDBConfig(python_datasource_transport="carrier-pigeon")


def test_worker_command_survives_cmd_quote_stripping():
    assert vgi_worker_command("C:/Program Files/py.exe", is_windows=True) == (
        '""C:/Program Files/py.exe" -m trilogy.dialect.duckdb_vgi"'
    )
    assert vgi_worker_command("/usr/bin/python3", is_windows=False) == (
        '"/usr/bin/python3" -m trilogy.dialect.duckdb_vgi'
    )


def test_setup_attaches_the_bridge_worker():
    sql = get_vgi_setup_sql("/opt/py's/python", is_windows=False)
    assert "LOAD vgi;" in sql
    assert (
        "ATTACH IF NOT EXISTS 'trilogy_io' (TYPE vgi, LOCATION "
        "'\"/opt/py''s/python\" -m trilogy.dialect.duckdb_vgi');"
    ) in sql


def test_vgi_failure_rebuilds_the_script_error():
    payload = {"script": "/w/a.py", "return_code": 65, "stderr": "ValueError: bad\n"}
    error = python_datasource_failure(
        Exception(
            "Invalid Input Error: VGI Worker Exception: RuntimeError: "
            f"{VGI_FAILURE_MARKER} {json.dumps(payload)}\nTraceback (most recent ..."
        )
    )

    assert isinstance(error, PythonDatasourceError)
    assert (error.script, error.return_code) == ("/w/a.py", 65)
    assert str(error) == (
        "Python datasource script '/w/a.py' failed (exit code 65): ValueError: bad"
    )


def test_vgi_renders_the_bridge_call_with_pushdown_args():
    from trilogy.dialect.duckdb import DuckDBDialect

    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=SCRIPTS),
        conf=DuckDBConfig(enable_python_datasources=True),
    )
    executor.parse_text(MODEL)
    executor.generator = DuckDBDialect(
        config=DuckDBConfig(
            enable_python_datasources=True, python_datasource_transport="vgi"
        )
    )
    sql = executor.generate_sql("where state = 'CA' select id;")[-1]

    assert "trilogy_io.main.script('" in sql
    assert "pushdown_source.py', '--filter ''state=CA''')" in sql
    assert "uv_run" not in sql


@pytest.fixture
def vgi_executor():
    pytest.importorskip("haybarn")
    pytest.importorskip("vgi")
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=SCRIPTS),
        conf=DuckDBConfig(
            enable_python_datasources=True, python_datasource_transport="vgi"
        ),
    )
    yield executor
    executor.close()


def test_vgi_transport_matches_shell(vgi_executor):
    shell = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=SCRIPTS),
        conf=DuckDBConfig(enable_python_datasources=True),
    )
    query = "where id > 2 select id, state, score order by id asc;"
    results = []
    for executor in (shell, vgi_executor):
        executor.parse_text(MODEL)
        results.append(executor.execute_text(query)[-1].fetchall())

    assert results[0] == results[1]
    assert results[1]


def test_vgi_scans_one_script_twice_in_one_query(vgi_executor):
    vgi_executor.parse_text(MODEL)
    path = (SCRIPTS / "pushdown_source.py").as_posix()
    rows = vgi_executor.execute_raw_sql(
        f"SELECT count(*) FROM trilogy_io.main.script('{path}', '') a "
        f"JOIN trilogy_io.main.script('{path}', '') b USING (id)"
    ).fetchall()

    assert rows[0][0] > 0


def test_vgi_script_failure_surfaces_as_script_error(vgi_executor):
    path = (SCRIPTS / "error.py").as_posix()
    with pytest.raises(PythonDatasourceError) as raised:
        vgi_executor.execute_raw_sql(
            f"SELECT * FROM trilogy_io.main.script('{path}', '')"
        )

    assert "A helpful error describing what went wrong." in str(raised.value)
