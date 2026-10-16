import json
import os
import sys
from pathlib import Path

import pytest

from tests.vgi_support import (  # noqa: F401
    flush_vgi_workers,
    require_vgi,
    skip_or_fail,
)
from trilogy import Dialects, Environment
from trilogy.core.enums import AddressType
from trilogy.core.models.datasource import Address
from trilogy.dialect.cancel import duckdb_side_cursor
from trilogy.dialect.config import DuckDBConfig, PythonDatasourceTransport
from trilogy.dialect.duckdb import (
    VGI_SETUP_SQL,
    attach_bridge,
    attach_resident,
    bridge_attach_sql,
    python_datasource_failure,
    resident_alias,
    resident_command,
    vgi_worker_command,
)
from trilogy.dialect.python_source import VGI_FAILURE_MARKER, PythonDatasourceError

SCRIPTS = Path(__file__).parent
ROOT = SCRIPTS.parents[2].as_posix()

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
    assert vgi_worker_command("/opt/my python/bin/python3", is_windows=False) == (
        "'/opt/my python/bin/python3' -m trilogy.dialect.duckdb_vgi"
    )


def test_setup_loads_vgi_without_starting_the_bridge():
    assert "LOAD vgi;" in VGI_SETUP_SQL
    assert "ATTACH" not in VGI_SETUP_SQL
    assert bridge_attach_sql("/opt/py/python", is_windows=False) == (
        "ATTACH IF NOT EXISTS 'trilogy_io' (TYPE vgi, LOCATION "
        "'/opt/py/python -m trilogy.dialect.duckdb_vgi')"
    )


def test_resident_command_adds_vgi_only_to_python_scripts():
    assert resident_command("/w/src.py") == [
        "uv",
        "run",
        "--no-project",
        "--quiet",
        "--with",
        "vgi-python",
        "/w/src.py",
        "--vgi",
    ]
    assert resident_command("/w/bin/landmarks") == ["/w/bin/landmarks", "--vgi"]


def test_resident_alias_follows_the_source_version(tmp_path: Path):
    source = tmp_path / "src.py"
    source.write_text("a")
    first = resident_alias(str(source))
    assert first == resident_alias(str(source))
    os.utime(source, ns=(1, 1))
    assert resident_alias(str(source)) != first


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


def test_a_truncated_vgi_payload_is_not_a_script_failure():
    assert python_datasource_failure(Exception(f"{VGI_FAILURE_MARKER} {{oops")) is None


def test_haybarn_url_needs_haybarn_installed(monkeypatch):
    import importlib.util

    monkeypatch.setattr(importlib.util, "find_spec", lambda name: None)
    with pytest.raises(ImportError, match=r"pytrilogy\[vgi\]"):
        DuckDBConfig(python_datasource_transport="vgi").connection_string()


def test_vgi_transport_connects_through_haybarn(monkeypatch):
    import importlib.util

    monkeypatch.setattr(importlib.util, "find_spec", lambda name: object())
    config = DuckDBConfig(path="/w/db.duckdb", python_datasource_transport="vgi")
    assert config.connection_string() == "haybarn:////w/db.duckdb"


def test_resident_attach_needs_a_duckdb_connection():
    sqlite = Dialects.SQLITE.default_executor(environment=Environment())
    assert duckdb_side_cursor(sqlite.connection) is None
    assert attach_resident(sqlite.connection, "/w/bin/landmarks") is None


def test_prepare_sources_skips_what_is_already_decided():
    from trilogy.dialect.duckdb import DuckDBDialect

    dialect = DuckDBDialect(
        config=DuckDBConfig(
            enable_python_datasources=True, python_datasource_transport="vgi"
        )
    )
    dialect._not_resident.add("/w/raw.py")
    dialect._bridge_attached = True
    addresses = [
        Address(location="/w/raw.py", type=AddressType.PYTHON_SCRIPT),
        Address(location="orders", type=AddressType.TABLE),
    ]
    # Nothing left to start, so the executor is never touched.
    dialect.prepare_sources(addresses, executor=None)  # type: ignore[arg-type]
    assert not dialect._residents


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
    require_vgi()
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=SCRIPTS),
        conf=DuckDBConfig(
            enable_python_datasources=True, python_datasource_transport="vgi"
        ),
    )
    yield executor
    executor.close()


def test_a_trilogy_io_source_runs_resident(vgi_executor):
    vgi_executor.parse_text(MODEL)
    rows = vgi_executor.execute_text("where id > 2 select id order by id asc;")[-1]

    assert rows.fetchall()
    resident = vgi_executor.generator._residents
    assert [Path(location).name for location in resident] == ["pushdown_source.py"]
    assert not vgi_executor.generator._bridge_attached


def test_a_raw_script_falls_back_to_the_bridge(vgi_executor):
    vgi_executor.parse_text("""
key index int;
property index.fibonacci int;
datasource fib (index: index, fibonacci: fibonacci) grain (index) file `./fib.py`;
""")
    rows = vgi_executor.execute_text(
        "where index > 23 select index, fibonacci order by index asc;"
    )[-1].fetchall()

    assert rows == [(24, 28657), (25, 46368)]
    assert not vgi_executor.generator._residents
    assert vgi_executor.generator._bridge_attached


def test_a_rust_source_runs_resident(vgi_executor):
    binary = SCRIPTS.parents[2] / "crates/trilogy-io/target/release/examples/landmarks"
    if not binary.with_suffix(".exe" if sys.platform == "win32" else "").exists():
        skip_or_fail("trilogy-io landmarks example not built")
    vgi_executor.parse_text(f"""
key lid int;
property lid.lname string;
property lid.lstate string;
datasource landmarks (id: lid, name: lname, state: lstate) grain (lid)
file `{binary.as_posix()}`;
""")
    rows = vgi_executor.execute_text(
        "where lstate = 'NY' select lid order by lid desc limit 2;"
    )[-1].fetchall()

    assert rows == [(97,), (93,)]
    if vgi_executor.generator._not_resident:
        skip_or_fail("landmarks was built without the vgi feature")
    assert len(vgi_executor.generator._residents) == 1


def test_one_query_starts_all_its_new_sources(vgi_executor, tmp_path: Path):
    """Each copy is its own source, so the query attaches all of them at once."""
    source = (SCRIPTS / "pushdown_source.py").read_text()
    copies = []
    for name in ("a", "b", "c"):
        copy = tmp_path / f"src_{name}.py"
        copy.write_text(source.replace('path = "../../../"', f'path = "{ROOT}"'))
        copies.append(copy)
    vgi_executor.parse_text(
        "\n".join(
            f"key {n}_id int;\n"
            f"datasource src_{n} (id: {n}_id) grain ({n}_id) file `{copy.as_posix()}`;"
            for n, copy in zip("abc", copies)
        )
    )
    rows = vgi_executor.execute_text(
        "select count(a_id) -> a, count(b_id) -> b, count(c_id) -> c;"
    )[-1].fetchall()

    assert len(set(rows[0])) == 1
    assert len(vgi_executor.generator._residents) == 3


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
    attach_bridge(vgi_executor.connection)
    path = (SCRIPTS / "pushdown_source.py").as_posix()
    rows = vgi_executor.execute_raw_sql(
        f"SELECT count(*) FROM trilogy_io.main.script('{path}', '') a "
        f"JOIN trilogy_io.main.script('{path}', '') b USING (id)"
    ).fetchall()

    assert rows[0][0] > 0


def test_vgi_script_failure_surfaces_as_script_error(vgi_executor):
    path = (SCRIPTS / "error.py").as_posix()
    attach_bridge(vgi_executor.connection)
    with pytest.raises(PythonDatasourceError) as raised:
        vgi_executor.execute_raw_sql(
            f"SELECT * FROM trilogy_io.main.script('{path}', '')"
        )

    assert "A helpful error describing what went wrong." in str(raised.value)
