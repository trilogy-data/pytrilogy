"""A source served over VGI (``--vgi``) answers exactly as when run once per request.

Resident mode delivers the same command line per call instead of per process,
so every contract case must return the rows the one-shot run writes -- from the
python and the rust implementation alike. Needs haybarn + vgi-python; the rust
half also needs the crate's ``vgi`` feature to build.
"""

import shlex
import shutil
import subprocess
import sys

import pyarrow as pa
import pytest

from tests.io.test_conformance import (
    CASES,
    CATALOG_CASES,
    CRATE,
    PY_CATALOG,
    PY_SOURCE,
    _binary_path,
    rows,
    run_python,
)
from tests.vgi_support import (  # noqa: F401
    flush_vgi_workers,
    require_vgi,
    skip_or_fail,
)
from trilogy.dialect.duckdb import VGI_SOURCE_CATALOG, vgi_failure, vgi_location

require_vgi()
import haybarn

IS_WINDOWS = sys.platform == "win32"
# Unordered requests come back in source order either way; ties are the one
# allowed difference (see test_conformance), so those cases are left out.
RESIDENT_CASES = [case for case in CASES if "name:asc" not in " ".join(case)]


def attach(argv: list[str]):
    con = haybarn.connect()
    con.execute("LOAD vgi")
    location = vgi_location([*argv, "--vgi"], IS_WINDOWS).replace("'", "''")
    con.execute(
        f"ATTACH '{VGI_SOURCE_CATALOG}' AS src (TYPE vgi, LOCATION '{location}')"
    )
    return con


def served(con, args: list[str]) -> list[dict]:
    sql_args = shlex.join(args).replace("'", "''")
    query = f"SELECT * FROM src.main.source('{sql_args}')"
    return con.execute(query).to_arrow_table().to_pylist()


@pytest.fixture(scope="module")
def python_worker():
    con = attach([sys.executable, str(PY_SOURCE)])
    yield con
    con.close()


@pytest.fixture(scope="module")
def python_catalog_worker():
    con = attach([sys.executable, str(PY_CATALOG)])
    yield con
    con.close()


@pytest.fixture(scope="module")
def rust_examples() -> None:
    if shutil.which("cargo") is None:
        skip_or_fail("cargo not available")
    build = subprocess.run(
        [
            "cargo",
            "build",
            "--release",
            "--example",
            "landmarks",
            "--example",
            "catalog",
            "--features",
            "vgi",
        ],
        cwd=CRATE,
        capture_output=True,
        check=False,
    )
    if build.returncode != 0:
        skip_or_fail(f"vgi feature build failed: {build.stderr.decode()[-1000:]}")


@pytest.fixture(scope="module")
def rust_worker(rust_examples):
    con = attach([str(_binary_path())])
    yield con
    con.close()


@pytest.fixture(scope="module")
def rust_catalog_worker(rust_examples):
    con = attach([str(_binary_path("catalog"))])
    yield con
    con.close()


@pytest.mark.parametrize(
    "args", RESIDENT_CASES, ids=lambda a: " ".join(a) or "no-flags"
)
def test_python_served_matches_one_shot(python_worker, args):
    assert served(python_worker, args) == rows(run_python(args).stdout)


@pytest.mark.parametrize(
    "args", RESIDENT_CASES, ids=lambda a: " ".join(a) or "no-flags"
)
def test_rust_served_matches_one_shot(rust_worker, args):
    assert served(rust_worker, args) == rows(run_python(args).stdout)


@pytest.mark.parametrize("worker", ["python_worker", "rust_worker"])
def test_served_failures_carry_the_contract_error(request, worker):
    con = request.getfixturevalue(worker)
    with pytest.raises(Exception) as raised:
        served(con, ["--filter", "nonexistent = 1"])
    error = vgi_failure(str(raised.value))

    assert error is not None
    assert error.return_code == 65
    assert error.reported is not None
    assert error.reported["type"] == "ContractError"
    assert "Available columns" in error.reported["message"]


@pytest.mark.parametrize("worker", ["python_catalog_worker", "rust_catalog_worker"])
@pytest.mark.parametrize("args", CATALOG_CASES, ids=" ".join)
def test_one_worker_serves_every_source_of_a_program(request, worker, args):
    con = request.getfixturevalue(worker)
    assert served(con, args) == rows(run_python(args, PY_CATALOG).stdout)


DECLARED = pa.schema([("id", pa.int64()), ("state", pa.string())])


def _rows():
    return [{"id": 1, "state": "CA"}]


def _owns_columns(columns=None):
    return [{"id": 1, "state": "CA"}]


@pytest.mark.parametrize(
    "fn, args, expected",
    [
        (_rows, "--limit 3 --filter 'state = CA'", DECLARED),
        (
            _rows,
            "--columns state,id",
            pa.schema([DECLARED.field(1), DECLARED.field(0)]),
        ),
        (_rows, "--columns nope", None),
        (_rows, "--order-by nope", None),
        (_owns_columns, "", None),
    ],
)
def test_bind_answers_from_a_declared_schema(fn, args, expected):
    """Bind need not run a source whose schema the request cannot reshape."""
    from trilogy.io.vgi import declared_schema

    assert declared_schema(fn, DECLARED, args) == expected


def test_bind_runs_a_source_that_declares_nothing_or_is_not_served():
    from trilogy.io.vgi import declared_schema

    assert declared_schema(_rows, None, "") is None
    assert declared_schema({"a": _rows}, None, "--source b") is None
