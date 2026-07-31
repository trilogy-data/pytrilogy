"""End-to-end python datasource runs against a real BigQuery project.

Reads `.env.secrets` (then `.env`) from the repo root if present and only sets
vars that aren't already in the environment. Credentials come from application
default credentials.

  TRILOGY_BIGQUERY_PROJECT          (optional; defaults to the ADC project)
  TRILOGY_BIGQUERY_STAGING_URI      e.g. `gs://my-bucket/trilogy-staging`
                                    ADC must be able to write and delete here.
  TRILOGY_BIGQUERY_STAGING_DATASET  (optional) exercises the external-table
                                    mode instead of per-job temp definitions.

`test_temp_definitions_read_a_public_object` needs no staging bucket — it
covers the BigQuery half (tableDefinitions, bare table name, no catalog entry)
against a public GCS parquet file, so it runs whenever ADC is available.
"""

import os
from collections.abc import Generator
from pathlib import Path

import pytest

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.dialect.bigquery_engine import BigQueryConnection
from trilogy.dialect.config import BigQueryConfig

pytestmark = pytest.mark.bigquery_execution

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = Path(__file__).parent.parent / "scripts"

PUBLIC_PARQUET = "gs://cloud-samples-data/bigquery/us-states/us-states.parquet"

FIB_MODEL = """
key fib_index int;
property fib_index.value int;

datasource fib_numbers(
    index:fib_index,
    fibonacci: value
)
grain (fib_index)
file `./fib.py`;
"""

# Same script address, but staging is redirected at a public object so the
# BigQuery side can be exercised without a writable bucket.
STATES_MODEL = """
key state_name string;
property state_name.post_abbr string;

datasource states(
    name: state_name,
    post_abbr: post_abbr
)
grain (state_name)
file `./fib.py`;
"""


def _load_dotenv(path: Path) -> None:
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip().strip('"').strip("'")
        if value:
            os.environ.setdefault(key.strip(), value)


_dotenv_loaded = False


def _load_dotenv_files() -> None:
    """Seed the environment from the dotenv files, once, on first lookup.

    Lazy rather than at import time: pytest imports this module during
    collection even when `bigquery_execution` is deselected, so seeding the
    process environment at module scope would leak into every other suite.
    """
    global _dotenv_loaded
    if _dotenv_loaded:
        return
    _dotenv_loaded = True
    for candidate in (".env.secrets", ".env"):
        _load_dotenv(REPO_ROOT / candidate)


def _env(name: str) -> str | None:
    _load_dotenv_files()
    value = os.environ.get(name)
    return value if value else None


def _has_adc() -> bool:
    _load_dotenv_files()
    try:
        from google.auth import default

        default()
        return True
    except Exception:
        return False


def make_executor(**config_overrides) -> Executor:
    return Dialects.BIGQUERY.default_executor(
        environment=Environment(working_path=SCRIPTS),
        conf=BigQueryConfig(
            project=_env("TRILOGY_BIGQUERY_PROJECT"),
            enable_python_datasources=True,
            **config_overrides,
        ),
    )


@pytest.fixture(scope="module")
def staging_uri() -> str:
    uri = _env("TRILOGY_BIGQUERY_STAGING_URI")
    if not uri:
        pytest.skip("TRILOGY_BIGQUERY_STAGING_URI not set")
    return uri


@pytest.fixture
def public_object_executor() -> Generator[Executor, None, None]:
    if not _has_adc():
        pytest.skip("no application default credentials")
    executor = make_executor(staging_uri="gs://unused-no-write-happens/prefix")
    # The BigQuery half is what this test covers; skip the GCS write by
    # pointing the staged artifact at a public parquet object.
    executor.generator.python_staging().stage = (  # type: ignore[attr-defined]
        lambda address, force=False: PUBLIC_PARQUET
    )
    try:
        yield executor
    finally:
        executor.close()


def test_temp_definitions_read_a_public_object(public_object_executor: Executor):
    executor = public_object_executor
    executor.parse_text(STATES_MODEL)

    sql = executor.generate_sql("select count(state_name) as state_count;")[0]
    # a bare temp name: no project, no dataset, nothing in the catalog
    assert "trilogy_py_fib_" in sql
    assert "`states`" in sql

    rows = executor.execute_text("select count(state_name) as state_count;")[
        -1
    ].fetchall()
    assert rows[0][0] == 50

    rows = executor.execute_text(
        "select state_name, post_abbr order by state_name asc limit 3;"
    )[-1].fetchall()
    assert [tuple(r) for r in rows] == [
        ("Alabama", "AL"),
        ("Alaska", "AK"),
        ("Arizona", "AZ"),
    ]

    connection = executor.connection
    assert isinstance(connection, BigQueryConnection)
    assert list(connection.external_tables) == ["trilogy_py_fib_c24cc58f7d45"]


@pytest.fixture(scope="module")
def temp_definition_executor(staging_uri: str) -> Generator[Executor, None, None]:
    executor = make_executor(staging_uri=staging_uri)
    try:
        yield executor
    finally:
        executor.close()


def test_python_datasource_round_trip(temp_definition_executor: Executor):
    temp_definition_executor.parse_text(FIB_MODEL)
    results = temp_definition_executor.execute_text("select sum(value) as total_fib;")

    assert results[-1].fetchone()[0] == 121392


def test_python_datasource_joins_and_filters(temp_definition_executor: Executor):
    temp_definition_executor.parse_text(FIB_MODEL)
    results = temp_definition_executor.execute_text(
        "select fib_index, value where fib_index <= 5 order by fib_index asc;"
    )
    rows = [tuple(r) for r in results[-1].fetchall()]

    assert rows == [(1, 0), (2, 1), (3, 1), (4, 2), (5, 3)]


def test_python_datasource_staged_once_per_executor(temp_definition_executor: Executor):
    temp_definition_executor.parse_text(FIB_MODEL)
    staging = temp_definition_executor.generator.python_staging()  # type: ignore[attr-defined]
    staging.staged.clear()

    temp_definition_executor.execute_text("select sum(value) as total_fib;")
    temp_definition_executor.execute_text("select count(fib_index) as fib_count;")

    assert len(staging.staged) == 1


def test_close_removes_the_staged_object(staging_uri: str):
    from trilogy.dialect.python_source import normalize_object_uri

    executor = make_executor(staging_uri=staging_uri)
    executor.parse_text(FIB_MODEL)
    executor.execute_text("select sum(value) as total_fib;")

    staging = executor.generator.python_staging()  # type: ignore[attr-defined]
    uri = next(iter(staging.staged.values()))
    executor.close()

    from pyarrow import fs as pafs

    filesystem, path = pafs.FileSystem.from_uri(normalize_object_uri(uri))
    assert filesystem.get_file_info(path).type == pafs.FileType.NotFound


def test_external_table_mode_round_trip(staging_uri: str):
    dataset = _env("TRILOGY_BIGQUERY_STAGING_DATASET")
    if not dataset:
        pytest.skip("TRILOGY_BIGQUERY_STAGING_DATASET not set")
    executor = make_executor(staging_uri=staging_uri, staging_dataset=dataset)
    try:
        executor.parse_text(FIB_MODEL)
        results = executor.execute_text("select sum(value) as total_fib;")
        assert results[-1].fetchone()[0] == 121392

        staging = executor.generator.python_staging()  # type: ignore[attr-defined]
        assert staging.uses_external_tables
        # the external table must outlive the session, so its object stays
        assert staging.cleanup() == []
    finally:
        executor.close()
