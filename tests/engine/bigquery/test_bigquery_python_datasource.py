from pathlib import Path

import pyarrow.parquet as pq
import pytest

from trilogy.core.enums import AddressType
from trilogy.core.models.datasource import Address
from trilogy.core.models.environment import Environment
from trilogy.core.models.execute import collect_source_addresses
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.dialect.bigquery_engine import BigQueryConnection
from trilogy.dialect.bigquery_staging import BigQueryPythonStaging
from trilogy.dialect.config import BigQueryConfig, DuckDBConfig
from trilogy.staging import StagingConfig

SCRIPTS = Path(__file__).parent.parent / "scripts"
FIB = SCRIPTS / "fib.py"

MODEL = """
key fib_index int;
property fib_index.value int;

datasource fib_numbers(
    index:fib_index,
    fibonacci: value
)
grain (fib_index)
file `./fib.py`;

select sum(value) as total_fib;
"""


def python_address(location: Path = FIB) -> Address:
    return Address(location=str(location), type=AddressType.PYTHON_SCRIPT)


def enabled_dialect(**overrides) -> BigqueryDialect:
    kwargs: dict = {
        "project": "my-project",
        "staging_uri": "gs://my-bucket/trilogy",
        "enable_python_datasources": True,
    }
    kwargs.update(overrides)
    return BigqueryDialect(config=BigQueryConfig(**kwargs), instance_id="exec-1")


class RecordingSql:
    """Stands in for executor.execute_raw_sql."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def __call__(self, sql: str, **kwargs):
        self.statements.append(sql)


class FakeExecutor:
    def __init__(self, connection=None) -> None:
        self.execute_raw_sql = RecordingSql()
        self.connection = connection if connection is not None else object()

    @property
    def statements(self) -> list[str]:
        return self.execute_raw_sql.statements


def local_staging(tmp_path: Path, **overrides) -> BigQueryPythonStaging:
    kwargs: dict = {"root_uri": tmp_path.as_posix(), "project": "my-project"}
    kwargs.update(overrides)
    return BigQueryPythonStaging(**kwargs)


# --- naming / mode selection -------------------------------------------------


def test_temp_definition_mode_uses_a_bare_table_name():
    staging = BigQueryPythonStaging(
        root_uri="gs://my-bucket/trilogy", project="my-project", instance_id="exec-1"
    )
    assert not staging.uses_external_tables
    reference = staging.table_reference(python_address())
    assert reference.startswith("`trilogy_py_fib_")
    assert "my-project" not in reference


def test_temp_definition_objects_are_namespaced_per_executor():
    staging = BigQueryPythonStaging(root_uri="gs://b/p", instance_id="exec-1")
    assert staging.object_uri(python_address()).startswith("gs://b/p/exec-1/")


def test_external_table_mode_uses_a_qualified_name_and_stable_path():
    staging = BigQueryPythonStaging(
        root_uri="gs://my-bucket/trilogy",
        dataset="staging",
        project="my-project",
        instance_id="exec-1",
    )
    assert staging.uses_external_tables
    assert staging.table_reference(python_address()).startswith(
        "`my-project.staging.trilogy_py_fib_"
    )
    # no instance namespace: the external table must outlive this executor
    assert staging.object_uri(python_address()).startswith(
        "gs://my-bucket/trilogy/trilogy_py_fib_"
    )


def test_staging_normalizes_root_uri():
    staging = BigQueryPythonStaging(root_uri="gcs://bucket/prefix//")
    assert staging.root_uri == "gs://bucket/prefix/"


def test_staging_respects_fully_qualified_dataset():
    staging = BigQueryPythonStaging(
        root_uri="gs://b/p", dataset="other-project.staging", project="my-project"
    )
    assert "`other-project.staging." in staging.table_reference(python_address())


def test_staging_omits_project_when_unset():
    staging = BigQueryPythonStaging(root_uri="gs://b/p", dataset="staging")
    assert staging.table_reference(python_address()).startswith("`staging.trilogy_py_")


# --- staging + external table DDL --------------------------------------------


def test_stage_streams_the_script_to_the_object_uri(tmp_path: Path):
    staging = local_staging(tmp_path)
    uri = staging.stage(python_address())

    assert uri is not None
    assert pq.read_table(uri).num_rows == 25


def test_stage_runs_a_script_once_per_instance(tmp_path: Path):
    staging = local_staging(tmp_path)
    address = python_address()

    assert staging.stage(address) is not None
    assert staging.stage(address) is None
    assert staging.stage(address, force=True) is not None


def test_materialize_creates_the_external_table(tmp_path: Path):
    staging = local_staging(tmp_path, dataset="staging")
    run_sql = RecordingSql()

    uri = staging.materialize(python_address(), run_sql)

    assert len(run_sql.statements) == 1
    ddl = run_sql.statements[0]
    assert "CREATE OR REPLACE EXTERNAL TABLE `my-project.staging.trilogy_py_fib_" in ddl
    assert "format = 'PARQUET'" in ddl
    assert f"uris = ['{uri}']" in ddl


# --- cleanup -----------------------------------------------------------------


def test_cleanup_deletes_objects_this_instance_staged(tmp_path: Path):
    staging = local_staging(tmp_path, instance_id="exec-1")
    uri = staging.stage(python_address())
    assert uri is not None
    assert Path(uri).exists()

    assert staging.cleanup() == [uri]
    assert not Path(uri).exists()
    assert staging.staged == {}


def test_cleanup_leaves_external_table_objects_alone(tmp_path: Path):
    staging = local_staging(tmp_path, dataset="staging")
    uri = staging.stage(python_address())
    assert uri is not None

    assert staging.cleanup() == []
    assert Path(uri).exists()


def test_cleanup_survives_an_already_deleted_object(tmp_path: Path):
    staging = local_staging(tmp_path, instance_id="exec-1")
    uri = staging.stage(python_address())
    assert uri is not None
    Path(uri).unlink()

    assert staging.cleanup() == []
    assert staging.staged == {}


def test_dialect_teardown_cleans_up(tmp_path: Path):
    dialect = enabled_dialect()
    dialect._python_staging = local_staging(tmp_path, instance_id="exec-1")
    uri = dialect._python_staging.stage(python_address())
    assert uri is not None

    dialect.teardown()
    assert not Path(uri).exists()


def test_dialect_teardown_without_staging_is_a_noop():
    BigqueryDialect(config=BigQueryConfig()).teardown()


# --- configuration errors -----------------------------------------------------


def test_render_source_requires_enable_flag():
    dialect = BigqueryDialect(config=BigQueryConfig(staging_uri="gs://b/p"))
    with pytest.raises(ValueError, match="enable_python_datasources"):
        dialect.render_source(python_address())


def test_render_source_requires_bigquery_config():
    dialect = BigqueryDialect(config=DuckDBConfig(enable_python_datasources=True))
    with pytest.raises(ValueError, match="enable_python_datasources"):
        dialect.render_source(python_address())


def test_render_source_requires_gcs_staging_uri():
    dialect = enabled_dialect(staging_uri=None)
    with pytest.raises(ValueError, match="GCS staging"):
        dialect.render_source(python_address())


def test_render_source_rejects_local_staging_path():
    dialect = BigqueryDialect(
        config=BigQueryConfig(enable_python_datasources=True, project="p"),
        staging=StagingConfig(path="/tmp/local"),
    )
    with pytest.raises(ValueError, match="GCS staging"):
        dialect.render_source(python_address())


def test_render_source_falls_back_to_executor_staging_config():
    dialect = BigqueryDialect(
        config=BigQueryConfig(enable_python_datasources=True, project="p"),
        staging=StagingConfig(path="gs://from-staging-config/prefix"),
    )
    assert dialect.python_staging().root_uri == "gs://from-staging-config/prefix/"


def test_config_staging_uri_wins_over_staging_config():
    dialect = BigqueryDialect(
        config=BigQueryConfig(
            staging_uri="gs://from-config/prefix", enable_python_datasources=True
        ),
        staging=StagingConfig(path="gs://from-staging-config/prefix"),
    )
    assert dialect.python_staging().root_uri == "gs://from-config/prefix/"


# --- prepare_sources ----------------------------------------------------------


def compiled_query(dialect: BigqueryDialect):
    environment = Environment(working_path=SCRIPTS)
    _, statements = environment.parse(MODEL)
    return dialect.generate_queries(environment, statements)[-1]


def test_generated_sql_reads_a_bare_temp_table():
    dialect = enabled_dialect()
    sql = dialect.compile_statement(compiled_query(dialect))

    assert dialect.python_staging().table_reference(python_address()) in sql
    assert "fib.py" not in sql
    assert "my-project" not in sql


def test_prepare_sources_registers_a_temp_definition(tmp_path: Path):
    dialect = enabled_dialect()
    dialect._python_staging = local_staging(tmp_path, instance_id="exec-1")
    connection = BigQueryConnection(client=object())
    executor = FakeExecutor(connection)
    query = compiled_query(dialect)

    addresses = collect_source_addresses(query.ctes)
    assert [a.location for a in addresses] == [str(FIB)]
    dialect.prepare_sources(addresses, executor)

    name = dialect.python_staging().table_name(python_address())
    assert list(connection.external_tables) == [name]
    assert connection.external_tables[name].source_uris == [
        dialect.python_staging().object_uri(python_address())
    ]
    # no catalog statement was issued
    assert executor.statements == []
    assert pq.read_table(connection.external_tables[name].source_uris[0]).num_rows == 25


def test_prepare_sources_uses_ddl_when_a_staging_dataset_is_set(tmp_path: Path):
    dialect = enabled_dialect(staging_dataset="staging")
    dialect._python_staging = local_staging(tmp_path, dataset="staging")
    executor = FakeExecutor()
    query = compiled_query(dialect)

    dialect.prepare_sources(collect_source_addresses(query.ctes), executor)

    assert len(executor.statements) == 1
    assert "CREATE OR REPLACE EXTERNAL TABLE" in executor.statements[0]


def test_sqlalchemy_without_a_staging_dataset_is_rejected_at_config_time():
    """SQLAlchemy cannot attach job config, so temp definitions are impossible."""
    dialect = enabled_dialect(use_sqlalchemy=True)
    with pytest.raises(ValueError, match="use_sqlalchemy=True cannot stage"):
        dialect.render_source(python_address())


def test_prepare_sources_rejects_temp_definitions_without_the_native_engine():
    dialect = enabled_dialect()
    query = compiled_query(dialect)

    with pytest.raises(ValueError, match="native BigQuery engine"):
        dialect.prepare_sources(collect_source_addresses(query.ctes), FakeExecutor())


def test_prepare_sources_ignores_non_python_addresses():
    dialect = enabled_dialect()
    executor = FakeExecutor(BigQueryConnection(client=object()))
    dialect.prepare_sources(
        [Address(location="dataset.table", type=AddressType.TABLE)], executor
    )
    assert executor.statements == []
