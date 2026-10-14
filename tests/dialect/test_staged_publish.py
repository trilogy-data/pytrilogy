"""Write, audit, publish on engines whose DDL cannot be rolled back.

An audited table build on such an engine lands in a staging table beside the
target, is checked there, and is published with the dialect's swap. The
rendering of each primitive is pinned per dialect; the flow itself is driven
on DuckDB with the dialect flagged non-transactional, so a real engine runs
every statement the executor issues.
"""

from pathlib import Path

import pytest

from trilogy import Dialects, Executor
from trilogy.core.enums import CreateMode
from trilogy.core.models.environment import Environment
from trilogy.dialect.results import BufferedResult
from trilogy.execution.assertions import DatasourceAssertionError

MODEL = """
key id int;
property id.source string;
root datasource raw (id: id, source: source) grain (id) address raw;
datasource built (id: id, source: source)
grain (id)
address built
freshness by id
assert count(id) >= 0.75 * prior(count(id))
assert count(id ? source = 'A') >= 100
;
"""

NON_TRANSACTIONAL = [
    Dialects.BIGQUERY,
    Dialects.SNOWFLAKE,
    Dialects.MYSQL,
    Dialects.CLICKHOUSE,
    Dialects.PRESTO,
    Dialects.TRINO,
]
TRANSACTIONAL = [
    Dialects.DUCK_DB,
    Dialects.POSTGRES,
    Dialects.SQLITE,
    Dialects.SQL_SERVER,
]


@pytest.mark.parametrize("dialect", NON_TRANSACTIONAL)
def test_engines_without_transactional_ddl_publish_through_staging(dialect):
    assert dialect.default_renderer().TRANSACTIONAL_DDL is False


@pytest.mark.parametrize("dialect", TRANSACTIONAL)
def test_engines_with_transactional_ddl_audit_in_the_transaction(dialect):
    assert dialect.default_renderer().TRANSACTIONAL_DDL is True


def test_staging_table_sits_beside_the_target():
    renderer = Dialects.BIGQUERY.default_renderer()
    assert (
        renderer.staged_table_location("proj.ds.facts", "abcd1234")
        == "proj.ds.facts_trilogy_stage_abcd1234"
    )
    assert renderer.staged_table_location("facts", "abcd1234") == (
        "facts_trilogy_stage_abcd1234"
    )


def test_bigquery_clones_and_replaces_by_clone():
    renderer = Dialects.BIGQUERY.default_renderer()
    assert renderer.render_table_clone("p.d.t", "p.d.s") == [
        "CREATE TABLE `p`.`d`.`s` CLONE `p`.`d`.`t`"
    ]
    assert renderer.render_table_swap("p.d.t", "p.d.s", replace=True) == [
        "CREATE OR REPLACE TABLE `p`.`d`.`t` CLONE `p`.`d`.`s`"
    ]
    assert renderer.render_table_swap("p.d.t", "p.d.s", replace=False) == [
        "CREATE TABLE `p`.`d`.`t` CLONE `p`.`d`.`s`"
    ]


def test_snowflake_clone_keeps_grants():
    renderer = Dialects.SNOWFLAKE.default_renderer()
    assert renderer.render_table_clone("db.sch.t", "db.sch.s") == [
        'CREATE TABLE "db"."sch"."s" CLONE "db"."sch"."t"'
    ]
    assert renderer.render_table_swap("db.sch.t", "db.sch.s", replace=True) == [
        'CREATE OR REPLACE TABLE "db"."sch"."t" CLONE "db"."sch"."s" COPY GRANTS'
    ]
    assert renderer.render_table_swap("t", "s", replace=False) == [
        'CREATE TABLE "t" CLONE "s"'
    ]


def test_clickhouse_exchanges_tables():
    renderer = Dialects.CLICKHOUSE.default_renderer()
    assert renderer.render_table_clone("db.t", "db.s") == [
        "CREATE TABLE `db`.`s` AS `db`.`t`",
        "INSERT INTO `db`.`s` SELECT * FROM `db`.`t`",
    ]
    assert renderer.render_table_swap("db.t", "db.s", replace=True) == [
        "CREATE TABLE IF NOT EXISTS `db`.`t` AS `db`.`s`",
        "EXCHANGE TABLES `db`.`t` AND `db`.`s`",
    ]
    assert renderer.render_table_swap("db.t", "db.s", replace=False) == [
        "RENAME TABLE `db`.`s` TO `db`.`t`"
    ]


def test_mysql_renames_atomically_and_retires_the_old_table():
    renderer = Dialects.MYSQL.default_renderer()
    assert renderer.render_table_clone("db.t", "db.s") == [
        "CREATE TABLE `db`.`s` LIKE `db`.`t`",
        "INSERT INTO `db`.`s` SELECT * FROM `db`.`t`",
    ]
    assert renderer.render_table_swap("db.t", "db.s", replace=True) == [
        "CREATE TABLE IF NOT EXISTS `db`.`t` LIKE `db`.`s`",
        "RENAME TABLE `db`.`t` TO `db`.`s_old`, `db`.`s` TO `db`.`t`",
        "DROP TABLE `db`.`s_old`",
    ]
    assert renderer.render_table_swap("db.t", "db.s", replace=False) == [
        "RENAME TABLE `db`.`s` TO `db`.`t`"
    ]


@pytest.mark.parametrize("dialect", [Dialects.PRESTO, Dialects.TRINO])
def test_presto_keeps_properties_and_renames_qualified(dialect):
    renderer = dialect.default_renderer()
    assert renderer.render_table_clone("hive.s.t", "hive.s.x") == [
        'CREATE TABLE "hive"."s"."x" (LIKE "hive"."s"."t" INCLUDING PROPERTIES)',
        'INSERT INTO "hive"."s"."x" SELECT * FROM "hive"."s"."t"',
    ]
    assert renderer.render_table_swap("hive.s.t", "hive.s.x", replace=True) == [
        'DROP TABLE IF EXISTS "hive"."s"."t"',
        'ALTER TABLE "hive"."s"."x" RENAME TO "hive"."s"."t"',
    ]


class RecordingEngine:
    """Wraps a real engine; answers ``execute_persist`` by running the SQL
    itself and records the location each persist was aimed at."""

    def __init__(self, inner, executor: Executor):
        self.inner = inner
        self.executor = executor
        self.targets: list[str] = []

    def connect(self):
        return self.inner.connect()

    def setup(self, env, connection):
        return self.inner.setup(env, connection)

    def dispose(self, close: bool = True):
        return self.inner.dispose(close)

    def execute_persist(self, query, executor):
        self.targets.append(query.output_to.address.location)
        for statement in executor.generator.compile_statements(query):
            executor.execute_raw_sql(statement)
        return BufferedResult([], [])


def _executor(tmp_path: Path) -> Executor:
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    executor.execute_raw_sql(
        "CREATE TABLE raw AS SELECT i AS id,"
        " CASE WHEN i % 10 = 0 THEN 'B' ELSE 'A' END AS source FROM range(1000) t(i)"
    )
    executor.parse_text(MODEL)
    executor.generator.TRANSACTIONAL_DDL = False
    return executor


def _tables(executor: Executor) -> list[str]:
    return [
        row[0]
        for row in executor.execute_raw_sql(
            "SELECT table_name FROM information_schema.tables ORDER BY 1"
        ).fetchall()
    ]


def _count(executor: Executor) -> int:
    return executor.execute_raw_sql("SELECT count(*) FROM built").fetchall()[0][0]


def test_first_build_publishes_through_a_staging_table(tmp_path: Path):
    executor = _executor(tmp_path)
    executor.update_datasource(executor.environment.datasources["built"])
    assert _count(executor) == 1000
    assert _tables(executor) == ["built", "raw"]


def test_failed_audit_keeps_the_target_and_drops_the_stage(tmp_path: Path):
    executor = _executor(tmp_path)
    built = executor.environment.datasources["built"]
    executor.update_datasource(built)
    executor.execute_raw_sql("DELETE FROM raw WHERE id >= 10")
    with pytest.raises(DatasourceAssertionError) as info:
        executor.update_datasource(built)
    assert "count(id) = 10, prior(count(id)) = 1000" in str(info.value)
    assert _count(executor) == 1000
    assert _tables(executor) == ["built", "raw"]


def test_append_is_applied_to_a_clone_and_audited_whole(tmp_path: Path):
    executor = _executor(tmp_path)
    built = executor.environment.datasources["built"]
    executor.update_datasource(built)
    executor.execute_raw_sql(
        "INSERT INTO raw SELECT i, 'A' FROM range(1000, 1200) t(i)"
    )
    executor.execute_text("append into built from select id, source where id >= 1000;")
    assert _count(executor) == 1200
    assert _tables(executor) == ["built", "raw"]


def test_native_writer_is_aimed_at_the_staging_table(tmp_path: Path):
    executor = _executor(tmp_path)
    engine = RecordingEngine(executor.engine, executor)
    executor.engine = engine
    executor.update_datasource(executor.environment.datasources["built"])
    assert len(engine.targets) == 1
    assert engine.targets[0].startswith("built_trilogy_stage_")
    assert _count(executor) == 1000
    assert _tables(executor) == ["built", "raw"]


def test_unaudited_build_does_not_stage(tmp_path: Path):
    executor = _executor(tmp_path)
    engine = RecordingEngine(executor.engine, executor)
    executor.engine = engine
    executor.update_datasource(
        executor.environment.datasources["built"], check_assertions=False
    )
    assert engine.targets == ["built"]


def test_plain_create_still_refuses_an_existing_target(tmp_path: Path):
    executor = _executor(tmp_path)
    executor.update_datasource(executor.environment.datasources["built"])
    with pytest.raises(Exception, match="already exists"):
        executor.execute_text("create datasource built with data;")
    assert _count(executor) == 1000
    assert _tables(executor) == ["built", "raw"]
    (processed,) = executor.parse_text("create datasource built with data;")
    assert processed.create_mode == CreateMode.CREATE
