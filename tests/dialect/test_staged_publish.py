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
    (clone,) = renderer.render_table_clone("p.d.t", "p.d.s")
    assert clone.startswith("CREATE TABLE `p`.`d`.`s` CLONE `p`.`d`.`t` ")
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


def test_empty_copy_keeps_each_engines_shape():
    """The rows table of an audited partitioned append has to carry the
    target's physical layout (BigQuery partitioning for decorator copies,
    Presto connector properties) and outlive the statement that made it."""
    bigquery = Dialects.BIGQUERY.default_renderer().render_empty_copy("p.d.t", "p.d.r")
    assert len(bigquery) == 1
    assert bigquery[0].startswith("CREATE TABLE `p`.`d`.`r` LIKE `p`.`d`.`t` OPTIONS(")
    assert Dialects.SNOWFLAKE.default_renderer().render_empty_copy("t", "r") == [
        'CREATE TABLE "r" LIKE "t"'
    ]
    assert Dialects.MYSQL.default_renderer().render_empty_copy("t", "r") == [
        "CREATE TABLE `r` LIKE `t`"
    ]
    assert Dialects.CLICKHOUSE.default_renderer().render_empty_copy("t", "r") == [
        "CREATE TABLE `r` AS `t`"
    ]
    assert Dialects.PRESTO.default_renderer().render_empty_copy("t", "r") == [
        'CREATE TABLE "r" (LIKE "t" INCLUDING PROPERTIES)'
    ]


def test_bigquery_clone_expires_on_its_own():
    (clone,) = Dialects.BIGQUERY.default_renderer().render_table_clone("p.d.t", "p.d.s")
    assert clone.startswith("CREATE TABLE `p`.`d`.`s` CLONE `p`.`d`.`t` OPTIONS(")
    assert "expiration_timestamp" in clone


def test_partition_merge_is_the_staged_delete_and_insert():
    renderer = Dialects.SNOWFLAKE.default_renderer()
    delete, insert = renderer.render_partition_merge("t", "r", ["day"])
    assert delete.startswith('DELETE FROM "t" WHERE EXISTS (SELECT 1 FROM "r" WHERE')
    assert insert == 'INSERT INTO "t" SELECT * FROM "r"'
    (bigquery,) = Dialects.BIGQUERY.default_renderer().render_partition_merge(
        "p.d.t", "p.d.r", ["day"]
    )
    assert bigquery.startswith("BEGIN TRANSACTION;\nDELETE FROM `p`.`d`.`t`")
    assert "INSERT INTO `p`.`d`.`t` SELECT * FROM `p`.`d`.`r`" in bigquery
    assert bigquery.endswith("COMMIT TRANSACTION;")
    mysql_delete, _ = Dialects.MYSQL.default_renderer().render_partition_merge(
        "t", "r", ["day"]
    )
    assert mysql_delete.startswith("DELETE `t` FROM `t` JOIN `r` ON")


PARTITIONED_MODEL = """
key id int;
property id.source string;
property id.day date;
root datasource raw (id: id, source: source, day: day) grain (id) address raw;
datasource built (id: id, source: source, day: day)
grain (id)
address built
partition by day
assert count(id) >= 0.9 * prior(count(id))
;
"""

APPEND_ONE_DAY = (
    "append into built by day from select id, source, day"
    " where day = '2024-01-02'::date;"
)


def _partitioned_executor(tmp_path: Path) -> Executor:
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    executor.execute_raw_sql(
        "CREATE TABLE raw AS SELECT i AS id, 'A' AS source,"
        " DATE '2024-01-01' + INTERVAL (i % 4) DAY AS day FROM range(1000) t(i)"
    )
    executor.parse_text(PARTITIONED_MODEL)
    executor.generator.TRANSACTIONAL_DDL = False
    return executor


def _by_day(executor: Executor) -> list[tuple]:
    return [
        (str(day), n)
        for day, n in executor.execute_raw_sql(
            "SELECT day, count(*) FROM built GROUP BY 1 ORDER BY 1"
        ).fetchall()
    ]


def test_partitioned_append_publishes_only_its_own_slices(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Another writer's slice, published while this build was being audited,
    survives: the publish merges this build's partitions rather than
    replacing the target with the clone's snapshot."""
    executor = _partitioned_executor(tmp_path)
    executor.update_datasource(executor.environment.datasources["built"])
    assert _by_day(executor) == [
        ("2024-01-01", 250),
        ("2024-01-02", 250),
        ("2024-01-03", 250),
        ("2024-01-04", 250),
    ]
    executor.execute_raw_sql("DELETE FROM raw WHERE day = DATE '2024-01-02'")
    executor.execute_raw_sql(
        "INSERT INTO raw SELECT i, 'B', DATE '2024-01-02' FROM range(2000, 2300) t(i)"
    )

    original = executor._check_assertions

    def audit_while_another_writer_publishes(datasource, address, prior):
        executor.execute_raw_sql(
            "INSERT INTO built VALUES (9999, 'Z', DATE '2024-01-03')"
        )
        original(datasource, address, prior)

    monkeypatch.setattr(
        executor, "_check_assertions", audit_while_another_writer_publishes
    )

    def never(*args, **kwargs):
        raise AssertionError("a partitioned append must not swap the whole table")

    monkeypatch.setattr(executor.generator, "render_table_swap", never)
    executor.execute_text(APPEND_ONE_DAY)
    assert _by_day(executor) == [
        ("2024-01-01", 250),
        ("2024-01-02", 300),
        ("2024-01-03", 251),
        ("2024-01-04", 250),
    ]
    assert _tables(executor) == ["built", "raw"]


def test_partitioned_append_that_fails_its_audit_touches_nothing(tmp_path: Path):
    executor = _partitioned_executor(tmp_path)
    executor.update_datasource(executor.environment.datasources["built"])
    executor.execute_raw_sql("DELETE FROM raw WHERE day = DATE '2024-01-02' AND id > 3")
    with pytest.raises(DatasourceAssertionError) as info:
        executor.execute_text(APPEND_ONE_DAY)
    assert "prior(count(id)) = 1000" in str(info.value)
    assert _by_day(executor)[1] == ("2024-01-02", 250)
    assert _tables(executor) == ["built", "raw"]
