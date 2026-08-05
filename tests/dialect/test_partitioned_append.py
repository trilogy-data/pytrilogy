"""An APPEND onto a partitioned datasource replaces its slices, not appends.

The portable form stages the new rows, deletes the partition keys they cover,
and inserts — so re-running one partition is idempotent and never touches a
neighbour. BigQuery runs the same steps as one script, because its temp tables
do not outlive the job that declared them.
"""

from datetime import date

import pytest

from trilogy import Dialects, Environment
from trilogy.core.enums import PersistMode
from trilogy.core.statements.author import PersistStatement
from trilogy.execution.state import PartitionObservation
from trilogy.execution.state.partitions import partition_filter
from trilogy.parser import parse

MODEL = """
key id int;
property id.created_at date;
property id.label string;

root datasource source_facts (
    id: id,
    created_at: created_at,
    label: label,
)
grain (id)
address raw_facts;

datasource facts (
    id: id,
    created_at: created_at,
    label: label,
)
grain (id)
address my_facts
partition by created_at;

append into facts by created_at from select id, created_at, label;
"""

STAGED_DIALECTS = [Dialects.DUCK_DB, Dialects.POSTGRES, Dialects.SNOWFLAKE]


def _persist(dialect: Dialects):
    env = Environment()
    _, statements = parse(MODEL, env)
    renderer = dialect.default_renderer()
    (processed,) = renderer.generate_queries(env, [statements[-1]])
    assert processed.persist_mode == PersistMode.APPEND
    return renderer, processed


@pytest.mark.parametrize("dialect", STAGED_DIALECTS)
def test_partitioned_append_stages_then_replaces(dialect):
    renderer, processed = _persist(dialect)
    statements = renderer.compile_statements(processed)
    assert len(statements) == 5
    create, stage, delete, insert, drop = statements
    assert create.startswith("CREATE TEMPORARY TABLE")
    assert "WHERE 1=0" in create
    assert stage.strip().startswith("INSERT INTO")
    assert delete.startswith("DELETE FROM")
    assert "EXISTS (SELECT 1 FROM" in delete
    assert insert.startswith("INSERT INTO")
    assert drop.startswith("DROP TABLE")
    # The partition key drives the delete; nothing else may be cleared.
    assert "created_at" in delete
    assert "label" not in delete


@pytest.mark.parametrize("dialect", [*STAGED_DIALECTS, Dialects.SQL_SERVER])
def test_partition_delete_is_null_safe(dialect):
    """A NULL partition key is a real slice (`__NULL__` in the state format), so
    the delete must match it — a bare `=` or a row-value `IN` would not."""
    renderer, processed = _persist(dialect)
    delete = renderer.compile_statements(processed)[2]
    assert delete.count("IS NULL") == 2
    assert " IN (" not in delete


def test_partitioned_append_never_truncates():
    """No unconditional DELETE/TRUNCATE: an empty select stages no keys, so the
    EXISTS matches nothing and no slice is cleared."""
    renderer, processed = _persist(Dialects.DUCK_DB)
    _, _, delete, _, _ = renderer.compile_statements(processed)
    assert "WHERE EXISTS (SELECT 1 FROM" in delete
    assert "TRUNCATE" not in delete


def test_mysql_uses_a_delete_join_with_the_null_safe_operator():
    """MySQL rejects a correlated EXISTS on the delete target, and spells
    null-safe equality `<=>`."""
    renderer, processed = _persist(Dialects.MYSQL)
    delete = renderer.compile_statements(processed)[2]
    assert delete.startswith("DELETE `my_facts` FROM `my_facts` JOIN")
    assert "<=>" in delete
    assert "EXISTS" not in delete


def test_unpartitioned_append_is_a_plain_insert():
    env = Environment()
    _, statements = parse(
        MODEL.replace("partition by created_at;", ";").replace(
            "append into facts by created_at", "append into facts"
        ),
        env,
    )
    renderer = Dialects.DUCK_DB.default_renderer()
    (processed,) = renderer.generate_queries(env, [statements[-1]])
    (sql,) = renderer.compile_statements(processed)
    assert sql.strip().startswith("INSERT INTO")
    assert "DELETE" not in sql


def test_sql_server_uses_select_into_and_a_null_safe_exists():
    """T-SQL has neither `CREATE TABLE AS` nor row-value `IN`, and its temp
    tables need a leading `#`."""
    renderer, processed = _persist(Dialects.SQL_SERVER)
    create, stage, delete, insert, drop = renderer.compile_statements(processed)
    assert create.startswith("SELECT * INTO")
    assert '"#_trilogy_stage_my_facts"' in create
    assert "CREATE TEMPORARY TABLE" not in create
    assert stage.strip().startswith("INSERT INTO")
    # The delete itself is the shared base form — T-SQL only differs in staging.
    assert delete.startswith("DELETE FROM") and "EXISTS (SELECT 1 FROM" in delete
    assert insert.startswith("INSERT INTO")
    assert drop.startswith("DROP TABLE")


@pytest.mark.parametrize("dialect", [Dialects.DUCK_DB, Dialects.SQL_SERVER])
def test_every_partition_key_is_matched(dialect):
    env = Environment()
    _, statements = parse(
        MODEL.replace("partition by created_at;", "partition by created_at, label;")
        .replace(
            "append into facts by created_at", "append into facts by created_at, label"
        )
        .replace("property id.label string;", "property id.label date;"),
        env,
    )
    renderer = dialect.default_renderer()
    (processed,) = renderer.generate_queries(env, [statements[-1]])
    delete = renderer.compile_statements(processed)[2]
    assert delete.count("IS NULL") == 4  # two keys, each with a two-sided NULL arm
    assert '"created_at"' in delete and '"label"' in delete


EXECUTING_MODEL = """
key id int;
property id.created_at date;

root datasource source_facts (
    id: id,
    created_at: created_at,
)
grain (id)
query '''
SELECT 1 as id, DATE '2024-01-01' as created_at
UNION ALL SELECT 2, DATE '2024-01-02'
UNION ALL SELECT 3, CAST(NULL AS DATE)
''';

datasource facts (
    id: id,
    created_at: created_at,
)
grain (id)
address my_facts
partition by created_at;

CREATE IF NOT EXISTS DATASOURCE facts;
"""

APPEND = "append into facts by created_at from select id, created_at;"


def _row_counts(executor) -> dict:
    rows = executor.execute_raw_sql(
        "SELECT created_at, count(*) FROM my_facts GROUP BY 1"
    ).fetchall()
    return {row[0]: row[1] for row in rows}


def test_repeated_partitioned_append_is_idempotent_including_the_null_slice():
    """The end-to-end guarantee: appending twice replaces rather than
    duplicates, and the NULL slice is replaced like any other."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(EXECUTING_MODEL)
    executor.execute_text(APPEND)
    first = _row_counts(executor)
    executor.execute_text(APPEND)
    assert _row_counts(executor) == first
    assert first == {date(2024, 1, 1): 1, date(2024, 1, 2): 1, None: 1}


def test_bigquery_runs_the_staged_replace_as_one_script():
    """A BigQuery temp table only outlives the job that declared it, so the
    shared five statements have to travel as a single script. No per-partition
    loop: the delete is one null-safe statement whatever the slice count."""
    renderer, processed = _persist(Dialects.BIGQUERY)
    (script,) = renderer.compile_statements(processed)
    assert "EXECUTE IMMEDIATE" not in script
    assert "WHILE" not in script
    assert script.startswith("CREATE TEMP TABLE `_trilogy_stage_my_facts`")
    assert "DELETE FROM `my_facts` AS trilogy_delete_target WHERE EXISTS" in script
    assert script.count(";") == 4


def test_bigquery_aliases_the_delete_target_for_a_qualified_address():
    """BigQuery resolves `project.dataset.table.column` inside a correlated
    subquery as a *name*, not a column, and fails with `Unrecognized name:
    project`. Only an alias on the delete target works for a three-part
    address — which is what a real BigQuery table has."""
    env = Environment()
    _, statements = parse(MODEL.replace("address my_facts", "address proj.ds.tbl"), env)
    renderer = Dialects.BIGQUERY.default_renderer()
    (processed,) = renderer.generate_queries(env, [statements[-1]])
    (script,) = renderer.compile_statements(processed)
    delete = next(x for x in script.split(";\n") if x.startswith("DELETE"))
    assert "DELETE FROM `proj`.`ds`.`tbl` AS trilogy_delete_target" in delete
    assert "`proj`.`ds`.`tbl`.`created_at`" not in delete
    assert delete.count("trilogy_delete_target.`created_at`") == 2


def test_bigquery_partition_delete_is_null_safe_and_multi_key():
    """The scripted loop it replaced could do neither: `ARRAY_AGG(DISTINCT k)`
    drops the NULL slice, and it only ever read `partition_by[0]`."""
    env = Environment()
    _, statements = parse(
        MODEL.replace("partition by created_at;", "partition by created_at, label;")
        .replace(
            "append into facts by created_at", "append into facts by created_at, label"
        )
        .replace("property id.label string;", "property id.label date;"),
        env,
    )
    renderer = Dialects.BIGQUERY.default_renderer()
    (processed,) = renderer.generate_queries(env, [statements[-1]])
    (script,) = renderer.compile_statements(processed)
    delete = next(x for x in script.split(";\n") if x.startswith("DELETE"))
    assert delete.count("IS NULL") == 4
    assert "`created_at`" in delete and "`label`" in delete


SLICES = [
    PartitionObservation(values={"created_at": date(2025, 12, 15)}),
    PartitionObservation(values={"created_at": date(2025, 12, 17)}),
]


def _slice_scoped_refresh(dialect: Dialects, slices) -> str:
    """The statement `Executor._update_datasource_once` builds for stale slices."""
    env = Environment()
    parse(MODEL, env)
    ds = env.datasources["facts"]
    statement = PersistStatement(
        datasource=ds,
        select=ds.create_update_statement(
            env, partition_filter(ds, env, slices), line_no=None
        ),
        persist_mode=PersistMode.APPEND,
        partition_by=ds.partition_by,
    )
    renderer = dialect.default_renderer()
    (processed,) = renderer.generate_queries(env, [statement])
    return "\n".join(renderer.compile_statements(processed))


@pytest.mark.parametrize(
    "dialect",
    [Dialects.BIGQUERY, *STAGED_DIALECTS, Dialects.SQL_SERVER, Dialects.TRINO],
)
def test_slice_filter_renders_portable_membership(dialect):
    """A slice filter is a value list, and every engine spells that `in (...)`.
    The negative assertions are the load-bearing half — a dialect array
    constructor after `in` parses on DuckDB alone."""
    sql = _slice_scoped_refresh(dialect, SLICES)
    assert "in (date '2025-12-15',date '2025-12-17')" in sql
    assert "in [" not in sql
    assert "ARRAY_CONSTRUCT" not in sql
    assert "ARRAY[" not in sql


def test_slice_filter_selects_a_null_slice_with_is_null():
    sql = _slice_scoped_refresh(
        Dialects.POSTGRES, [*SLICES, PartitionObservation(values={"created_at": None})]
    )
    assert (
        "in (date '2025-12-15',date '2025-12-17')) or \"source_facts\".\"created_at\" is null"
        in sql
    )
