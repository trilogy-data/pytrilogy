"""An APPEND onto a partitioned datasource replaces its slices, not appends.

The portable form stages the new rows, deletes the partition keys they cover,
and inserts — so re-running one partition is idempotent and never touches a
neighbour. BigQuery keeps its own scripted equivalent.
"""

from datetime import date

import pytest

from trilogy import Dialects, Environment
from trilogy.core.enums import PersistMode
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


def test_bigquery_keeps_its_scripted_per_partition_delete():
    renderer, processed = _persist(Dialects.BIGQUERY)
    (script,) = renderer.compile_statements(processed)
    assert "EXECUTE IMMEDIATE" in script
    assert "CREATE TEMPORARY TABLE" not in script
