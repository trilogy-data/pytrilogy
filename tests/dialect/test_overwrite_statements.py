"""A persist OVERWRITE must never leave an empty table behind a failed select.

Engines with transactional DDL get that from the executor: DDL and INSERT run
in one implicit transaction that is rolled back on failure. BigQuery has no
transaction that can hold permanent-table DDL, so its OVERWRITE is rendered as
a single CREATE ... AS SELECT, which BigQuery applies atomically.
"""

import pytest

from trilogy import Dialects, Environment
from trilogy.core.enums import CreateMode, PersistMode
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
"""

OVERWRITE = "persist facts into my_facts from select id, created_at, label;"


def _overwrite(dialect: Dialects, statement: str = OVERWRITE):
    env = Environment()
    _, statements = parse(MODEL + statement, env)
    renderer = dialect.default_renderer()
    (processed,) = renderer.generate_queries(env, [statements[-1]])
    assert processed.persist_mode == PersistMode.OVERWRITE
    return renderer, processed


def test_bigquery_overwrite_is_one_create_as_select():
    renderer, processed = _overwrite(Dialects.BIGQUERY)
    (statement,) = renderer.compile_statements(processed)
    assert statement.lstrip().startswith("CREATE OR REPLACE TABLE `my_facts` (")
    assert "`created_at` DATE" in statement
    assert "PARTITION BY `created_at`\nAS\n" in statement
    assert "INSERT INTO" not in statement
    assert "FROM\n    `raw_facts`" in statement
    assert statement.count(";") == 0
    assert renderer.compile_statement(processed) == statement


def test_bigquery_create_with_data_keeps_its_create_mode():
    renderer, processed = _overwrite(
        Dialects.BIGQUERY, "create datasource facts with data;"
    )
    assert processed.create_mode == CreateMode.CREATE
    (statement,) = renderer.compile_statements(processed)
    assert statement.lstrip().startswith("CREATE TABLE `my_facts` (")
    assert "\nAS\n" in statement


def test_bigquery_column_descriptions_survive_the_single_statement():
    described = MODEL.replace(
        "property id.label string;",
        "property id.label string; # the display label",
    )
    env = Environment()
    _, statements = parse(described + OVERWRITE, env)
    renderer = Dialects.BIGQUERY.default_renderer()
    (processed,) = renderer.generate_queries(env, [statements[-1]])
    (statement,) = renderer.compile_statements(processed)
    assert "OPTIONS(description=" in statement


@pytest.mark.parametrize("dialect", [Dialects.DUCK_DB, Dialects.POSTGRES])
def test_transactional_dialects_keep_ddl_then_insert(dialect):
    renderer, processed = _overwrite(dialect)
    ddl, insert = renderer.compile_statements(processed)
    assert ddl.lstrip().startswith("CREATE OR REPLACE TABLE")
    assert insert.lstrip().startswith("INSERT INTO")
    assert " AS" not in ddl.split("(")[0]
