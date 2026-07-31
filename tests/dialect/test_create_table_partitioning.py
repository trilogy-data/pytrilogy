import pytest

from trilogy import Dialects, Environment
from trilogy.core.enums import CreateMode
from trilogy.core.models.datasource import Datasource
from trilogy.core.table_processor import datasource_to_create_table_info
from trilogy.parser import parse

# Partition column declared LAST: valid everywhere, and the only ordering the
# Presto/Trino Hive connector accepts (see test_presto_requires_partition_columns_last).
MODEL = """
key id int;
property id.created_at {type};
property id.label string;

datasource facts (
    id: id,
    label: label,
    created_at: created_at,
)
grain (id)
address my_facts
partition by {partition};
"""

ALL_DIALECTS = [d for d in Dialects if d != Dialects.DATAFRAME]


def _datasource(type: str = "date", partition: str = "created_at") -> Datasource:
    env = Environment()
    parse(MODEL.format(type=type, partition=partition), env)
    return env.datasources["facts"]


def _ddl(dialect: Dialects, **kwargs) -> str:
    info = datasource_to_create_table_info(_datasource(**kwargs))
    return "\n".join(
        dialect.default_renderer().compile_create_table_statements(
            info, CreateMode.CREATE_OR_REPLACE
        )
    )


def test_partition_reference_resolves_to_a_column():
    ds = _datasource()
    assert [c.address for c in ds.partition_by] == ["local.created_at"]
    assert datasource_to_create_table_info(ds).partition_keys == ["created_at"]


def test_partition_reference_survives_import_namespacing(tmp_path):
    (tmp_path / "inner.preql").write_text(
        MODEL.format(type="date", partition="created_at")
    )
    env = Environment(working_path=tmp_path)
    parse("import inner as inner;", env)
    ds = env.datasources["inner.facts"]
    assert [c.address for c in ds.partition_by] == ["inner.created_at"]
    assert datasource_to_create_table_info(ds).partition_keys == ["created_at"]


def test_append_partition_matches_datasource_declaration():
    """An unqualified reference on either side reads as a partition mismatch."""
    env = Environment()
    _, statements = parse(
        MODEL.format(type="date", partition="created_at")
        + "append into facts by created_at from select id, created_at;",
        env,
    )
    assert statements[-1].partition_by[0].address == "local.created_at"


def test_partition_on_missing_column_raises():
    ds = _datasource(partition="id")
    ds.partition_by[0].address = "local.not_a_column"
    with pytest.raises(ValueError, match="not a concrete column"):
        datasource_to_create_table_info(ds)


@pytest.mark.parametrize(
    "type,expected",
    [
        ("date", "PARTITION BY `created_at`"),
        ("datetime", "PARTITION BY DATETIME_TRUNC(`created_at`, DAY)"),
        ("timestamp", "PARTITION BY TIMESTAMP_TRUNC(`created_at`, DAY)"),
    ],
)
def test_bigquery_emits_partition_by(type, expected):
    assert expected in _ddl(Dialects.BIGQUERY, type=type)


def test_bigquery_rejects_non_temporal_partition():
    with pytest.raises(ValueError, match="requires a date/time column"):
        _ddl(Dialects.BIGQUERY, partition="id")


def test_bigquery_rejects_multiple_partition_keys():
    info = datasource_to_create_table_info(_datasource())
    info.partition_keys = ["created_at", "label"]
    with pytest.raises(ValueError, match="single partition column"):
        Dialects.BIGQUERY.default_renderer().compile_create_table_statements(
            info, CreateMode.CREATE_OR_REPLACE
        )


def test_bigquery_escapes_column_descriptions():
    env = Environment()
    parse(MODEL.format(type="date", partition="created_at"), env)
    env.concepts["label"].metadata.description = "it's a label"
    info = datasource_to_create_table_info(env.datasources["facts"])
    ddl = "\n".join(
        Dialects.BIGQUERY.default_renderer().compile_create_table_statements(
            info, CreateMode.CREATE_OR_REPLACE
        )
    )
    assert "OPTIONS(description='it\\'s a label')" in ddl


@pytest.mark.parametrize("dialect", [Dialects.PRESTO, Dialects.TRINO])
def test_presto_declares_partitioning_as_a_table_property(dialect):
    assert "WITH (partitioned_by = ARRAY['created_at'])" in _ddl(dialect)


@pytest.mark.parametrize("dialect", [Dialects.PRESTO, Dialects.TRINO])
def test_presto_requires_partition_columns_last(dialect):
    """The Hive connector rejects a table whose partition columns aren't last,
    so this raises instead of emitting DDL the engine will refuse."""
    env = Environment()
    parse(
        MODEL.format(type="date", partition="created_at").replace(
            "    label: label,\n    created_at: created_at,",
            "    created_at: created_at,\n    label: label,",
        ),
        env,
    )
    info = datasource_to_create_table_info(env.datasources["facts"])
    with pytest.raises(ValueError, match="last columns of the table"):
        dialect.default_renderer().compile_create_table_statements(
            info, CreateMode.CREATE_OR_REPLACE
        )


#: Dialects that can express partitioning in a CREATE. Everything else must fall
#: back to an unpartitioned table rather than syntax the engine rejects — see the
#: physical-partitioning notes in ``BaseDialect.render_partition_clause``.
PARTITIONING_DIALECTS = {Dialects.BIGQUERY, Dialects.PRESTO, Dialects.TRINO}


@pytest.mark.parametrize("dialect", ALL_DIALECTS)
def test_no_dialect_emits_unsupported_partition_syntax(dialect):
    ddl = _ddl(dialect)
    assert "PARTITIONED BY" not in ddl
    if dialect not in PARTITIONING_DIALECTS:
        assert "PARTITION BY" not in ddl
        assert "partitioned_by" not in ddl
