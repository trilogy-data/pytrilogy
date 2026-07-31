"""The DDL trilogy emits for a `create datasources` is checked against the
schema duckdb actually ends up with, not just against the rendered string."""

from trilogy import Dialects, Executor

MODEL = """
key id int;
property id.created_at date;
property id.amount float;
property id.label string;

datasource facts (
    id: id,
    created_at: created_at,
    amount: amount,
    label: label,
)
grain (id)
address facts_tbl
partition by created_at;

create or replace datasources facts;
"""


def _columns(executor: Executor, table: str) -> list[tuple[str, str]]:
    rows = executor.execute_raw_sql(
        "select column_name, data_type from information_schema.columns "
        f"where table_name = '{table}' order by ordinal_position"
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def test_create_datasource_schema_matches_declaration():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(MODEL)

    assert _columns(executor, "facts_tbl") == [
        ("id", "INTEGER"),
        ("created_at", "DATE"),
        ("amount", "FLOAT"),
        ("label", "VARCHAR"),
    ]
    executor.close()


def test_partition_declaration_does_not_reach_duckdb_ddl():
    """duckdb has no table partitioning; the declaration must be dropped rather
    than rendered into syntax duckdb rejects."""
    executor = Dialects.DUCK_DB.default_executor()
    statements = [
        sql
        for command in executor.parse_text(MODEL)
        for sql in executor.generate_sql(command)
    ]
    create = next(s for s in statements if "CREATE" in s)
    assert "PARTITION" not in create.upper()

    executor.execute_text(MODEL)
    executor.execute_raw_sql(
        "insert into facts_tbl values (1, date '2024-01-01', 1.5, 'a'), "
        "(2, date '2024-01-02', 2.5, 'b')"
    )
    rows = executor.execute_text(
        MODEL.replace("create or replace datasources facts;", "")
        + "select created_at, amount order by created_at asc;"
    )[-1].fetchall()
    assert [(r[0].isoformat(), r[1]) for r in rows] == [
        ("2024-01-01", 1.5),
        ("2024-01-02", 2.5),
    ]
    executor.close()
