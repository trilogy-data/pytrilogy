import pytest

from trilogy import Dialects, Environment
from trilogy.core.statements.execute import ProcessedQueryPersist
from trilogy.executor import GENERATABLE_STATEMENT_TYPES
from trilogy.parser import parse_text
from trilogy.render import get_dialect_generator

MODEL = """
key id int;
property id.name string;
datasource t (id:id, name:name) grain (id) address tbl;
"""


@pytest.mark.parametrize("dialect", [d for d in Dialects if d != Dialects.DATAFRAME])
def test_persist_render_keeps_insert_prefix(dialect: Dialects):
    gen = get_dialect_generator(dialect)
    env, statements = parse_text(
        MODEL + "persist out into out_table from select id, name limit 5;",
        Environment(),
    )
    generatable = [x for x in statements if isinstance(x, GENERATABLE_STATEMENT_TYPES)]
    processed = gen.generate_queries(env, generatable)
    assert isinstance(processed[-1], ProcessedQueryPersist)
    sql = gen.compile_statement(processed[-1])
    if dialect == Dialects.BIGQUERY:
        # No transaction can hold its DDL, so the overwrite is one CTAS.
        assert "INSERT INTO" not in sql, sql
        assert sql.index("CREATE") < sql.index("\nAS\n") < sql.index("SELECT")
    else:
        assert "CREATE" in sql and "INSERT INTO" in sql, sql
        assert sql.index("CREATE") < sql.index("INSERT INTO") < sql.index("SELECT")
    assert "out_table" in sql
    if dialect == Dialects.SQL_SERVER:
        assert "TOP 5" in sql
    else:
        assert "LIMIT" in sql
