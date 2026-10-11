"""A datasource's own `where` is applied to every read of it, so a build must
write only the rows a read keeps. It gates the build's finished rows as a
HAVING, never a WHERE, so the columns keep the values the concepts have: an
aggregate column is not recomputed over the surviving rows. The incremental
key filter and a partial target's `complete where` still scope it as WHERE.
"""

from trilogy import Dialects, Environment
from trilogy.core.enums import PersistMode
from trilogy.core.statements.author import PersistStatement, SelectStatement

MODEL = """
key id int;
property id.flag bool;
property id.region string;

datasource rows (id: id, flag: flag, region: region)
grain (id)
query '''
select * from (values (1, true, 'east'), (2, false, 'east'), (3, true, 'west'))
as t(id, flag, region)
''';

datasource kept (id: id, flag: flag, region: region)
grain (id)
address kept_rows
where flag = true;

partial datasource kept_east (id: id, flag: flag, region: region)
grain (id)
complete where region = 'east'
address kept_east_rows
where flag = true;
"""


def _env() -> Environment:
    env = Environment()
    env.parse(MODEL)
    return env


def test_build_select_gates_on_the_datasource_where():
    env = _env()
    kept = env.datasources["kept"]
    statement = kept.create_update_statement(env, None, line_no=None)
    assert statement.where_clause is None
    assert statement.having_clause is not None
    assert str(statement.having_clause.conditional) == str(kept.where)


def test_build_select_scopes_by_the_incremental_filter_and_gates_on_the_where():
    env = _env()
    kept = env.datasources["kept"]
    incremental = env.parse("select id where id > 1;")[-1][-1]
    assert isinstance(incremental, SelectStatement)
    statement = kept.create_update_statement(
        env, incremental.where_clause, line_no=None
    )
    assert str(statement.where_clause) == str(incremental.where_clause)
    assert statement.having_clause is not None
    assert str(statement.having_clause.conditional) == str(kept.where)


def test_refresh_writes_only_rows_the_datasource_admits():
    env = _env()
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    executor.update_datasource(env.datasources["kept"])
    rows = executor.execute_raw_sql("select id from kept_rows order by id").fetchall()
    assert rows == [(1,), (3,)]


def test_partial_target_applies_both_complete_where_and_where():
    env = _env()
    target = env.datasources["kept_east"]
    statement = PersistStatement(
        datasource=target,
        select=target.create_update_statement(env, None, line_no=None),
        persist_mode=PersistMode.OVERWRITE,
    )
    renderer = Dialects.DUCK_DB.default_renderer()
    (processed,) = renderer.generate_queries(env, [statement])
    sql = "\n".join(renderer.compile_statements(processed))
    assert "'east'" in sql
    assert "flag" in sql.split("WHERE", 1)[1]
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    executor.update_datasource(env.datasources["kept"])
    executor.update_datasource(target)
    rows = executor.execute_raw_sql(
        "select id from kept_east_rows order by id"
    ).fetchall()
    assert rows == [(1,)]
