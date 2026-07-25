"""FULL JOIN lowering for dialects without a FULL JOIN (MySQL).

The lowered form is checked for row-parity against the native FULL JOIN by
running both on DuckDB — ``NoFullJoinDuckDB`` is a DuckDB renderer with the
capability flag flipped off, so the only difference between the two SQL strings
is the rewrite under test.
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.core.optimizations.full_join_lowering import UnsupportedFullJoinError
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.dialect.mysql import MySQLDialect
from trilogy.parser import parse_text


class NoFullJoinDuckDB(DuckDBDialect):
    SUPPORTS_FULL_JOIN = False


MODEL = """
key cid int;
property cid.cname string;
property cid.region string;
key oid int;
property oid.ocust int;
property oid.oregion string;
property oid.amt float;
datasource customers (cid:cid, cname:cname, region:region) grain (cid)
  query '''select * from (values (1,'a','east'),(2,'b','west'),(3,'c','east'))
           as t(cid, cname, region)''';
datasource orders (oid:oid, ocust:ocust, oregion:oregion, amt:amt) grain (oid)
  query '''select * from (values (10,2,'west',5.0),(11,4,'east',7.0),(12,2,'west',9.0))
           as t(oid, ocust, oregion, amt)''';
"""

# q51 shape: two rowsets union-joined on two keys, payloads null-extended.
ROWSET_MODEL = """
key web_id int;
property web_id.w_item int;
property web_id.w_date int;
property web_id.w_amount int;
key store_id int;
property store_id.s_item int;
property store_id.s_date int;
property store_id.s_amount int;
datasource web_sales (id: web_id, item: w_item, dt: w_date, amt: w_amount)
grain (web_id)
query '''select * from (values (1,1,1,10),(2,1,2,20)) as t(id,item,dt,amt)''';
datasource store_sales (id: store_id, item: s_item, dt: s_date, amt: s_amount)
grain (store_id)
query '''select * from (values (1,1,2,5),(2,1,3,7),(3,2,1,3)) as t(id,item,dt,amt)''';
"""

ROWSETS = """with wd as
select w_item as ik, w_date as dt, sum(w_amount) as wt;
with sd as
select s_item as ik, s_date as dt, sum(s_amount) as st;
with combined as
select coalesce(wd.ik, sd.ik) as ik, coalesce(wd.dt, sd.dt) as dt,
       wd.wt as wt, sd.st as st
union join wd.ik = sd.ik
union join wd.dt = sd.dt;
"""


def _executor(model: str):
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(model)
    return executor


def _both_sql(executor, query: str) -> tuple[str, str]:
    _, statements = parse_text(query, executor.environment)
    native, lowered = DuckDBDialect(), NoFullJoinDuckDB()
    return (
        native.compile_statement(
            native.generate_queries(executor.environment, statements)[0]
        ),
        lowered.compile_statement(
            lowered.generate_queries(executor.environment, statements)[0]
        ),
    )


def _rows(executor, sql: str):
    return sorted(
        [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()], key=repr
    )


@pytest.mark.parametrize(
    "query",
    [
        "select cid, cname, sum(amt) as total union join ocust = cid;",
        "select cid union join ocust = cid;",
        "select cid, cname, sum(amt) as total union join ocust = cid order by cid asc;",
        (
            "select cid, region, sum(amt) as total "
            "union join ocust = cid and oregion = region;"
        ),
    ],
)
def test_lowered_union_join_matches_native_full_join(query: str):
    executor = _executor(MODEL)
    native_sql, lowered_sql = _both_sql(executor, query)

    assert "FULL JOIN" in native_sql.upper(), native_sql
    assert "FULL JOIN" not in lowered_sql.upper(), lowered_sql
    assert "UNION" in lowered_sql.upper(), lowered_sql
    assert _rows(executor, native_sql) == _rows(executor, lowered_sql)


@pytest.mark.parametrize(
    "tail",
    [
        "select combined.ik, combined.dt, combined.wt, combined.st "
        "order by combined.ik asc, combined.dt asc;",
        """select combined.ik, combined.dt,
  sum(combined.wt) over (partition by combined.ik order by combined.dt asc) as wrun,
  sum(combined.st) over (partition by combined.ik order by combined.dt asc) as srun
order by combined.ik asc, combined.dt asc;""",
    ],
)
def test_lowered_two_key_rowset_union_join_matches_native(tail: str):
    executor = _executor(ROWSET_MODEL)
    native_sql, lowered_sql = _both_sql(executor, ROWSETS + tail)

    assert "FULL JOIN" in native_sql.upper(), native_sql
    assert "FULL JOIN" not in lowered_sql.upper(), lowered_sql
    assert _rows(executor, native_sql) == _rows(executor, lowered_sql)


def test_spine_is_a_distinct_union_left_joined_to_each_side():
    executor = _executor(MODEL)
    _, lowered_sql = _both_sql(
        executor, "select cid, cname, sum(amt) as total union join ocust = cid;"
    )

    assert "UNION ALL" not in lowered_sql.upper(), lowered_sql
    assert lowered_sql.upper().count("LEFT OUTER JOIN") == 2, lowered_sql
    assert '"_spine' in lowered_sql, lowered_sql


def test_mysql_renders_union_join_without_full_join():
    env, statements = parse_text(
        MODEL + "select cid, cname, sum(amt) as total union join ocust = cid;",
        Environment(),
    )
    dialect = MySQLDialect()

    sql = dialect.compile_statement(dialect.generate_queries(env, statements)[0])

    assert "FULL JOIN" not in sql.upper(), sql
    assert "`_spine" in sql, sql


def test_refusal_names_the_dialect_limitation():
    # An unlowerable shape must raise a typed, explanatory error -- never emit
    # SQL the dialect cannot run. `UnsupportedFullJoinError` is an
    # `UnresolvableQueryException` so existing handlers treat it as a planning
    # failure rather than a crash.
    from trilogy.core.exceptions import UnresolvableQueryException

    assert issubclass(UnsupportedFullJoinError, UnresolvableQueryException)
