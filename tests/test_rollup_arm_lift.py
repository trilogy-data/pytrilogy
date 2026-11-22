from trilogy import Dialects
from trilogy.core.enums import Derivation
from trilogy.core.statements.author import RollupSelectStatement, SelectStatement
from trilogy.parsing.parse_engine_v2 import parse_text

FIXTURE = """
key id int;
property id.g1 string?;
property id.g2 string?;
property id.v int;
property id.w int;
datasource t (id: id, g1: g1, g2: g2, v: v, w: w)
grain (id)
query '''
select 1 id, 'a' g1, 'x' g2, 10 v, 1 w
union all select 2, 'a', 'y', 20 v, 2 w
union all select 3, 'b', 'x', 5 v, 3 w
union all select 4, null, null, 7 v, 4 w
''';
auto total <- sum(v);
auto wtotal <- sum(w);
const scale <- 2;
"""

SPREAD = """
select *rollup((select g1, g2, total,
    rank(g1, g2) over (partition by g1 order by sum(w) desc) as r,
    total / wtotal as ratio,
    case when grouping(g1) = 1 then 'ALL' else g1 end as lbl
), g1, g2) having total > 5;
"""


def _executor():
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(FIXTURE)
    return executor


def test_arm_keeps_only_the_pass():
    executor = _executor()
    _, statements = parse_text(SPREAD, executor.environment)
    statement = statements[-1]
    assert isinstance(statement, SelectStatement)
    assert statement.source is not None
    body = statement.source.select
    assert isinstance(body, RollupSelectStatement)
    arm = body.selects[0]
    assert arm.having_clause is None
    derivations = {
        executor.environment.concepts[c.address].derivation
        for c in arm.output_components
    }
    assert derivations <= {Derivation.ROOT, Derivation.AGGREGATE}
    assert {"local.r", "local.ratio", "local.lbl"} <= {
        label.address for label in statement.labels.values()
    }


ORACLE = """
with t as (
    select * from (values (1, 'a', 'x', 10, 1), (2, 'a', 'y', 20, 2),
        (3, 'b', 'x', 5, 3), (4, null, null, 7, 4)) as t(id, g1, g2, v, w)
)
select g1, g2, sum(v) as total,
    rank() over (partition by g1 order by sum(w) desc) as r,
    sum(v) / sum(w) as ratio,
    case when grouping(g1) = 1 then 'ALL' else g1 end as lbl
from t group by rollup (g1, g2) having sum(v) > 5
"""


def _sorted(rows):
    return sorted(
        (tuple(row) for row in rows),
        key=lambda r: tuple((x is None, str(x)) for x in r),
    )


def test_lifted_columns_read_the_relation():
    executor = _executor()
    got = executor.execute_text(SPREAD)[-1].fetchall()
    assert _sorted(got) == _sorted(executor.execute_raw_sql(ORACLE).fetchall())


def test_having_still_filters_after_the_window():
    rows = (
        _executor()
        .execute_text(
            "select *rollup((select g1, total, rank() over (order by total desc) as rk"
            " having total < 40), g1) order by rk;"
        )[-1]
        .fetchall()
    )
    assert rows == [("a", 30, 2), (None, 7, 3), ("b", 5, 4)]


def test_named_and_signed_calls_lift():
    executor = _executor()
    executor.parse_text(
        "with r as rollup((select g1, total, rank() over (order by total desc) as rk,"
        " total * scale as dbl), g1) -> (a, t, k, d);"
    )
    named = executor.execute_text("select r.a, r.t, r.k, r.d order by r.k;")[-1]
    expected = [(None, 42, 1, 84), ("a", 30, 2, 60), (None, 7, 3, 14), ("b", 5, 4, 10)]
    assert named.fetchall() == expected
    inline = executor.execute_text(
        "from rollup((select g1, total, rank() over (order by total desc) as rk,"
        " total * 2 as dbl), g1) -> (a, t, k, d) select a, t, k, d order by k;"
    )[-1]
    assert inline.fetchall() == expected
