"""A membership WHERE (`x in <set>`) whose left operand lives in another
scoped-joined rowset must source the existence set as a parent of the post-join
merge -- otherwise the membership subselect renders against a dangling CTE
(`INVALID_REFERENCE_BUG`) and the renderer raises an uncaught ValueError.

Regression for TPC-DS q2. `gen_rowset_node` intercepts a WHERE comparing a
rowset's output against *another* rowset (the q11/q23 cross-rowset family),
merges the rowsets, and applies the predicate post-join -- but it short-circuited
the normal completion-merge path that sources a membership's existence set, so
the set was never materialized. The crash fires for BOTH a plain join key
(`cur.ws = ftr.ws`) and an expression join key (`cur.ws = ftr.ws - 53`); the
existence set must come from a *dimension* (here `week_seq`/`year` via a date
table) so discovery routes it through the cross-rowset branch rather than
inlining it onto the fact scan.
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

MODEL = """
key date_id int;
property date_id.week_seq int;
property date_id.dow int;
property date_id.year int;
datasource dates (id: date_id, ws: week_seq, d: dow, y: year) grain (date_id)
query '''
select 1 id, 1 ws, 0 d, 2001 y union all
select 2 id, 2 ws, 1 d, 2001 y union all
select 3 id, 54 ws, 0 d, 2002 y union all
select 4 id, 55 ws, 0 d, 2002 y''';

key sale_id int;
property sale_id.channel string;
property sale_id.date_id int;
property sale_id.val float;
datasource sales (id: sale_id, ch: channel, dt: date_id, v: val) grain (sale_id)
query '''
select 1 id,'STORE' ch,1 dt,10.0 v union all
select 2 id,'STORE' ch,2 dt,20.0 v union all
select 3 id,'WEB'   ch,1 dt,30.0 v union all
select 4 id,'WEB'   ch,3 dt,50.0 v union all
select 5 id,'STORE' ch,4 dt,40.0 v''';
property sale_id.s_week_seq <- date_id.week_seq;
property sale_id.s_dow <- date_id.dow;

auto weeks_2001 <- date_id.week_seq ? date_id.year = 2001;

with cur_sales as where channel in ('WEB','STORE')
  select s_week_seq as ws, s_dow as dw, sum(val) as sales_amt;
with ftr_sales as where channel in ('WEB','STORE')
  select s_week_seq as ws, s_dow as dw, sum(val) as sales_amt;
"""

PROJ = (
    "cur_sales.ws as out_wk, "
    "(cur_sales.sales_amt ? cur_sales.dw = 0) as sun_cur, "
    "(ftr_sales.sales_amt ? ftr_sales.dw = 0) as sun_ftr"
)


def _query(join_clause: str) -> str:
    return (
        MODEL
        + f"select {PROJ} {join_clause} "
        + "where ftr_sales.sales_amt is not null and cur_sales.ws in weeks_2001 "
        + "order by cur_sales.ws;"
    )


@pytest.fixture
def executor() -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


@pytest.mark.parametrize(
    "join_clause,expected",
    [
        (
            "subset join ftr_sales.ws = cur_sales.ws",
            [(1, 40.0, 40.0), (2, None, None)],
        ),
        (
            "subset join ftr_sales.ws - 53 = cur_sales.ws",
            [(1, 40.0, 50.0), (2, None, 40.0)],
        ),
    ],
    ids=["plain_key", "expression_key"],
)
def test_cross_rowset_membership_sources_existence(executor, join_clause, expected):
    query = _query(join_clause)
    sql = executor.generate_sql(query)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql
    rows = [
        tuple(None if v is None else float(v) for v in row)
        for row in executor.execute_text(query)[0].fetchall()
    ]
    rows = [(int(r[0]), r[1], r[2]) for r in rows]
    assert rows == expected
