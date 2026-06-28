"""Lock: a same-grain scalar BASIC over a WINDOW folds into the window node
(audit: q2.1/q2.2 window/round fusion).

A `round(agg / lead(agg) over (...))` ratio is a scalar projection whose only
non-grain input is the window's output. v3 renders it in the window node's own
SELECT (the `lead(...)` inline inside `round(...)`); v4 used to give the window
and the round separate groups, forcing the window to materialize the lead as a
passthrough column carried through a chain of CTEs. `_merge_basic_into_window_
parent` folds the BASIC into the WINDOW group so the lead renders inline and is
never materialized. The post-window `is not null` defers to FINAL untouched
(condition placement won't host a filter on a window group's own output).

Rows are identical between planners; this is purely plan size / CTE shape."""

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG

_MODEL = """
key sale_id int;
property sale_id.cat string;
property sale_id.wk int;
property sale_id.amt float;
datasource facts ( sid: sale_id, cat: cat, wk: wk, amt: amt )
grain (sale_id)
query '''
select 1 sid, 'A' cat, 1 wk, 10.0 amt union all
select 2 sid, 'A' cat, 1 wk, 20.0 amt union all
select 3 sid, 'A' cat, 2 wk, 40.0 amt union all
select 5 sid, 'A' cat, 3 wk, 80.0 amt union all
select 4 sid, 'B' cat, 1 wk, 5.0 amt
''';
"""

_QUERY = """
auto wk_amt <- sum(amt) by cat, wk;
auto ratio <- round(wk_amt / (lead(wk_amt, 1) over (partition by cat order by wk asc)), 2);
where ratio is not null
select cat, wk, ratio
order by cat asc, wk asc;
"""

_EXPECTED_ROWS = [("A", 1, 0.75), ("A", 2, 0.5)]


def _gen(v4: bool) -> str:
    prior = CONFIG.use_v4_discovery
    CONFIG.use_v4_discovery = v4
    try:
        env, _ = Environment().parse(_MODEL)
        engine = Dialects.DUCK_DB.default_executor(environment=env)
        return engine.generate_sql(_QUERY)[-1]
    finally:
        CONFIG.use_v4_discovery = prior


def _run(v4: bool) -> list[tuple]:
    prior = CONFIG.use_v4_discovery
    CONFIG.use_v4_discovery = v4
    try:
        env, _ = Environment().parse(_MODEL)
        engine = Dialects.DUCK_DB.default_executor(environment=env)
        rows = engine.execute_text(_QUERY)[-1].fetchall()
        return [(r[0], r[1], float(r[2])) for r in rows]
    finally:
        CONFIG.use_v4_discovery = prior


def test_window_basic_merge_rows_match_baseline():
    assert _run(v4=False) == _EXPECTED_ROWS
    assert _run(v4=True) == _EXPECTED_ROWS


def test_window_basic_merge_renders_lead_inline():
    v4_sql = _gen(v4=True)
    # the window result rides inline inside round(...); it is never materialized
    # as a standalone column carried through passthrough CTEs.
    assert ' as "_virt_window_lead' not in v4_sql, v4_sql
    assert "round(" in v4_sql and "lead(" in v4_sql
    # one agg CTE + one fused window/round CTE + one filter CTE, no lead passthrough
    assert v4_sql.count(" as (\n") <= 3, v4_sql
