"""Regression: a HAVING/membership filter must NOT be pushed below a window
function, even when the projected expression mixes a non-windowed term with the
windowed one (``val / lead(val, N) over (...)``). Pushing the filter into the
window CTE restricts the window's input — SQL applies WHERE before window
evaluation — so ``lead/lag`` lose the rows they look ahead/back to and return
NULL. See evals/tpcds_agent/having_pushed_below_window_mixed_expression.md.
"""

from trilogy import Dialects, parse

MODEL = """
key wk int;
property wk.val float;
datasource d ( wk: wk, val: val ) grain (wk)
query '''
select 1 as wk, 10.0 as val
union all select 2, 20.0
union all select 3, 30.0
union all select 4, 40.0
union all select 5, 50.0
union all select 6, 60.0
''';
auto small <- wk ? val <= 30;
"""


def _run(query: str):
    env, queries = parse(MODEL + query)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = executor.generate_sql(queries[-1])[0]
    rows = {r[0]: r[1] for r in executor.execute_query(queries[-1]).fetchall()}
    return sql, rows


def test_mixed_window_expression_keeps_having_above_window():
    # wk=2 is the proof: ratio = val(2)/lead(val,2)=20/40=0.5 requires the window
    # to still see wk=4, which the having filter (wk in {1,2,3}) would push out.
    _, rows = _run(
        "select wk, val / lead(val, 2) over (order by wk asc) as ratio "
        "having wk in small order by wk asc;"
    )
    assert rows == {1: 10 / 30, 2: 20 / 40, 3: 30 / 50}


def test_bare_window_expression_keeps_having_above_window():
    # The window-alone form already worked; assert it still does.
    _, rows = _run(
        "select wk, lead(val, 2) over (order by wk asc) as nxt "
        "having wk in small order by wk asc;"
    )
    assert rows == {1: 30, 2: 40, 3: 50}


def test_membership_not_pushed_into_window_cte():
    # The window and the membership IN must not land in the same CTE: SQL would
    # render the IN as a WHERE that runs before the window.
    sql, _ = _run(
        "select wk, val / lead(val, 2) over (order by wk asc) as ratio "
        "having wk in small order by wk asc;"
    )
    window_line = next(line for line in sql.splitlines() if "lead(" in line)
    cte_body = sql.split(window_line)[1].split("),")[0]
    assert " in (select" not in cte_body.lower()
