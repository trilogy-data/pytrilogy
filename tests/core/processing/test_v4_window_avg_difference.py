"""Lock for the join-stream window/regrouped-avg consolidation (tpc-ds q47/q57).

A query that pairs a base aggregate (`sum_amt`), a coarser regrouped aggregate
over it (`avg_amt = avg(sum_amt)`), a window over it (`lag_amt`), and a scalar
difference of the two aggregates (`diff = sum_amt - avg_amt`) used to emit an
extra merge under v4: the difference BASIC merged the base aggregate with the
avg in its *own* CTE, then FINAL re-joined the window -- two joins where one
suffices. Make the window the spine (it already exposes `sum_amt`) and join the
avg straight onto it, computing the difference inline.

`_spine_regraft_parent` (group_graph) reroutes the BASIC's spine input through
the same-grain window sibling so the window's outputs ride through and the two
collapse into one merge. Rows are identical either way; this guards the plan
shape (one join, not two)."""

from trilogy import Dialects, Environment

_MODEL = """
key sale_id int;
property sale_id.cat string;
property sale_id.yr int;
property sale_id.mth int;
property sale_id.amt float;

datasource facts (
    sid: sale_id,
    cat: cat,
    yr: yr,
    mth: mth,
    amt: amt,
)
grain (sale_id)
query '''
select 1 sid, 'A' cat, 2000 yr, 1 mth, 10.0 amt union all
select 2 sid, 'A' cat, 2000 yr, 1 mth, 20.0 amt union all
select 3 sid, 'A' cat, 2000 yr, 2 mth, 40.0 amt union all
select 4 sid, 'B' cat, 2000 yr, 1 mth, 5.0 amt
''';
"""

_QUERY = """
auto sum_amt <- sum(amt) by cat, yr, mth;
auto avg_amt <- avg(sum_amt) by cat, yr;
auto lag_amt <- lag(sum_amt) over (partition by cat order by yr asc, mth asc);
auto diff <- sum_amt - avg_amt;
select
    cat,
    yr,
    mth,
    sum_amt,
    avg_amt,
    diff,
    lag_amt,
order by cat asc, mth asc;
"""

_EXPECTED_ROWS = [
    ("A", 2000, 1, 30.0, 35.0, -5.0, None),
    ("A", 2000, 2, 40.0, 35.0, 5.0, 30.0),
    ("B", 2000, 1, 5.0, 5.0, 0.0, None),
]


def _gen() -> str:
    env, _ = Environment().parse(_MODEL)
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    return engine.generate_sql(_QUERY)[-1]


def _run():
    env, _ = Environment().parse(_MODEL)
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    rows = engine.execute_text(_QUERY)[-1].fetchall()
    return [
        (
            r[0],
            r[1],
            r[2],
            float(r[3]),
            float(r[4]),
            float(r[5]),
            None if r[6] is None else float(r[6]),
        )
        for r in rows
    ]


def test_window_avg_difference_rows_match_baseline():
    assert _run() == _EXPECTED_ROWS


def test_window_avg_difference_consolidates_into_one_join():
    """The avg joins the window stream exactly once.

    Without the spine regraft the difference BASIC builds its own
    base-aggregate-joins-avg merge, so the avg ends up joined twice (its own
    merge + the FINAL re-join of the window). The consolidated shape joins once.
    """
    sql = _gen()
    join_count = sql.count("INNER JOIN") + sql.count("OUTER JOIN")
    assert join_count == 1, f"expected one consolidated join, got {join_count}:\n{sql}"
