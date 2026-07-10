"""A window ordering by an inline aggregate over a ROLLUP/CUBE pass must not
split the pass and stitch it back on the (nullable) visible dims.

The selected measure and the window's inline aggregate are distinct concepts
(`total` vs `_virt_agg_sum_*`), but they are outputs of ONE grouping pass: the
visible dims are not a row identity across grouping sets (a rolled-up NULL and
a data NULL collide), so any join-back fans subtotal/grand-total rows out
across ranks. All pass outputs must co-source through the window parent in a
single grouped CTE — the fixture's source data carries NULL grouping keys so
an accidental-collision pass cannot mask the fanout. See
evals/tpcds_agent/handoff_q67_inline_window_aggregate_rollup_identity.md (q67).
"""

import pytest

from trilogy import Dialects

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
"""

RAW_BASE = """
select * from (values
 (1, 'a', 'x', 10, 1),
 (2, 'a', 'y', 20, 2),
 (3, 'b', 'x', 5, 3),
 (4, null, null, 7, 4)
) as t(id, g1, g2, v, w)
"""

ORDER = "order by g1 nulls first, g2 nulls first, total nulls first, r nulls first"


def _oracle_sql(measure: str, grouping: str) -> str:
    return f"""
    select g1, g2, sum(v) as total,
      rank() over (partition by g1 order by {measure} desc) as r
    from ({RAW_BASE})
    group by {grouping}
    {ORDER}
    """


CASES = [
    pytest.param(
        f"""
select g1, g2, total,
    rank(g1, g2) over (partition by g1 order by sum(v) desc) as r
by rollup (g1, g2)
{ORDER};
""",
        _oracle_sql("sum(v)", "rollup (g1, g2)"),
        id="inline_same_measure",
    ),
    pytest.param(
        f"""
select g1, g2, total,
    rank(g1, g2) over (partition by g1 order by sum(w) desc) as r
by rollup (g1, g2)
{ORDER};
""",
        _oracle_sql("sum(w)", "rollup (g1, g2)"),
        id="inline_different_measure",
    ),
    pytest.param(
        f"""
select g1, g2, total, wtotal,
    rank(g1, g2) over (partition by g1 order by wtotal desc) as r
by rollup (g1, g2)
{ORDER};
""",
        f"""
    select g1, g2, sum(v) as total, sum(w) as wtotal,
      rank() over (partition by g1 order by sum(w) desc) as r
    from ({RAW_BASE})
    group by rollup (g1, g2)
    {ORDER}
    """,
        id="alias_with_sibling_measure",
    ),
    pytest.param(
        f"""
select g1, g2,
    sum(coalesce(v, 0) * coalesce(w, 0)) as total,
    rank(g1, g2) over (partition by g1 order by sum(coalesce(v, 0) * coalesce(w, 0)) desc) as r
by rollup (g1, g2)
{ORDER};
""",
        f"""
    select g1, g2, sum(coalesce(v, 0) * coalesce(w, 0)) as total,
      rank() over (partition by g1 order by sum(coalesce(v, 0) * coalesce(w, 0)) desc) as r
    from ({RAW_BASE})
    group by rollup (g1, g2)
    {ORDER}
    """,
        id="inline_composite_expression_q67_shape",
    ),
    pytest.param(
        f"""
select g1, g2, total,
    rank(g1, g2) over (partition by g1 order by sum(v) desc) as r
by cube (g1, g2)
{ORDER};
""",
        _oracle_sql("sum(v)", "cube (g1, g2)"),
        id="inline_cube",
    ),
    pytest.param(
        f"""
select g1, g2, total,
    rank(g1, g2) over (partition by g1 order by sum(v) desc) as r
by grouping sets ((g1), (g1, g2))
{ORDER};
""",
        _oracle_sql("sum(v)", "grouping sets ((g1), (g1, g2))"),
        id="inline_grouping_sets",
    ),
]


@pytest.fixture
def executor():
    exec = Dialects.DUCK_DB.default_executor()
    exec.execute_text(FIXTURE)
    return exec


def _norm(rows):
    return sorted(
        (tuple(row) for row in rows),
        key=lambda t: tuple((x is not None, str(x)) for x in t),
    )


@pytest.mark.parametrize("query,oracle", CASES)
def test_single_grouping_pass_no_stitch_join(executor, query, oracle):
    sql = executor.generate_sql(query)[-1]
    groupings = (
        sql.upper().count("ROLLUP")
        + sql.upper().count("CUBE")
        + sql.upper().count("GROUPING SETS")
    )
    assert groupings == 1, sql
    assert " JOIN " not in sql.upper(), sql


@pytest.mark.parametrize("query,oracle", CASES)
def test_rows_match_single_statement_oracle(executor, query, oracle):
    got = _norm(executor.execute_raw_sql(executor.generate_sql(query)[-1]).fetchall())
    expected = _norm(executor.execute_raw_sql(oracle).fetchall())
    assert got == expected


GROUPING_PARTITION_DEFS = """
auto gg <- grouping(g1);
auto bucket <- case when gg = 1 then '~total~' else g1 end;
"""


@pytest.mark.parametrize(
    "partition",
    ["bucket", "grouping(g1)", "case when grouping(g1) = 1 then '~t~' else g1 end"],
    ids=["named_bucket", "direct_grouping", "inline_case_over_grouping"],
)
def test_grouping_derived_partition_keeps_rollup(executor, partition):
    executor.execute_text(GROUPING_PARTITION_DEFS)
    sql = executor.generate_sql(f"""
select g1, g2, total,
    rank(g1, g2) over (partition by {partition} order by total desc) as r
by rollup (g1, g2);
""")[-1]
    assert sql.upper().count("ROLLUP") == 1, sql
    assert " JOIN " not in sql.upper(), sql
    rows = executor.execute_raw_sql(sql).fetchall()
    assert len(rows) == 8


@pytest.mark.xfail(
    strict=True,
    reason="inferred-key `by rollup ()` co-grains the inline window aggregate to "
    "the partition, landing it in a different grouping pass; the cross-pass "
    "stitch join still collides grouping sets (residual)",
)
def test_inferred_rollup_keys_inline_residual(executor):
    query = f"""
select g1, g2, total,
    rank(g1, g2) over (partition by g1 order by sum(v) desc) as r
by rollup ()
{ORDER};
"""
    got = executor.execute_raw_sql(executor.generate_sql(query)[-1]).fetchall()
    assert len(got) == 8
