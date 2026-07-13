"""Two aggregate rowsets joined on a projected key equality PLUS a derived
offset key (q59 store/offset-week shape) must plan and join on exactly the
authored keys.

The subset-side rowset marks its own projected member key partial
(scoped_partial), so enrichment read the node's own output as unsatisfiable
and sourced the "full" column through the anchor rowset — whose symmetric
enrichment requested it back, unbounded (RecursionError surfaced as "query
could not be planned"). Trigger requires the member key to land in
local_optional rather than be the generation target, so the week axis is named
to sort before the store key
(evals/tpcds_agent/bug_q59_projected_week_rowset_join_cannot_be_planned.md).
"""

from pathlib import Path

from tests.join_matrix.harness import make_engine, sort_rows

MODEL = """key sale_id int;
property sale_id.store_key int;
property sale_id.store_label string;
property sale_id.axis_week int;
property sale_id.yr int;
property sale_id.dow int;
property sale_id.price float;
datasource sales (i: sale_id, sk: store_key, sl: store_label, wk: axis_week,
 y: yr, d: dow, p: price)
grain (sale_id)
query '''
select 1 i, 1 sk, 'A' sl, 100 wk, 2001 y, 0 d, 5.0 p
union all select 2, 1, 'A', 100, 2001, 0, 7.0
union all select 3, 1, 'A', 100, 2001, 1, 3.0
union all select 4, 2, 'B', 100, 2001, 0, 9.0
union all select 5, 2, 'B', 101, 2001, 0, 2.0
union all select 6, 1, 'A', 152, 2002, 0, 4.0
union all select 7, 2, 'B', 152, 2002, 0, 3.0
union all select 8, 2, 'B', 154, 2002, 0, 6.0
''';
"""

QUERY = """import sales as ss;

rowset this_yr <-
where ss.yr = 2001
select
    ss.store_key,
    ss.store_label,
    ss.axis_week,
    sum(ss.price ? ss.dow = 0) as sun_sum,
    sum(ss.price ? ss.dow = 1) as mon_sum
;

rowset next_yr <-
where ss.yr = 2002
select
    ss.store_key,
    ss.store_label,
    ss.axis_week,
    sum(ss.price ? ss.dow = 0) as sun_sum,
    sum(ss.price ? ss.dow = 1) as mon_sum
;

select
    this_yr.store_label,
    this_yr.store_key,
    this_yr.axis_week,
    this_yr.sun_sum / next_yr.sun_sum as sun_ratio,
    this_yr.mon_sum / next_yr.mon_sum as mon_ratio
subset join this_yr.store_key = next_yr.store_key
subset join this_yr.axis_week + 52 = next_yr.axis_week
;
"""


def _generate(tmp_path: Path) -> tuple:
    (tmp_path / "sales.preql").write_text(MODEL)
    engine = make_engine(tmp_path)
    statements = engine.parse_text(QUERY)
    sql = engine.generate_sql(statements[-1])[-1]
    return engine, sql


def test_offset_join_between_aggregate_rowsets_plans_on_authored_keys(
    tmp_path: Path,
) -> None:
    engine, sql = _generate(tmp_path)
    assert "INVALID_REFERENCE_BUG" not in sql, sql

    cross_joins = [
        ln
        for ln in sql.splitlines()
        if " JOIN " in ln and "this_yr" in ln and "next_yr" in ln
    ]
    assert len(cross_joins) == 1, sql
    join = cross_joins[0]
    # exactly the two authored predicates: store equality + materialized offset
    assert join.count("=") == 2 and " AND " in join, join
    assert "store_key" in join, join
    assert "virt_func_add" in join, join
    # aggregate payloads are never identity keys
    assert "sun_sum" not in join and "mon_sum" not in join, join

    rows = sort_rows([tuple(r) for r in engine.execute_raw_sql(sql).fetchall()])
    # anchor (next_yr) rows preserved: A/B wk152 match wk100 across the offset;
    # B wk154 has no 2001 wk102 counterpart (NULLs, key coalesces to anchor);
    # this_yr-only B wk101 (offset 153, absent from anchor) drops
    assert rows == [
        ("A", 1, 100, 3.0, None),
        ("B", 2, 100, 3.0, None),
        (None, 2, None, None, None),
    ], rows
