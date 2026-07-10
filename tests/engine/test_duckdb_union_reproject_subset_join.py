"""Composite `subset join` onto a rowset that re-projects a `union(...)` — the
q14 cross-channel intersection family (bug_q14_union_multiselect_output_resolution).

The merge CTE for a composite (2+ key) subset join collapses the RHS onto the
LHS and exposes the RHS key columns as pseudonym-only outputs (no source_map
entry). The renderer recovers such outputs through its pseudonym-candidate
loop, but that loop only catches ValueError: when the RHS column deref chain
bottoms out in a union/multiselect output, ``find_source`` raised RuntimeError
and killed the render before the (perfectly renderable) pseudonym twin was
tried. Single-key joins (semijoin lowering), plain-rowset RHS (deref fails
with a recoverable ValueError), and base-fact LHS (union column still
sourceable) all dodged it — hence the very specific trigger shape.
"""

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import (
    UnionOutputResolutionError,
    UnresolvableQueryException,
)

MODEL = """
key s_id int;
property s_id.s_brand int;
property s_id.s_class int;
property s_id.s_qty int;
datasource store_sales (id: s_id, b: s_brand, c: s_class, q: s_qty) grain (s_id)
query '''select 1 id, 1 b, 1 c, 10 q union all select 2 id, 1 b, 2 c, 5 q union all select 3 id, 2 b, 1 c, 7 q''';

key c_id int;
property c_id.c_brand int;
property c_id.c_class int;
property c_id.c_qty int;
datasource cat_sales (id: c_id, b: c_brand, c: c_class, q: c_qty) grain (c_id)
query '''select 1 id, 1 b, 1 c, 3 q union all select 2 id, 3 b, 1 c, 4 q''';

with all_combos as union(
    (select s_brand as b, s_class as c, 'store' as ch),
    (select c_brand as b, c_class as c, 'catalog' as ch)
) -> (b, c, ch);

with qualifying as
select all_combos.b as brand_id, all_combos.c as class_id;

with cross_channel as
select all_combos.b as brand_id, all_combos.c as class_id
having count_distinct(all_combos.ch) = 2;

with nov_data as
select s_brand as brand_id, s_class as class_id, sum(s_qty) as total_sales;
"""

ALL_NOV_ROWS = [(1, 1, 10), (1, 2, 5), (2, 1, 7)]


@pytest.fixture
def executor():
    exec = Dialects.DUCK_DB.default_executor()
    exec.execute_text(MODEL)
    return exec


def _rows(executor, query):
    return sorted(tuple(r) for r in executor.execute_text(query)[0].fetchall())


def test_composite_subset_join_union_reproject_rowset_lhs(executor):
    """The minimal crash shape (MINL): 2-key subset join, union-reprojected
    RHS, rowset LHS. Nothing from the RHS is selected, so the declaration is
    pure domain metadata and the plan collapses to the LHS alone (merge-form
    parity below pins the same rows)."""
    rows = _rows(
        executor,
        """
select nov_data.brand_id, nov_data.class_id, sum(nov_data.total_sales) as q
subset join nov_data.brand_id = qualifying.brand_id
subset join nov_data.class_id = qualifying.class_id;
""",
    )
    assert rows == ALL_NOV_ROWS


def test_composite_merge_union_reproject_parity(executor):
    """Merge-form parity for the crash shape: `merge a into ~b` is the same
    relation as `subset join a = b` (one relation, two scopes) and must give
    the same rows."""
    rows = _rows(
        executor,
        """
merge nov_data.brand_id into ~qualifying.brand_id;
merge nov_data.class_id into ~qualifying.class_id;
select nov_data.brand_id, nov_data.class_id, sum(nov_data.total_sales) as q;
""",
    )
    assert rows == ALL_NOV_ROWS


def test_composite_subset_join_union_reproject_filtered_matches_membership(executor):
    """The real q14 shape: the RHS is HAVING-restricted to cross-channel
    pairs, so the join must actually filter. Oracle = the canonical concat-key
    membership idiom."""
    rows = _rows(
        executor,
        """
select nov_data.brand_id, nov_data.class_id, sum(nov_data.total_sales) as q
subset join nov_data.brand_id = cross_channel.brand_id
subset join nov_data.class_id = cross_channel.class_id
where cross_channel.brand_id is not null and cross_channel.class_id is not null;
""",
    )
    oracle = _rows(
        executor,
        """
auto s_tuple <- concat(s_brand::string, '|', s_class::string);
with cross_tuples as
select concat(all_combos.b::string, '|', all_combos.c::string) as ck
having count_distinct(all_combos.ch) = 2;
with nov2 as
where s_tuple in cross_tuples.ck
select s_brand as brand_id, s_class as class_id, sum(s_qty) as total_sales;
select nov2.brand_id, nov2.class_id, sum(nov2.total_sales) as q;
""",
    )
    assert rows == oracle
    assert rows == [(1, 1, 10)]


def test_single_key_subset_join_union_reproject(executor):
    """rB: single-key lowers to a semijoin and never crashed — keep it that way."""
    rows = _rows(
        executor,
        """
select nov_data.brand_id, sum(nov_data.total_sales) as q
subset join nov_data.brand_id = qualifying.brand_id;
""",
    )
    assert rows == [(1, 15), (2, 7)]


def test_composite_subset_join_plain_rowset_rhs(executor):
    """rF: composite key but a plain (non-union) rowset RHS — worked via the
    recoverable-ValueError path; guard it."""
    rows = _rows(
        executor,
        """
with plain_combos as
select s_brand as brand_id, s_class as class_id;
select nov_data.brand_id, nov_data.class_id, sum(nov_data.total_sales) as q
subset join nov_data.brand_id = plain_combos.brand_id
subset join nov_data.class_id = plain_combos.class_id;
""",
    )
    assert rows == ALL_NOV_ROWS


@pytest.mark.parametrize(
    "rhs",
    [
        ("qualifying.brand_id", "qualifying.class_id"),
        ("all_combos.b", "all_combos.c"),
    ],
    ids=["union_reproject", "union_direct"],
)
def test_composite_subset_join_base_fact_lhs_clean_error(executor, rhs):
    """Composite subset join declaring raw datasource-bound concepts marks
    their only binding partial, so resolution fails — for ANY RHS kind (plain
    rowset included), pre-existing and orthogonal to the union crash. Pin that
    it stays a clean author-facing error, never an internal planner error."""
    with pytest.raises(UnresolvableQueryException):
        executor.execute_text(f"""
select s_brand, s_class, sum(s_qty) as q
subset join s_brand = {rhs[0]}
subset join s_class = {rhs[1]};
""")


def test_composite_subset_join_direct_union_rhs_rowset_lhs(executor):
    """Sibling cell the report didn't isolate: rowset LHS joined straight onto
    the union outputs (no re-projection layer). The union rowset participates
    for real here (no pseudonym collapse), at its own (b, c, ch) grain: the
    preserving join fans (1, 1) out over both channel rows and null-extends
    the union-only pair (3, 1). Merge form gives the same rows (parity)."""
    query = """
select nov_data.brand_id, nov_data.class_id, sum(nov_data.total_sales) as q
subset join nov_data.brand_id = all_combos.b
subset join nov_data.class_id = all_combos.c;
"""
    rows = _rows(executor, query)
    assert rows == [(1, 1, 20), (1, 2, 5), (2, 1, 7), (3, 1, None)]


def test_composite_subset_join_union_reproject_rollup(executor):
    """The full agent-q14 form: union-reprojected RHS + intersection wnn +
    `by rollup`. test_duckdb_rollup_scoped_join.py covers the rollup family
    with a plain-rowset RHS only; this is the union-RHS cell."""
    rows = executor.execute_text("""
select
    case when grouping(nov_data.brand_id) = 1 then null else nov_data.brand_id end as out_b,
    case when grouping(nov_data.class_id) = 1 then null else nov_data.class_id end as out_c,
    sum(nov_data.total_sales) as q
subset join nov_data.brand_id = cross_channel.brand_id
subset join nov_data.class_id = cross_channel.class_id
where cross_channel.brand_id is not null and cross_channel.class_id is not null
by rollup (nov_data.brand_id, nov_data.class_id);
""")[0].fetchall()
    normed = sorted(
        (tuple(r) for r in rows), key=lambda t: tuple((x is None, x) for x in t)
    )
    assert normed == [(1, 1, 10), (1, None, 10), (None, None, 10)]


def test_union_output_resolution_error_is_recoverable_value_error():
    """The renderer's pseudonym-candidate probing recovers only from
    ValueError; find_source's failure must stay in that hierarchy or the
    composite-subset-join-onto-union family regresses to a hard crash."""
    assert issubclass(UnionOutputResolutionError, ValueError)
