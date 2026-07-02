"""Scoped INNER join == set intersection.

Two datasources each carry a value set: `lval` on the left, `rval` on the right.
Writing `inner join lval = rval` declares "these two concepts are the same thing";
where the two sets differ, the output must reflect only the INTERSECTION of them.

These tests pin that intent across:
  * projection shape -- project the left set, the right set, both, or just a count;
  * null handling  -- both sides contain null, neither does, or only one side does.

NULL semantics: a scoped INNER join matches NULL to NULL (`is not distinct from`),
so a null that appears on BOTH sides is part of the intersection, but a null on
only one side has nothing to match and drops.

Every case asserts the semantically-correct answer and is spelled out explicitly
(no parametrization) so the file reads top-to-bottom as a behavior spec. Tests that
fail mark where today's engine does not yet honor the intersection semantics.
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor


@pytest.fixture
def executor() -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


def _rows(executor: Executor, text: str) -> list[tuple]:
    rows = [tuple(r) for r in executor.execute_text(text)[-1].fetchall()]
    # deterministic order with NULLs last, independent of dialect ordering
    return sorted(rows, key=lambda r: tuple((v is None, v) for v in r))


# ---------------------------------------------------------------------------
# Set A: no nulls.  left = {1,2,3}   right = {2,3,4}   intersection = {2,3}
# ---------------------------------------------------------------------------
NO_NULLS = """
key l_id int;
property l_id.lval int;
datasource lefts (id: l_id, v: lval) grain (l_id)
query '''select 1 id, 1 v union all select 2 id, 2 v union all select 3 id, 3 v''';

key r_id int;
property r_id.rval int;
datasource rights (id: r_id, v: rval) grain (r_id)
query '''select 1 id, 2 v union all select 2 id, 3 v union all select 3 id, 4 v''';
"""


def test_no_nulls_project_left_set_is_intersection(executor: Executor):
    # project only the LEFT value -> the left set restricted to the intersection
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    text = NO_NULLS + "select lval inner join lval = rval;"
    assert _rows(executor, text) == [(2,), (3,)]


def test_no_nulls_project_right_set_is_intersection(executor: Executor):
    text = NO_NULLS + "select rval inner join lval = rval;"
    assert _rows(executor, text) == [(2,), (3,)]


def test_no_nulls_project_both_sides(executor: Executor):
    text = NO_NULLS + "select lval, rval inner join lval = rval;"
    assert _rows(executor, text) == [(2, 2), (3, 3)]


def test_no_nulls_count_left_rows_in_intersection(executor: Executor):
    # l_id 2 (val 2) and l_id 3 (val 3) survive; l_id 1 (val 1) drops
    text = NO_NULLS + "select count(l_id) as n inner join lval = rval;"
    assert _rows(executor, text) == [(2,)]


# ---------------------------------------------------------------------------
# Set B: null on BOTH sides.
# left = {1, 2, NULL}   right = {2, 4, NULL}   intersection = {2, NULL}
# (NULL matches NULL via `is not distinct from`)
# ---------------------------------------------------------------------------
BOTH_NULLS = """
key l_id int;
property l_id.lval int;
datasource lefts (id: l_id, v: ?lval) grain (l_id)
query '''select 1 id, 1 v union all select 2 id, 2 v union all select 3 id, cast(null as int) v''';

key r_id int;
property r_id.rval int;
datasource rights (id: r_id, v: ?rval) grain (r_id)
query '''select 1 id, 2 v union all select 2 id, 4 v union all select 3 id, cast(null as int) v''';
"""


def test_both_nulls_project_left_includes_null(executor: Executor):
    # 2 is shared; NULL is on both sides so it is in the intersection
    text = BOTH_NULLS + "select lval inner join lval = rval;"
    assert _rows(executor, text) == [(2,), (None,)]


def test_both_nulls_count_left_rows_in_intersection(executor: Executor):
    # l_id 2 (val 2) and l_id 3 (val NULL) survive
    text = BOTH_NULLS + "select count(l_id) as n inner join lval = rval;"
    assert _rows(executor, text) == [(2,)]


# ---------------------------------------------------------------------------
# Set C: null on ONE side only.
# left = {1, 2, NULL}   right = {2, 3, 4}   intersection = {2}
# (the left NULL has no NULL on the right to match, so it drops)
# ---------------------------------------------------------------------------
ONE_SIDE_NULL = """
key l_id int;
property l_id.lval int;
datasource lefts (id: l_id, v: ?lval) grain (l_id)
query '''select 1 id, 1 v union all select 2 id, 2 v union all select 3 id, cast(null as int) v''';

key r_id int;
property r_id.rval int;
datasource rights (id: r_id, v: rval) grain (r_id)
query '''select 1 id, 2 v union all select 2 id, 3 v union all select 3 id, 4 v''';
"""


def test_one_side_null_project_left_excludes_null(executor: Executor):
    # only 2 is shared; the left NULL has no match on the right
    text = ONE_SIDE_NULL + "select lval inner join lval = rval;"
    assert _rows(executor, text) == [(2,)]


def test_one_side_null_count_left_rows_in_intersection(executor: Executor):
    # only l_id 2 (val 2) survives
    text = ONE_SIDE_NULL + "select count(l_id) as n inner join lval = rval;"
    assert _rows(executor, text) == [(1,)]


# ===========================================================================
# Property-attribute joins: the joined concept is a DIMENSION attribute
# (reachable via an FK from a fact), not a value living on the fact itself.
# This is the shape TPC-DS q72 / the commutativity test use (join on week_seq).
# ===========================================================================

# Single-key: two date dims with DIFFERENT week domains; join on the week attr.
# left domain {1,2,3}   right domain {2,3,4}   intersection {2,3}
PROP_SINGLE_KEY = """
key ld_id int;
property ld_id.lweek int;
datasource ldays (d: ld_id, w: lweek) grain (ld_id)
query '''select 1 d, 1 w union all select 2 d, 2 w union all select 3 d, 3 w''';

key rd_id int;
property rd_id.rweek int;
datasource rdays (d: rd_id, w: rweek) grain (rd_id)
query '''select 2 d, 2 w union all select 3 d, 3 w union all select 4 d, 4 w''';
"""


def test_property_single_key_domain_intersection(executor: Executor):
    # a dimension-attribute join is the intersection of the two domains -- works
    text = PROP_SINGLE_KEY + "select lweek inner join lweek = rweek;"
    assert _rows(executor, text) == [(2,), (3,)]


# Two-key: join on item AND a dimension week attribute, counting one fact by
# (item, week). The fact's OWN week (via its FK) must be honored, but today the
# week key is sourced from the standalone date dim, so a cs row's week is
# overridden and it lands in a bucket its data doesn't point to.
#   cs: item1 sold wk100 & wk200, item2 wk100
#   inv: item1 stocked ONLY wk100, item2 wk100
#   expected (item,week) with a match: (1,100), (2,100)  -- NOT (1,200)
PROP_TWO_KEY = """
key ld_id int;
property ld_id.cs_week int;
datasource ldays (d: ld_id, w: cs_week) grain (ld_id)
query '''select 100 d, 100 w union all select 200 d, 200 w''';

key rd_id int;
property rd_id.inv_week int;
datasource rdays (d: rd_id, w: inv_week) grain (rd_id)
query '''select 100 d, 100 w union all select 200 d, 200 w''';

key cs_id int;
property cs_id.cs_item int;
datasource cs (id: cs_id, it: cs_item, d: ld_id) grain (cs_id)
query '''select 1 id, 1 it, 100 d union all select 2 id, 1 it, 200 d union all select 3 id, 2 it, 100 d''';

key inv_id int;
property inv_id.inv_item int;
datasource inv (id: inv_id, it: inv_item, d: rd_id) grain (inv_id)
query '''select 10 id, 1 it, 100 d union all select 11 id, 2 it, 100 d''';
"""


def test_property_two_key_fact_correlation(executor: Executor):
    text = PROP_TWO_KEY + (
        "select cs_item, cs_week, count(cs_id) as n "
        "inner join cs_item = inv_item inner join cs_week = inv_week;"
    )
    assert _rows(executor, text) == [(1, 100, 1), (2, 100, 1)]


# ===========================================================================
# Rowset <-> rowset intersection: the two sets are ROWSET outputs (each a
# filtered/aggregated select), joined on their output. Same intent -- the
# result is the intersection -- but the join lives at the rowset layer.
# ===========================================================================
ROWSETS = """
key s_id int;
property s_id.s_name string;
property s_id.s_yr int;
datasource stores (id: s_id, n: s_name, y: s_yr) grain (s_id)
query '''select 1 id, 'Smith' n, 2000 y union all select 2 id, 'Jones' n, 2000 y''';

key c_id int;
property c_id.c_name string;
property c_id.c_yr int;
datasource cats (id: c_id, n: c_name, y: c_yr) grain (c_id)
query '''select 1 id, 'Smith' n, 2000 y union all select 2 id, 'Brown' n, 2000 y''';

rowset store_names <- where s_yr = 2000 select s_name as name;
rowset cat_names <- where c_yr = 2000 select c_name as name;
rowset both <- select store_names.name as name
    inner join store_names.name = cat_names.name;
"""


@pytest.mark.xfail(
    reason="rowset<->rowset INNER intersection still routed through the guard / "
    "collapse; not yet migrated to the de-collapse path",
    strict=True,
)
def test_rowset_rowset_intersection(executor: Executor):
    text = ROWSETS + "select both.name order by both.name;"
    assert _rows(executor, text) == [("Smith",)]
