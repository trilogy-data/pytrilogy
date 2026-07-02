"""Set-intersection semantics via a membership WHERE.

Two datasources each carry a value set: `lval` on the left, `rval` on the right.
Restricting the left set to `where lval in rval` keeps only the INTERSECTION of the
two sets (formerly spelled `inner join lval = rval`, a query-scoped join type that
has since been removed from the language).

These tests pin that intent across:
  * projection shape -- project the left set, the right set, or just a count;
  * null handling  -- neither side has null, or only one side does.

Every case asserts the semantically-correct answer and is spelled out explicitly
(no parametrization) so the file reads top-to-bottom as a behavior spec.
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
    text = NO_NULLS + "select lval where lval in rval;"
    assert _rows(executor, text) == [(2,), (3,)]


def test_no_nulls_project_right_set_is_intersection(executor: Executor):
    text = NO_NULLS + "select rval where rval in lval;"
    assert _rows(executor, text) == [(2,), (3,)]


def test_no_nulls_count_left_rows_in_intersection(executor: Executor):
    # l_id 2 (val 2) and l_id 3 (val 3) survive; l_id 1 (val 1) drops
    text = NO_NULLS + "select count(l_id) as n where lval in rval;"
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
    text = ONE_SIDE_NULL + "select lval where lval in rval;"
    assert _rows(executor, text) == [(2,)]


def test_one_side_null_count_left_rows_in_intersection(executor: Executor):
    # only l_id 2 (val 2) survives
    text = ONE_SIDE_NULL + "select count(l_id) as n where lval in rval;"
    assert _rows(executor, text) == [(1,)]


# ===========================================================================
# Property-attribute intersection: the joined concept is a DIMENSION attribute
# (reachable via an FK from a fact), not a value living on the fact itself.
# ===========================================================================

# Single-key: two date dims with DIFFERENT week domains; intersect the week attr.
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
    # a dimension-attribute intersection is the intersection of the two domains
    text = PROP_SINGLE_KEY + "select lweek where lweek in rweek;"
    assert _rows(executor, text) == [(2,), (3,)]
