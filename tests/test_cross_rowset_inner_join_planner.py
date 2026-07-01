"""Acceptance spec (currently xfail): scoped INNER joins must behave as real
filtering joins, not equivalence collapses.

Today a scoped `inner join a = b` folds both keys onto one canonical column
(union-find + substitution/pseudonym). When only one side contributes projected
columns, discovery sources that side alone and drops the join entirely -- so the
INNER filter is silently lost (fact/dim: a non-matching FK row survives; rowset:
the intersection is lost). When both sides are projected, the pruned side has no
source and the renderer emits `INVALID_REFERENCE_BUG`.

Design notes for the fix (from an initial spike, reverted):
  * De-collapsing the INNER key (keep both endpoints' identity + a mutual
    pseudonym, like LEFT/FULL) is correct and necessary, but not sufficient.
  * Forcing the absent side in by injecting the bare join-key concept works ONLY
    when that key is the other datasource's grain (`fk = b_pk`). For a property/
    FK key shared via import (e.g. `week_seq`, a property of `date`), injecting
    the bare key sources it from the shared dimension, disconnected from the fact
    that owns the relationship -> the join filters against a free-floating
    dimension and the filter is lost (regressed TPC-DS q72 + the inventory/sales
    commutativity test).
  * The correct mechanism is a datasource-level SEMIJOIN: materialize the SPECIFIC
    other datasource grouped to the join key (`a where a.k in (select k from b
    where ...)`), so a shared-column WHERE narrows both sides and there is no
    fan-out.

These tests pin the target behavior and are strict-xfail until that lands.
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

pytestmark = pytest.mark.xfail(
    reason="scoped INNER join collapse drops the filter; fix (datasource-level "
    "semijoin materialization of the absent side) not yet landed",
    strict=True,
)


@pytest.fixture
def executor() -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


# ---- fact/dim INNER join: projecting one side must still filter -------------

FACTDIM = """
key a_id int;
property a_id.fk int;
property a_id.a_val float;
datasource a (id: a_id, f: fk, v: a_val) grain (a_id)
query '''select 1 id, 1 f, 10.0 v union all select 2 id, 2 f, 20.0 v union all select 3 id, 3 f, 30.0 v''';

key b_pk int;
property b_pk.b_val string;
datasource b (id: b_pk, v: b_val) grain (b_pk)
query '''select 1 id, 'one' v union all select 2 id, 'two' v''';
"""


def test_factdim_inner_join_one_side_filters_nonmatching(executor: Executor):
    # a_id=3 has fk=3 with no matching b_pk -> INNER join must drop it.
    query = FACTDIM + "select a_id, a_val inner join fk = b_pk order by a_id;"
    sql = executor.generate_sql(query)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql
    rows = [(int(r[0]), float(r[1])) for r in executor.execute_text(query)[0].fetchall()]
    assert rows == [(1, 10.0), (2, 20.0)]


FANOUT = """
key a_id int;
property a_id.region string;
property a_id.a_val float;
datasource a (id: a_id, r: region, v: a_val) grain (a_id)
query '''select 1 id, 'W' r, 10.0 v union all select 2 id, 'E' r, 20.0 v union all select 3 id, 'Z' r, 30.0 v''';

key b_pk int;
property b_pk.b_region string;
datasource b (id: b_pk, r: b_region) grain (b_pk)
query '''select 10 id, 'W' r union all select 11 id, 'W' r union all select 12 id, 'E' r''';
"""


def test_factdim_inner_join_one_side_no_fanout(executor: Executor):
    # Join on a NON-unique column (region 'W' appears in two b rows). The semijoin
    # must dedup the b side to distinct region -> a_id=1 stays single (no fan-out);
    # region 'Z' has no b match -> a_id=3 dropped.
    query = FANOUT + "select a_id, a_val inner join region = b_region order by a_id;"
    rows = [(int(r[0]), float(r[1])) for r in executor.execute_text(query)[0].fetchall()]
    assert rows == [(1, 10.0), (2, 20.0)]


# ---- rowset-rowset INNER intersection ---------------------------------------

ROWSET = """
key store_sale_id int;
property store_sale_id.s_last_name string;
property store_sale_id.s_yr int;
datasource store_sales (id: store_sale_id, ln: s_last_name, y: s_yr) grain (store_sale_id)
query '''select 1 id, 'Smith' ln, 2000 y union all select 2 id, 'Jones' ln, 2000 y union all select 3 id, 'Smith' ln, 1999 y''';

key cat_sale_id int;
property cat_sale_id.c_last_name string;
property cat_sale_id.c_yr int;
datasource cat_sales (id: cat_sale_id, ln: c_last_name, y: c_yr) grain (cat_sale_id)
query '''select 1 id, 'Smith' ln, 2000 y union all select 2 id, 'Brown' ln, 2000 y''';

rowset store_combos <- where store_sale_id.s_yr = 2000
    select store_sale_id.s_last_name as last_name;
rowset catalog_combos <- where cat_sale_id.c_yr = 2000
    select cat_sale_id.c_last_name as last_name;

rowset sc <-
    select store_combos.last_name as last_name
    inner join store_combos.last_name = catalog_combos.last_name;
"""


def test_rowset_inner_join_intersect_selects_correct_rows(executor: Executor):
    query = ROWSET + "select sc.last_name order by sc.last_name;"
    sql = executor.generate_sql(query)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql
    rows = [r[0] for r in executor.execute_text(query)[0].fetchall()]
    assert rows == ["Smith"]


def test_rowset_inner_join_intersect_count(executor: Executor):
    query = ROWSET + "select count(sc.last_name) as c;"
    sql = executor.generate_sql(query)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql
    rows = [int(r[0]) for r in executor.execute_text(query)[0].fetchall()]
    assert rows == [1]
