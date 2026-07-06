"""A bare (no ``by``) aggregate consumed in a WHERE co-grains to the select
grain (HAVING-like). When the aggregate's inputs are already AT that grain —
``max(sct.total_spent)`` filtered against ``sct.total_spent`` per ``sct.cid`` —
the co-grain is degenerate: one row per group, so ``max(x)`` renders as ``x``
and the predicate collapses to ``x > 0.5*x`` (true for every positive row).
Such an aggregate must instead resolve as a global single-row scalar and be
broadcast onto the filter.

Regression for TPC-DS q23 ("best customers"): the agent's idiomatic gate

    with sct as select cust as cid, sum(val) as total_spent;
    auto max_total <- max(sct.total_spent);
    select sct.cid where sct.total_spent > 0.5 * max_total;

silently returned every customer instead of those above half the max.
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

# totals: cust1=100, cust2=500, cust3=50. max=500; 0.5*max=250; only cust2 passes.
MODEL = """
key cust_id int;
key txn_id int;
property txn_id.amount float;
property txn_id.txn_cust int;
datasource txns (t: txn_id, a: amount, c: txn_cust) grain (txn_id)
query '''
select 1 t,  60.0 a, 1 c union all
select 2 t,  40.0 a, 1 c union all
select 3 t, 500.0 a, 2 c union all
select 4 t,  50.0 a, 3 c''';

datasource customers (c: cust_id) grain (cust_id)
query '''
select 1 c union all
select 2 c union all
select 3 c''';

with sct as
select txn_cust as cid, sum(amount) as total_spent;

auto max_total <- max(sct.total_spent);
"""

TOP_LEVEL = MODEL + """
select sct.cid
where sct.total_spent > 0.5 * max_total
order by sct.cid;
"""

# the agent's exact shape: gate consumed through a wrapping rowset
WRAPPED = MODEL + """
with best as
select sct.cid
where sct.total_spent > 0.5 * max_total;
select best.cid order by best.cid;
"""

INLINE = MODEL + """
select sct.cid
where sct.total_spent > 0.5 * max(sct.total_spent)
order by sct.cid;
"""

SCALAR_ALONE = MODEL + """
select max_total;
"""

# non-degenerate control: aggregate input (amount, per txn) is finer than the
# select grain (cust), so the WHERE aggregate must keep HAVING-like co-graining
NON_DEGENERATE = MODEL + """
select txn_cust
where sum(amount) > 250.0
order by txn_cust;
"""

# non-degenerate rowset control: the rowset measure is per txn, coarser select
# grain (cust) -> the WHERE aggregate still co-grains to the select grain
NON_DEGENERATE_ROWSET = MODEL + """
with per_txn as
select txn_id as tid, txn_cust as tcust, amount as amt;

select per_txn.tcust
where sum(per_txn.amt) > 250.0
order by per_txn.tcust;
"""

# base-model row-arg alongside a rowset aggregate: condition-input injection
# only handles all-rowset-sourced row-args, so this shape cannot plan today.
# The invariant is it must NEVER silently drop the filter: correct rows or a
# loud error are both acceptable.
MIXED_BASE_AND_ROWSET_ARG = MODEL + """
with per_txn as
select txn_id as tid, txn_cust as tcust, amount as amt;

select per_txn.tcust
where txn_cust > 0 and sum(per_txn.amt) > 250.0
order by per_txn.tcust;
"""


@pytest.fixture
def executor(tmp_path: Path) -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


def test_where_scalar_max_top_level(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(TOP_LEVEL)[0].fetchall()]
    assert rows == [(2,)]


def test_where_scalar_max_wrapped_rowset(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(WRAPPED)[0].fetchall()]
    assert rows == [(2,)]


def test_where_scalar_max_inline(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(INLINE)[0].fetchall()]
    assert rows == [(2,)]


def test_scalar_alone_unchanged(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(SCALAR_ALONE)[0].fetchall()]
    assert rows == [(500.0,)]


def test_non_degenerate_where_aggregate_keeps_select_grain(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(NON_DEGENERATE)[0].fetchall()]
    assert rows == [(2,)]


def test_non_degenerate_rowset_where_aggregate_keeps_select_grain(
    executor: Executor,
):
    rows = [
        tuple(r) for r in executor.execute_text(NON_DEGENERATE_ROWSET)[0].fetchall()
    ]
    assert rows == [(2,)]


def test_mixed_base_and_rowset_arg_never_silently_drops(executor: Executor):
    try:
        rows = [
            tuple(r)
            for r in executor.execute_text(MIXED_BASE_AND_ROWSET_ARG)[0].fetchall()
        ]
    except Exception:
        return
    assert rows == [(2,)]
