"""A bare (no ``by``) aggregate is grain-polymorphic: it co-grains to the
consuming/select grain (grain inheritance), in WHERE and HAVING alike. When
the aggregate's inputs are already AT that grain — ``max(sct.total_spent)``
filtered against ``sct.total_spent`` per ``sct.cid`` — each group holds one
row, so ``max(x) == x`` and the predicate collapses to ``x > 0.5*x`` (true for
every positive row). That tautology is the correct application of the rule,
not a bug: a global reduction must be pinned with ``by *`` (or authored as a
grained select output, e.g. ``rowset m <- select max(sct.total_spent) as mx``).

This file previously guarded a WHERE-only exception
(``_degenerate_aggregate_cograin``) that silently resolved the degenerate case
as a global scalar, making WHERE and HAVING answer differently for the same
construct. That carveout was removed; these tests now pin the uniform
co-grain behavior plus the two correct global authorings.
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

# totals: cust1=100, cust2=500, cust3=50. max=500; 0.5*max=250; only cust2 exceeds.
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

# bare max co-grains to the select grain (per cid) -> one row per group ->
# max(x)==x -> tautology: every positive-total customer passes
TOP_LEVEL = MODEL + """
select sct.cid
where sct.total_spent > 0.5 * max_total
order by sct.cid;
"""

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

# HAVING must agree with WHERE: same bare aggregate, same co-grain, same rows
HAVING_FORM = MODEL + """
with best as
select sct.cid
having sct.total_spent > 0.5 * max_total;
select best.cid order by best.cid;
"""

# `by *` pins the aggregate to the global grain: the intended authoring for a
# global reduction
PINNED_GLOBAL = MODEL + """
auto max_total_global <- max(sct.total_spent) by *;
select sct.cid
where sct.total_spent > 0.5 * max_total_global
order by sct.cid;
"""

# grained select-output form: aggregate outputs of a SELECT are grained to
# that select's grain and do not re-grain when referenced elsewhere
GRAINED_OUTPUT = MODEL + """
with mx as
select max(sct.total_spent) as cmax;
select sct.cid
where sct.total_spent > 0.5 * mx.cmax
order by sct.cid;
"""

# standalone select: query grain is global, so the bare aggregate co-grains to
# global — same rule, no special case
SCALAR_ALONE = MODEL + """
select max_total;
"""

# non-degenerate control: aggregate input (amount, per txn) is finer than the
# select grain (cust), so the co-grain is a meaningful per-cust aggregation
NON_DEGENERATE = MODEL + """
select txn_cust
where sum(amount) > 250.0
order by txn_cust;
"""

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

ALL_CUSTS = [(1,), (2,), (3,)]


@pytest.fixture
def executor(tmp_path: Path) -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


def test_where_bare_max_cograins_to_tautology(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(TOP_LEVEL)[0].fetchall()]
    assert rows == ALL_CUSTS


def test_where_bare_max_wrapped_rowset_cograins(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(WRAPPED)[0].fetchall()]
    assert rows == ALL_CUSTS


def test_where_bare_max_inline_cograins(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(INLINE)[0].fetchall()]
    assert rows == ALL_CUSTS


def test_having_bare_max_matches_where(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(HAVING_FORM)[0].fetchall()]
    assert rows == ALL_CUSTS


def test_by_star_pins_global_grain(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(PINNED_GLOBAL)[0].fetchall()]
    assert rows == [(2,)]


def test_grained_select_output_stays_global(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(GRAINED_OUTPUT)[0].fetchall()]
    assert rows == [(2,)]


def test_scalar_alone_global_grain(executor: Executor):
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
