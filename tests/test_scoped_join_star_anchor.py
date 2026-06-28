"""A star scoped join — one anchor key INNER-joined to several spokes — must
resolve and return the same rows regardless of operand order.

Regression for TPC-DS q11: writing the anchor as the LEFT (source) operand of
every join (`a=b`, `a=c`) is the natural star form, but the spoke `c` was only
ever a join TARGET. The INNER membership sets (scoped_merge_sources /
scoped_pseudonym_sources / scoped_rowset_inner_sources) were keyed off the raw
source operand, so `c` — which union-find collapses transitively onto the group
canonical — never got its identity+pseudonym wiring. For rowset spokes that
meant `c` was substituted onto the canonical, dropping its own WHERE filter and
making it unsourceable -> DisconnectedConceptsException. INNER equality is
symmetric, so every collapsed endpoint must be wired, not just the source.
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

# cust 1 in all three years; cust 2 in 2001+2002; cust 3 in 2001 only.
MODEL = """
key sale_id int;
property sale_id.cust int;
property sale_id.yr int;
property sale_id.amt float;
datasource sales (id: sale_id, c: cust, y: yr, a: amt) grain (sale_id)
query '''
select 1 id,1 c,2001 y,10.0 a union all
select 2 id,1 c,2002 y,20.0 a union all
select 3 id,1 c,2003 y,30.0 a union all
select 4 id,2 c,2001 y,5.0 a union all
select 5 id,2 c,2002 y,6.0 a union all
select 6 id,3 c,2001 y,7.0 a''';
"""

_ROWSETS = """
rowset s01 <- where yr=2001 select cust as cust_id, sum(amt) as rev;
rowset s02 <- where yr=2002 select cust as cust_id, sum(amt) as rev;
rowset w01 <- where yr=2003 select cust as cust_id, sum(amt) as rev;
"""

_PROJECTION = "select s01.cust_id, s01.rev as r1, s02.rev as r2, w01.rev as w1\n"
_TAIL = "order by s01.cust_id;"

# anchor s01 is the LEFT operand of BOTH joins — the natural star form.
ANCHOR_LEFT = (
    MODEL
    + _ROWSETS
    + _PROJECTION
    + (
        "inner join s01.cust_id = s02.cust_id\n"
        "inner join s01.cust_id = w01.cust_id\n" + _TAIL
    )
)
# chained `=` — a known-good arrangement of the same group.
CHAINED = (
    MODEL
    + _ROWSETS
    + _PROJECTION
    + ("inner join s01.cust_id = s02.cust_id = w01.cust_id\n" + _TAIL)
)
# anchor on the right of the second join — another known-good arrangement.
ANCHOR_RIGHT = (
    MODEL
    + _ROWSETS
    + _PROJECTION
    + (
        "inner join s01.cust_id = s02.cust_id\n"
        "inner join w01.cust_id = s01.cust_id\n" + _TAIL
    )
)


@pytest.fixture
def executor(tmp_path: Path) -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


def _rows(executor: Executor, text: str) -> list[tuple]:
    return [tuple(r) for r in executor.execute_text(text)[0].fetchall()]


def test_star_anchor_left_resolves_and_filters(executor: Executor):
    # Only cust 1 appears in all three years.
    assert _rows(executor, ANCHOR_LEFT) == [(1, 10.0, 20.0, 30.0)]


def test_star_anchor_left_matches_other_orderings(executor: Executor):
    left = _rows(executor, ANCHOR_LEFT)
    assert left == _rows(executor, CHAINED)
    assert left == _rows(executor, ANCHOR_RIGHT)
