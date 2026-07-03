"""Contract: an implicit join between two rowsets sourced from the SAME base
table joins ONLY on the authored key equality, never on the shared base key.

Two rowsets `a` and `b` both filter the same `orders` table (on different
properties), so both carry the base `orders.oid`. When the final select joins
them on a DERIVED equality (`a.oid = b.oid + 1`), the generated SQL must join on
exactly that expression and must NOT additionally weld the two rowsets on their
common base `orders.oid` — which would collapse the offset to `b.oid = b.oid+1`
(zero rows) or silently AND-in a base-key condition.

Asserted two ways: the join ON clause carries a single equality that does not
reference b's raw oid (it joins via the materialized `+1` expression), and the
end-to-end result over deliberately disjoint oid sets returns the offset match —
a base-key join would return nothing.
"""

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest

from trilogy import Dialects, Executor
from trilogy.constants import CONFIG, ParserBackend
from trilogy.core.models.environment import Environment

ORDERS = """key oid int;
property oid.status int;
property oid.amt float;
datasource orders (oid: oid, st: status, amt: amt)
grain (oid) address orders_tbl;
"""

# joins are row-preserving (docs/subset_union_join_design.md): both rowsets are
# filtered, so the declared subset cannot be proven and unmatched rows from
# EITHER side survive — the intersection is the explicit both-sides
# `is not null` idiom.
QUERY = """import orders as orders;

with a as select orders.oid as oid, orders.amt as amt where orders.status = 1;
with b as select orders.oid as oid, orders.amt as amt where orders.status = 2;

where a.amt is not null and b.amt is not null
select a.oid, a.amt, b.oid, b.amt
left join a.oid = b.oid + 1
order by a.oid asc;
"""

PRESERVING_QUERY = """import orders as orders;

with a as select orders.oid as oid, orders.amt as amt where orders.status = 1;
with b as select orders.oid as oid, orders.amt as amt where orders.status = 2;

select a.oid, a.amt, b.oid, b.amt
left join a.oid = b.oid + 1
order by a.oid asc nulls last;
"""

BACKENDS = [ParserBackend.PEST, ParserBackend.LARK]


@pytest.fixture
def models(tmp_path: Path) -> Path:
    (tmp_path / "orders.preql").write_text(ORDERS)
    return tmp_path


@contextmanager
def _using_backend(backend: ParserBackend) -> Iterator[None]:
    prev = CONFIG.parser_backend
    CONFIG.parser_backend = backend
    try:
        yield
    finally:
        CONFIG.parser_backend = prev


def _seeded_executor(models: Path) -> Executor:
    eng = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=models)
    )
    eng.execute_raw_sql("create table orders_tbl (oid int, st int, amt double)")
    # a (status 1) = {3, 7}; b (status 2) = {1, 2} — disjoint oid sets, so a base
    # `a.oid = b.oid` join yields zero rows; only the offset `a.oid = b.oid + 1`
    # matches (3 = 2 + 1).
    eng.execute_raw_sql(
        "insert into orders_tbl values (1,2,100.0),(2,2,200.0),(3,1,30.0),(7,1,70.0)"
    )
    return eng


def _join_line(sql: str) -> str:
    lines = [ln for ln in sql.splitlines() if "INNER JOIN" in ln]
    assert len(lines) == 1, f"expected one inner join, got: {lines}"
    return lines[0]


@pytest.mark.parametrize("backend", BACKENDS)
def test_join_condition_is_offset_only(models: Path, backend: ParserBackend) -> None:
    with _using_backend(backend):
        sql = _seeded_executor(models).generate_sql(QUERY)[-1]
    join = _join_line(sql)
    # single equality, never AND-ed with a second (base-key) condition
    assert " and " not in join.lower()
    assert join.count("=") == 1
    # b participates via the materialized `+ 1` expression, never its raw oid
    assert "b_oid" not in join
    assert "+ 1" in sql


@pytest.mark.parametrize("backend", BACKENDS)
def test_offset_join_execution(models: Path, backend: ParserBackend) -> None:
    with _using_backend(backend):
        res = _seeded_executor(models).execute_text(QUERY)[-1]
        rows = [tuple(r) for r in res.fetchall()]
    # a base-key join would return [] (disjoint sets); the offset returns one row
    assert rows == [(3, 30.0, 2, 200.0)]


@pytest.mark.parametrize("backend", BACKENDS)
def test_offset_join_execution_preserving(models: Path, backend: ParserBackend) -> None:
    """Without explicit filters the join preserves: unmatched rows from both
    filtered rowsets survive NULL-padded alongside the offset match."""
    with _using_backend(backend):
        res = _seeded_executor(models).execute_text(PRESERVING_QUERY)[-1]
        rows = [tuple(r) for r in res.fetchall()]
    assert (3, 30.0, 2, 200.0) in rows
    assert (7, 70.0, None, None) in rows
    # the b-only row's a.oid coalesces with the derived key (b.oid + 1 = 2)
    assert (2, None, 1, 100.0) in rows
