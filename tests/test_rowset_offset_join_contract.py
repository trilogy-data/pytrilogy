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

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

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

# the derived-key relation declares (b.oid + 1) ⊆ a.oid, so `a` is the anchor
# and the join narrows to a directional LEFT (a rowset output is complete at its
# own rename boundary). The explicit both-sides `is not null` idiom below further
# restricts to the intersection.
QUERY = """import orders as orders;

with a as select orders.oid as oid, orders.amt as amt where orders.status = 1;
with b as select orders.oid as oid, orders.amt as amt where orders.status = 2;

where a.amt is not null and b.amt is not null
select a.oid, a.amt, b.oid, b.amt
subset join b.oid + 1 = a.oid
order by a.oid asc;
"""

PRESERVING_QUERY = """import orders as orders;

with a as select orders.oid as oid, orders.amt as amt where orders.status = 1;
with b as select orders.oid as oid, orders.amt as amt where orders.status = 2;

select a.oid, a.amt, b.oid, b.amt
subset join b.oid + 1 = a.oid
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
def test_offset_join_narrows_to_anchor(models: Path, backend: ParserBackend) -> None:
    """The derived-key relation declares (b.oid + 1) ⊆ a.oid, so `a` is the
    superset anchor: the offset match and a's unmatched rows survive, while the
    b-only row (derived key 2, absent from a) drops — directional LEFT."""
    with _using_backend(backend):
        res = _seeded_executor(models).execute_text(PRESERVING_QUERY)[-1]
        rows = [tuple(r) for r in res.fetchall()]
    assert rows == [(3, 30.0, 2, 200.0), (7, 70.0, None, None)]
