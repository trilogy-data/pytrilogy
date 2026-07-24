"""Query-scoped joins on arbitrary expression keys.

A `join` key may be any expression — a bare concept reference, arithmetic, an
aggregate, or a window — not only a plain concept. Non-reference keys are
materialized as anonymous (virtual) concepts during parsing so the build phase
and discovery source them like any other derived concept. These tests cover
parse shape, end-to-end execution, and the equality-only constraint, on both
grammar backends.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from trilogy import Dialects, Executor
from trilogy.constants import CONFIG, ParserBackend
from trilogy.core.enums import JoinType
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.environment import Environment
from trilogy.parsing.common import VIRTUAL_CONCEPT_PREFIX
from trilogy.parsing.parse_engine_v2 import parse_text
from trilogy.parsing.v2.model import HydrationError

ORDERS = """key oid int;
property oid.amt float;
property oid.ckey int;
datasource orders (oid: oid, amt: amt, ck: ckey)
grain (oid) address orders_tbl;
"""

CUSTOMERS = """key cid int;
property cid.region string;
property cid.rnk int;
datasource customers (cid: cid, reg: region, rk: rnk)
grain (cid) address customers_tbl;
"""

SHIPMENTS = """key skey int;
property skey.scount int;
datasource shipments (sk: skey, sc: scount)
grain (skey) address shipments_tbl;
"""


@pytest.fixture
def models(tmp_path: Path) -> Path:
    (tmp_path / "orders.preql").write_text(ORDERS)
    (tmp_path / "customers.preql").write_text(CUSTOMERS)
    (tmp_path / "shipments.preql").write_text(SHIPMENTS)
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
    eng.execute_raw_sql("create table orders_tbl (oid int, amt double, ck int)")
    eng.execute_raw_sql(
        "insert into orders_tbl values (1,10.0,100),(2,20.0,200),(3,30.0,100)"
    )
    eng.execute_raw_sql("create table customers_tbl (cid int, reg varchar, rk int)")
    eng.execute_raw_sql(
        "insert into customers_tbl values (100,'east',1),(200,'west',2)"
    )
    eng.execute_raw_sql("create table shipments_tbl (sk int, sc int)")
    eng.execute_raw_sql("insert into shipments_tbl values (100,5),(200,7)")
    return eng


def _run(models: Path, backend: ParserBackend, query: str) -> list[tuple]:
    body = "import orders as orders;\nimport customers as customers;\n" + query
    with _using_backend(backend):
        eng = _seeded_executor(models)
        return [tuple(r) for r in eng.execute_text(body)[-1].fetchall()]


BACKENDS = [ParserBackend.PEST, ParserBackend.LARK]


# --- parse shape -----------------------------------------------------------


@pytest.mark.parametrize("backend", BACKENDS)
def test_bare_reference_key_unchanged(models: Path, backend: ParserBackend) -> None:
    text = (
        "import orders as orders;\nimport customers as customers;\n"
        "subset join customers.cid = orders.ckey SELECT customers.region;"
    )
    with _using_backend(backend):
        _, parsed = parse_text(text, root=models)
    join = parsed[-1].join_clauses[0]
    assert join.join_type is JoinType.LEFT_OUTER
    assert join.source_address == "orders.ckey"
    assert join.target_address == "customers.cid"


@pytest.mark.parametrize("backend", BACKENDS)
def test_expression_keys_become_virtual_concepts(
    models: Path, backend: ParserBackend
) -> None:
    text = (
        "import orders as orders;\nimport customers as customers;\n"
        "subset join customers.cid + 1 = orders.ckey + 1 SELECT customers.region;"
    )
    with _using_backend(backend):
        env, parsed = parse_text(text, root=models)
    join = parsed[-1].join_clauses[0]
    # both sides are arithmetic expressions, so each side resolves to a distinct
    # anonymous concept that is committed to the environment for discovery
    assert join.source_address != join.target_address
    assert VIRTUAL_CONCEPT_PREFIX in join.source_address
    assert VIRTUAL_CONCEPT_PREFIX in join.target_address
    assert join.source_address in env.concepts
    assert join.target_address in env.concepts


# --- execution -------------------------------------------------------------


@pytest.mark.parametrize("backend", BACKENDS)
def test_arithmetic_expression_join(models: Path, backend: ParserBackend) -> None:
    # inner intersection expressed as subset join + where the optional side is present
    rows = _run(
        models,
        backend,
        "WHERE customers.region is not null\n"
        "SELECT customers.region, sum(orders.amt) -> total\n"
        "subset join customers.cid + 1 = orders.ckey + 1 ORDER BY customers.region asc;",
    )
    assert rows == [("east", 40.0), ("west", 20.0)]


@pytest.mark.parametrize("backend", BACKENDS)
def test_arithmetic_join_matches_bare_join(
    models: Path, backend: ParserBackend
) -> None:
    prefix = (
        "WHERE customers.region is not null\n"
        "SELECT customers.region, sum(orders.amt) -> total\n"
    )
    suffix = " ORDER BY customers.region asc;"
    bare = _run(
        models, backend, prefix + "subset join customers.cid = orders.ckey" + suffix
    )
    expr = _run(
        models,
        backend,
        prefix + "subset join customers.cid + 0 = orders.ckey + 0" + suffix,
    )
    assert bare == expr == [("east", 40.0), ("west", 20.0)]


@pytest.mark.parametrize("backend", BACKENDS)
def test_aggregate_expression_join(models: Path, backend: ParserBackend) -> None:
    # count of orders per ckey {100: 2, 200: 1}; join that count to customers.rnk
    # {east:1, west:2}: count=2 (ckey 100) -> west, count=1 (ckey 200) -> east.
    # union join + where the optional side is present reproduces the intersection
    # (subset widens on this re-aggregated derived key; union + not-null is faithful).
    rows = _run(
        models,
        backend,
        "WHERE customers.region is not null\n"
        "SELECT customers.region, customers.rnk\n"
        "union join count(orders.oid) by orders.ckey = customers.rnk ORDER BY customers.rnk asc;",
    )
    assert rows == [("east", 1), ("west", 2)]


@pytest.mark.parametrize("backend", BACKENDS)
def test_window_expression_join(models: Path, backend: ParserBackend) -> None:
    # rank orders by amt desc: oid3->1, oid2->2, oid1->3; join rank to
    # customers.rnk {east:1, west:2}. oid3 (amt30)->east, oid2 (amt20)->west,
    # oid1 (rank3) has no matching rnk -> its customer side is null and the
    # where-not-null drops it, reproducing the intersection.
    rows = _run(
        models,
        backend,
        "WHERE customers.region is not null\n"
        "SELECT customers.region, orders.amt\n"
        "union join rank orders.oid order by orders.amt desc = customers.rnk ORDER BY orders.amt desc;",
    )
    assert rows == [("east", 30.0), ("west", 20.0)]


@pytest.mark.parametrize("backend", BACKENDS)
def test_subset_join_on_expression_key(models: Path, backend: ParserBackend) -> None:
    rows = _run(
        models,
        backend,
        "subset join customers.cid + 1 = orders.ckey + 1\n"
        "SELECT customers.region, sum(orders.amt) -> total ORDER BY customers.region asc;",
    )
    assert rows == [("east", 40.0), ("west", 20.0)]


@pytest.mark.parametrize("backend", BACKENDS)
def test_three_part_chained_join(models: Path, backend: ParserBackend) -> None:
    # `a = b = c` collapses all three keys into ONE equivalence group, blending
    # orders, customers, and shipments on the shared customer key.
    rows = _run(
        models,
        backend,
        "import shipments as shipments;\n"
        "WHERE shipments.scount is not null\n"
        "SELECT customers.region, sum(orders.amt) -> total, shipments.scount\n"
        "subset join shipments.skey = customers.cid = orders.ckey "
        "ORDER BY customers.region asc;",
    )
    assert rows == [("east", 40.0, 5), ("west", 20.0, 7)]


@pytest.mark.parametrize("backend", BACKENDS)
def test_three_part_chained_expression_join(
    models: Path, backend: ParserBackend
) -> None:
    # the same three-way blend, but every key in the chain is an expression, so
    # each resolves to its own anonymous concept collapsed into one group.
    rows = _run(
        models,
        backend,
        "import shipments as shipments;\n"
        "WHERE shipments.scount is not null\n"
        "SELECT customers.region, sum(orders.amt) -> total, shipments.scount\n"
        "subset join shipments.skey + 1 = customers.cid + 1 = orders.ckey + 1 "
        "ORDER BY customers.region asc;",
    )
    assert rows == [("east", 40.0, 5), ("west", 20.0, 7)]


# --- constraints -----------------------------------------------------------


@pytest.mark.parametrize("backend", BACKENDS)
def test_non_equality_operator_rejected(models: Path, backend: ParserBackend) -> None:
    text = (
        "import orders as orders;\nimport customers as customers;\n"
        "subset join orders.ckey > customers.cid SELECT customers.region;"
    )
    with _using_backend(backend), pytest.raises(InvalidSyntaxException, match="join"):
        parse_text(text, root=models)


@pytest.mark.parametrize("backend", BACKENDS)
def test_scoped_inner_join_rejected(models: Path, backend: ParserBackend) -> None:
    # query-scoped `inner join` has been removed; use subset/union + a filter.
    text = (
        "import orders as orders;\nimport customers as customers;\n"
        "INNER JOIN orders.ckey = customers.cid SELECT customers.region;"
    )
    with _using_backend(backend), pytest.raises(HydrationError, match="inner` join is not supported"):
        parse_text(text, root=models)


@pytest.mark.parametrize("backend", BACKENDS)
def test_filter_chained_onto_join_points_to_where(
    models: Path, backend: ParserBackend
) -> None:
    text = (
        "import orders as orders;\nimport customers as customers;\n"
        "subset join customers.cid = orders.ckey and orders.amt > 0 "
        "SELECT customers.region;"
    )
    with _using_backend(backend), pytest.raises(
        InvalidSyntaxException, match="Filter input rows in `where`"
    ):
        parse_text(text, root=models)
