from pathlib import Path

import pytest

from trilogy.core.enums import JoinType, Modifier
from trilogy.core.query_processor import process_query
from trilogy.core.statements.author import SelectStatement
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.parsing.exceptions import ParseError
from trilogy.parsing.parse_engine_v2 import parse_text

ORDERS = """key customer_id int;
property customer_id.order_amount float;

datasource orders (
    cid: customer_id,
    amt: order_amount
)
grain (customer_id)
address orders_tbl;
"""

CUSTOMERS = """key customer_id int;
property customer_id.region string;

datasource customers (
    cid: customer_id,
    reg: region
)
grain (customer_id)
address customers_tbl;
"""


@pytest.fixture
def models(tmp_path: Path) -> Path:
    (tmp_path / "orders.preql").write_text(ORDERS)
    (tmp_path / "customers.preql").write_text(CUSTOMERS)
    return tmp_path


def _select(jointype: str) -> str:
    return f"""
import orders as orders;
import customers as customers;

{jointype} JOIN orders.customer_id = customers.customer_id
SELECT
    customers.region,
    sum(orders.order_amount) -> total_amount
ORDER BY customers.region asc;
"""


def test_inner_join_parses_to_select_join(models: Path):
    _, parsed = parse_text(_select("INNER"), root=models)
    stmt = parsed[-1]
    assert isinstance(stmt, SelectStatement)
    assert len(stmt.join_clauses) == 1
    join = stmt.join_clauses[0]
    assert join.join_type is JoinType.INNER
    # source is the joined-in model's key; target is the anchor key
    assert join.source_address == "orders.customer_id"
    assert join.target_address == "customers.customer_id"
    assert join.modifiers == []


def test_left_join_marks_partial(models: Path):
    _, parsed = parse_text(_select("LEFT"), root=models)
    join = parsed[-1].join_clauses[0]
    assert join.join_type is JoinType.LEFT_OUTER
    assert join.modifiers == [Modifier.PARTIAL]


def test_inner_join_blends_both_models(models: Path):
    env, parsed = parse_text(_select("INNER"), root=models)
    sql = DuckDBDialect().compile_statement(process_query(env, parsed[-1]))
    assert "INNER JOIN" in sql
    assert "orders_tbl" in sql and "customers_tbl" in sql


def test_left_join_renders_left_outer(models: Path):
    env, parsed = parse_text(_select("LEFT"), root=models)
    sql = DuckDBDialect().compile_statement(process_query(env, parsed[-1]))
    assert "LEFT OUTER JOIN" in sql


def test_scoped_join_does_not_mutate_global_environment(models: Path):
    env, parsed = parse_text(_select("INNER"), root=models)
    src = env.concepts["orders.customer_id"]
    before = set(src.pseudonyms)
    process_query(env, parsed[-1])
    after = set(env.concepts["orders.customer_id"].pseudonyms)
    assert before == after
    # the anchor key likewise gains no scoped pseudonym in the global env
    assert "orders.customer_id" not in env.concepts["customers.customer_id"].pseudonyms


def test_scoped_join_round_trips_through_render(models: Path):
    from trilogy.parsing.render import render_query

    _, parsed = parse_text(_select("LEFT"), root=models)
    rendered = render_query(parsed[-1])
    assert "left join orders.customer_id = customers.customer_id" in rendered
    text = """
import orders as orders;
import customers as customers;

""" + rendered
    _, reparsed = parse_text(text, root=models)
    join = reparsed[-1].join_clauses[0]
    assert join.join_type is JoinType.LEFT_OUTER
    assert join.source_address == "orders.customer_id"


def test_full_join_not_yet_supported(models: Path):
    with pytest.raises(ParseError, match="not yet supported"):
        parse_text(_select("FULL"), root=models)


DIM_ITEM = """key item_id int;
property item_id.item_name string;

datasource ditems (iid: item_id, nm: item_name)
grain (item_id)
address ditems_tbl;
"""

SALES = """import dim_item as item;

key sale_id int;
property sale_id.amt float;

datasource s (sid: sale_id, iid: item.item_id, a: amt)
grain (sale_id)
address sales_tbl;
"""

CATALOG = """import dim_item as item;

key cat_id int;

datasource c (cid: cat_id, iid: item.item_id)
grain (cat_id)
address catalog_tbl;
"""


@pytest.fixture
def nested_models(tmp_path: Path) -> Path:
    (tmp_path / "dim_item.preql").write_text(DIM_ITEM)
    (tmp_path / "sales.preql").write_text(SALES)
    (tmp_path / "catalog.preql").write_text(CATALOG)
    return tmp_path


def test_join_resolves_nested_namespace_key(nested_models: Path):
    # nested-namespace keys (`catalog.item.item_id`) resolve as fully-addressed
    # concepts; direction is positional (left = source, right = anchor).
    text = """
import sales as sales;
import catalog as catalog;

INNER JOIN catalog.item.item_id = sales.item.item_id
SELECT sales.item.item_id, sum(sales.amt) -> total;
"""
    _, parsed = parse_text(text, root=nested_models)
    join = parsed[-1].join_clauses[0]
    assert join.source_address == "catalog.item.item_id"
    assert join.target_address == "sales.item.item_id"


def test_join_key_must_exist(models: Path):
    from trilogy.core.exceptions import UndefinedConceptException

    text = """
import orders as orders;
import customers as customers;

INNER JOIN orders.nonexistent = customers.customer_id
SELECT customers.region;
"""
    with pytest.raises(UndefinedConceptException, match="orders.nonexistent"):
        parse_text(text, root=models)
