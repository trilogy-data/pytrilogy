"""Row-level contract for queries over `~` (partial) key bindings.

Semantics (see ``trilogy.core.processing.partial_bridging``):

- A SINGLE live partial key extends: its unmatched dimension rows survive into
  the output exactly once, carrying their own attributes, with NULLs elsewhere.
- A multi-`~` fact anchors the result whether or not its row identity is in
  the output: each row is a fact row (projected to the requested grain) or one
  dimension's extension row. Extension families never cross-pair — a customer
  with no orders and a product never sold yield two rows, not an invented
  pairing.
- A not-null pin on the partial keys kills every extension row a partial key
  could license, the bindings heal to complete for the statement, and the
  query plans as a plain star over the fact's own rows — asserted here as the
  ``_PIN`` variant of each spanning shape.

The xfail(strict) tests pin the CORRECT output for shapes a pre-existing
non-partial discovery defect still corrupts (a by-key aggregate compared
against a row value beside additional keys — reproduces with no `~` in the
model at all). They flip to XPASS when that fix lands; promote them to plain
asserts then.
"""

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import UnresolvableQueryException

# users: 1 never orders. products: 1 never sold. items redundantly bind
# ~user_id (the thelook order_items shape).
_SIMPLE = """
key user_id int;
property user_id.state string;
key product_id int;
property product_id.brand string;
key order_id int;
property order_id.amount int;
key item_id int;
property item_id.qty int;

auto total_qty <- sum(qty);
auto total_amount <- sum(amount);

root datasource users (
    user_id: user_id,
    state: state,
)
grain (user_id)
query '''
select 1 as user_id, 'CA' as state union all
select 2, 'NY' union all
select 3, 'TX'
''';

root datasource products (
    product_id: product_id,
    brand: brand,
)
grain (product_id)
query '''
select 10 as product_id, 'A' as brand union all
select 20, 'B' union all
select 30, 'C'
''';

root datasource orders (
    order_id: order_id,
    user_id: ~user_id,
    amount: amount,
)
grain (order_id)
query '''
select 100 as order_id, 1 as user_id, 50 as amount union all
select 101, 2, 60 union all
select 102, 1, 70
''';

root datasource items (
    item_id: item_id,
    order_id: order_id,
    product_id: ~product_id,
    user_id: ~user_id,
    qty: qty,
)
grain (item_id)
query '''
select 1000 as item_id, 100 as order_id, 10 as product_id, 1 as user_id, 5 as qty union all
select 1001, 100, 20, 1, 7 union all
select 1002, 101, 10, 2, 11 union all
select 1003, 102, 20, 1, 13
''';
"""

# _SIMPLE plus the group-forking derivations of the thelook sales_reporting
# model: a (product x order)-grain scalar, its aggregate, and a by-user
# aggregate compared against a row value.
_FORKED = (
    _SIMPLE.replace(
        "auto total_qty <- sum(qty);",
        """property product_id.cost int;

auto total_qty <- sum(qty);
auto pair_cost <- cost * amount;
auto total_pair_cost <- sum(pair_cost);
auto user_first_amount <- min(amount) by user_id;
auto order_status <- case when amount = user_first_amount then 'FIRST' else 'LATER' end;""",
    )
    .replace(
        "select 10 as product_id, 'A' as brand union all\nselect 20, 'B' union all\nselect 30, 'C'",
        "select 10 as product_id, 'A' as brand, 2 as cost union all\nselect 20, 'B', 3 union all\nselect 30, 'C', 4",
    )
    .replace(
        "    brand: brand,\n)",
        "    brand: brand,\n    cost: cost,\n)",
    )
)

_PIN = "where product_id is not null and user_id is not null\n"


@pytest.fixture(scope="module")
def simple():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_SIMPLE)
    return executor


@pytest.fixture(scope="module")
def forked():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_FORKED)
    return executor


def _rows(executor, query: str):
    return [tuple(r) for r in executor.execute_text(query)[0].fetchall()]


def test_all_keys_dims_and_metric(simple):
    query = """select item_id, order_id, product_id, user_id, state, brand, total_qty
        order by item_id asc nulls last, user_id asc nulls last, product_id asc nulls last;"""
    assert _rows(simple, query) == [
        (1000, 100, 10, 1, "CA", "A", 5),
        (1001, 100, 20, 1, "CA", "B", 7),
        (1002, 101, 10, 2, "NY", "A", 11),
        (1003, 102, 20, 1, "CA", "B", 13),
        (None, None, None, 3, "TX", None, None),
        (None, None, 30, None, None, "C", None),
    ]
    assert _rows(simple, _PIN + query) == [
        (1000, 100, 10, 1, "CA", "A", 5),
        (1001, 100, 20, 1, "CA", "B", 7),
        (1002, 101, 10, 2, "NY", "A", 11),
        (1003, 102, 20, 1, "CA", "B", 13),
    ]


def test_dim_only_uses_dim_domain(simple):
    assert _rows(simple, "select user_id, state order by user_id asc;") == [
        (1, "CA"),
        (2, "NY"),
        (3, "TX"),
    ]


def test_metric_by_complete_dim_attribute(simple):
    assert _rows(simple, "select state, total_qty order by state asc nulls last;") == [
        ("CA", 25),
        ("NY", 11),
        ("TX", None),
    ]


def test_metric_by_partial_key(simple):
    assert _rows(
        simple, "select user_id, total_amount order by user_id asc nulls last;"
    ) == [(1, 120), (2, 60), (3, None)]


def test_metrics_from_two_grains(simple):
    assert _rows(
        simple,
        "select order_id, total_amount, total_qty order by order_id asc nulls last;",
    ) == [(100, 50, 12), (101, 60, 11), (102, 70, 13)]


def test_by_dim_key_aggregate_vs_row_value(simple):
    assert (
        _rows(
            simple,
            """auto user_max_amount <- max(amount) by user_id;
        select order_id, amount, user_max_amount, (amount = user_max_amount) -> is_biggest
        order by order_id asc nulls last;""",
        )
        == [
            (100, 50, 70, False),
            (101, 60, 60, True),
            (102, 70, 70, True),
            (None, None, None, None),
        ]
    )


def test_keys_only(simple):
    query = """select order_id, item_id, product_id, user_id
        order by item_id asc nulls last, user_id asc nulls last, product_id asc nulls last;"""
    assert _rows(simple, query) == [
        (100, 1000, 10, 1),
        (100, 1001, 20, 1),
        (101, 1002, 10, 2),
        (102, 1003, 20, 1),
        (None, None, None, 3),
        (None, None, 30, None),
    ]
    assert _rows(simple, _PIN + query) == [
        (100, 1000, 10, 1),
        (100, 1001, 20, 1),
        (101, 1002, 10, 2),
        (102, 1003, 20, 1),
    ]


def test_keys_without_fact_anchor(simple):
    """The pair grain WITHOUT the fact's own row key: fact pairs projected to
    the pair grain, plus one extension row per unmatched member of each `~`
    dimension — never a cross-pairing of the two extension families."""
    query = "select user_id, product_id order by user_id asc nulls last, product_id asc nulls last;"
    assert _rows(simple, query) == [
        (1, 10),
        (1, 20),
        (2, 10),
        (3, None),
        (None, 30),
    ]
    assert _rows(simple, _PIN + query) == [
        (1, 10),
        (1, 20),
        (2, 10),
    ]


def test_dims_without_fact_anchor(simple):
    """The flagship shape: customer attributes x product attributes, related
    only by the partial fact — pair rows plus each side's extension rows."""
    query = "select state, brand order by state asc nulls last, brand asc nulls last;"
    assert _rows(simple, query) == [
        ("CA", "A"),
        ("CA", "B"),
        ("NY", "A"),
        ("TX", None),
        (None, "C"),
    ]
    assert _rows(simple, _PIN + query) == [
        ("CA", "A"),
        ("CA", "B"),
        ("NY", "A"),
    ]


def test_pair_grain_aggregate(forked):
    # order_id is complete in items, so only product's extension family is in
    # play — a single-family span stays generatable, extension row included.
    query = "select order_id, product_id, total_pair_cost order by order_id asc nulls last, product_id asc nulls last;"
    assert _rows(forked, query) == [
        (100, 10, 100),
        (100, 20, 150),
        (101, 10, 120),
        (102, 20, 210),
        (None, 30, None),
    ]
    assert _rows(forked, _PIN + query) == [
        (100, 10, 100),
        (100, 20, 150),
        (101, 10, 120),
        (102, 20, 210),
    ]


def test_forked_with_state(forked):
    query = """select item_id, order_id, product_id, user_id, state, total_qty, total_pair_cost
        order by item_id asc nulls last, product_id asc nulls last;"""
    assert _rows(forked, query) == [
        (1000, 100, 10, 1, "CA", 5, 100),
        (1001, 100, 20, 1, "CA", 7, 150),
        (1002, 101, 10, 2, "NY", 11, 120),
        (1003, 102, 20, 1, "CA", 13, 210),
        (None, None, 30, None, None, None, None),
        (None, None, None, 3, "TX", None, None),
    ]
    assert _rows(forked, _PIN + query) == [
        (1000, 100, 10, 1, "CA", 5, 100),
        (1001, 100, 20, 1, "CA", 7, 150),
        (1002, 101, 10, 2, "NY", 11, 120),
        (1003, 102, 20, 1, "CA", 13, 210),
    ]


def test_forked_with_state_and_brand(forked):
    query = """select item_id, order_id, product_id, user_id, state, brand, total_qty, total_pair_cost
        order by item_id asc nulls last, product_id asc nulls last;"""
    assert _rows(forked, query) == [
        (1000, 100, 10, 1, "CA", "A", 5, 100),
        (1001, 100, 20, 1, "CA", "B", 7, 150),
        (1002, 101, 10, 2, "NY", "A", 11, 120),
        (1003, 102, 20, 1, "CA", "B", 13, 210),
        (None, None, 30, None, None, "C", None, None),
        (None, None, None, 3, "TX", None, None, None),
    ]
    assert _rows(forked, _PIN + query) == [
        (1000, 100, 10, 1, "CA", "A", 5, 100),
        (1001, 100, 20, 1, "CA", "B", 7, 150),
        (1002, 101, 10, 2, "NY", "A", 11, 120),
        (1003, 102, 20, 1, "CA", "B", 13, 210),
    ]


def test_forked_keys_and_metrics(forked):
    query = """select item_id, order_id, product_id, user_id, total_qty, total_pair_cost
        order by item_id asc nulls last, product_id asc nulls last;"""
    assert _rows(forked, query) == [
        (1000, 100, 10, 1, 5, 100),
        (1001, 100, 20, 1, 7, 150),
        (1002, 101, 10, 2, 11, 120),
        (1003, 102, 20, 1, 13, 210),
        (None, None, 30, None, None, None),
        (None, None, None, 3, None, None),
    ]
    assert _rows(forked, _PIN + query) == [
        (1000, 100, 10, 1, 5, 100),
        (1001, 100, 20, 1, 7, 150),
        (1002, 101, 10, 2, 11, 120),
        (1003, 102, 20, 1, 13, 210),
    ]


def test_forked_with_brand_only(forked):
    query = """select item_id, order_id, product_id, user_id, brand, total_qty, total_pair_cost
        order by item_id asc nulls last, product_id asc nulls last;"""
    assert _rows(forked, query) == [
        (1000, 100, 10, 1, "A", 5, 100),
        (1001, 100, 20, 1, "B", 7, 150),
        (1002, 101, 10, 2, "A", 11, 120),
        (1003, 102, 20, 1, "B", 13, 210),
        (None, None, 30, None, "C", None, None),
        (None, None, None, 3, None, None, None),
    ]
    assert _rows(forked, _PIN + query) == [
        (1000, 100, 10, 1, "A", 5, 100),
        (1001, 100, 20, 1, "B", 7, 150),
        (1002, 101, 10, 2, "A", 11, 120),
        (1003, 102, 20, 1, "B", 13, 210),
    ]


@pytest.mark.xfail(
    strict=True,
    raises=UnresolvableQueryException,
    reason=(
        "pre-existing non-partial defect: a by-key aggregate compared against "
        "a row value beside additional keys trips the keyless-join guard "
        "(reproduces with no `~` in the model at all)"
    ),
)
def test_forked_with_status(forked):
    assert (
        _rows(
            forked,
            """select item_id, order_id, product_id, user_id, order_status, total_qty, total_pair_cost
        order by item_id asc nulls last, product_id asc nulls last;""",
        )
        == [
            (1000, 100, 10, 1, "FIRST", 5, 100),
            (1001, 100, 20, 1, "FIRST", 7, 150),
            (1002, 101, 10, 2, "FIRST", 11, 120),
            (1003, 102, 20, 1, "LATER", 13, 210),
            (None, None, 30, None, "LATER", None, None),
            (None, None, None, 3, "LATER", None, None),
        ]
    )


@pytest.mark.xfail(
    strict=True,
    raises=UnresolvableQueryException,
    reason=(
        "same keyless-join guard rejection as test_forked_with_status, under "
        "the pin that heals the `~` keys — the defect is grain composition, "
        "not partiality"
    ),
)
def test_forked_with_status_pinned(forked):
    assert (
        _rows(
            forked,
            _PIN
            + """select item_id, order_id, product_id, user_id, order_status, total_qty, total_pair_cost
        order by item_id asc nulls last;""",
        )
        == [
            (1000, 100, 10, 1, "FIRST", 5, 100),
            (1001, 100, 20, 1, "FIRST", 7, 150),
            (1002, 101, 10, 2, "FIRST", 11, 120),
            (1003, 102, 20, 1, "LATER", 13, 210),
        ]
    )


@pytest.mark.xfail(
    strict=True,
    reason="full column set fans out with cross-paired keys and values",
)
def test_forked_full_column_set(forked):
    assert (
        _rows(
            forked,
            """select item_id, order_id, product_id, user_id, state, brand, order_status, total_qty, total_pair_cost
        order by item_id asc nulls last, user_id asc nulls last, product_id asc nulls last;""",
        )
        == [
            (1000, 100, 10, 1, "CA", "A", "FIRST", 5, 100),
            (1001, 100, 20, 1, "CA", "B", "FIRST", 7, 150),
            (1002, 101, 10, 2, "NY", "A", "FIRST", 11, 120),
            (1003, 102, 20, 1, "CA", "B", "LATER", 13, 210),
            (None, None, None, 3, "TX", None, "LATER", None, None),
            (None, None, 30, None, None, "C", "LATER", None, None),
        ]
    )
