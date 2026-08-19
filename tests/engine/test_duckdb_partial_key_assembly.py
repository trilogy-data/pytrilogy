"""Row-level contract for multi-group FINAL assembly over `~` (partial) keys.

The passing tests pin the domain-extension semantics the assembly rewrite must
preserve: a `~` key's unmatched dimension rows survive into the output exactly
once, carrying their own attributes, with NULLs elsewhere.

The xfail(strict) tests pin the CORRECT output for column combinations the
assembler currently corrupts (split extension rows, phantom all-NULL rows,
mismatched pair values, or an UnresolvableQueryException). Same model, same
semantics — correctness flips on incidental column choice. They flip to XPASS
when the assembly fix lands; promote them to plain asserts then.
"""

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import UnresolvableQueryException

# users: 3 never orders. products: 30 never sold. items redundantly bind
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
    assert (
        _rows(
            simple,
            """select item_id, order_id, product_id, user_id, state, brand, total_qty
        order by item_id asc nulls last, user_id asc nulls last, product_id asc nulls last;""",
        )
        == [
            (1000, 100, 10, 1, "CA", "A", 5),
            (1001, 100, 20, 1, "CA", "B", 7),
            (1002, 101, 10, 2, "NY", "A", 11),
            (1003, 102, 20, 1, "CA", "B", 13),
            (None, None, None, 3, "TX", None, None),
            (None, None, 30, None, None, "C", None),
        ]
    )


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
    assert (
        _rows(
            simple,
            """select order_id, item_id, product_id, user_id
        order by item_id asc nulls last, user_id asc nulls last, product_id asc nulls last;""",
        )
        == [
            (100, 1000, 10, 1),
            (100, 1001, 20, 1),
            (101, 1002, 10, 2),
            (102, 1003, 20, 1),
            (None, None, None, 3),
            (None, None, 30, None),
        ]
    )


def test_pair_grain_aggregate(forked):
    assert _rows(
        forked,
        "select order_id, product_id, total_pair_cost order by order_id asc nulls last, product_id asc nulls last;",
    ) == [
        (100, 10, 100),
        (100, 20, 150),
        (101, 10, 120),
        (102, 20, 210),
        (None, 30, None),
    ]


def test_forked_with_state(forked):
    assert (
        _rows(
            forked,
            """select item_id, order_id, product_id, user_id, state, total_qty, total_pair_cost
        order by item_id asc nulls last, product_id asc nulls last;""",
        )
        == [
            (1000, 100, 10, 1, "CA", 5, 100),
            (1001, 100, 20, 1, "CA", 7, 150),
            (1002, 101, 10, 2, "NY", 11, 120),
            (1003, 102, 20, 1, "CA", 13, 210),
            (None, None, 30, None, None, None, None),
            (None, None, None, 3, "TX", None, None),
        ]
    )


def test_forked_with_state_and_brand(forked):
    assert (
        _rows(
            forked,
            """select item_id, order_id, product_id, user_id, state, brand, total_qty, total_pair_cost
        order by item_id asc nulls last, product_id asc nulls last;""",
        )
        == [
            (1000, 100, 10, 1, "CA", "A", 5, 100),
            (1001, 100, 20, 1, "CA", "B", 7, 150),
            (1002, 101, 10, 2, "NY", "A", 11, 120),
            (1003, 102, 20, 1, "CA", "B", 13, 210),
            (None, None, 30, None, None, "C", None, None),
            (None, None, None, 3, "TX", None, None, None),
        ]
    )


def test_forked_keys_and_metrics(forked):
    assert (
        _rows(
            forked,
            """select item_id, order_id, product_id, user_id, total_qty, total_pair_cost
        order by item_id asc nulls last, product_id asc nulls last;""",
        )
        == [
            (1000, 100, 10, 1, 5, 100),
            (1001, 100, 20, 1, 7, 150),
            (1002, 101, 10, 2, 11, 120),
            (1003, 102, 20, 1, 13, 210),
            (None, None, 30, None, None, None),
            (None, None, None, 3, None, None),
        ]
    )


def test_forked_with_brand_only(forked):
    assert (
        _rows(
            forked,
            """select item_id, order_id, product_id, user_id, brand, total_qty, total_pair_cost
        order by item_id asc nulls last, product_id asc nulls last;""",
        )
        == [
            (1000, 100, 10, 1, "A", 5, 100),
            (1001, 100, 20, 1, "B", 7, 150),
            (1002, 101, 10, 2, "A", 11, 120),
            (1003, 102, 20, 1, "B", 13, 210),
            (None, None, 30, None, "C", None, None),
            (None, None, None, 3, None, None, None),
        ]
    )


@pytest.mark.xfail(
    strict=True,
    raises=UnresolvableQueryException,
    reason="keyless-join guard rejects keys+metrics+order_status",
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
    reason="full column set fans out to 25 rows with cross-paired keys and values",
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
