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
"""

import pytest

from trilogy import Dialects

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


def test_forked_with_status(forked):
    """`order_status` reads the order's `amount`. An extension row has no
    order, so it is NULL there like everything outside the span's closure; the
    CASE's ELSE does not fire on padding."""
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
            (None, None, 30, None, None, None, None),
            (None, None, None, 3, None, None, None),
        ]
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
            (None, None, None, 3, "TX", None, None, None, None),
            (None, None, 30, None, None, "C", None, None, None),
        ]
    )


# sales anchors returns' `~` grain keys (the store_sales / store_returns
# shape); return 9 has no sale, return date is a nullable key on returns only.
_ANCHORED = """
key order_id int;
key item_id int;
key date_id int;
property date_id.week int;
properties <order_id, item_id> (
    amount int?,
    refund int?,
);

root datasource sales (
    order_id: order_id,
    item_id: item_id,
    amount: amount,
)
grain (order_id, item_id)
query '''
select 1 as order_id, 10 as item_id, 50 as amount union all
select 1, 20, 60 union all
select 2, 10, 70
''';

root datasource returns (
    order_id: ~order_id,
    item_id: ~item_id,
    date_id: ?date_id,
    refund: refund,
)
grain (order_id, item_id)
query '''
select 1 as order_id, 10 as item_id, 5 as date_id, 5 as refund union all
select 2, 10, 6, 7 union all
select 9, 10, 5, 9
''';

root datasource dates (
    date_id: date_id,
    week: week,
)
grain (date_id)
query '''
select 5 as date_id, 1 as week union all
select 6, 2
''';
"""


@pytest.fixture(scope="module")
def anchored():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_ANCHORED)
    return executor


def test_anchor_exclusive_pin_heals(anchored):
    """A pin on a concept only the `~` fact can supply (its return date) kills
    every sales-only row, so the returns keys heal for the statement: no
    sibling stitch, no sales scan, and the saleless return is a plain fact row."""
    query = "where week = 1 select order_id, sum(refund) as total_refund order by order_id asc;"
    sql = anchored.generate_sql(query)[-1]
    assert "FULL JOIN" not in sql, sql
    assert "50 as amount" not in sql, sql
    assert _rows(anchored, query) == [(1, 5), (9, 9)]


_ANCHOR_NEEDED = "where week = 1 select order_id, sum(refund) as total_refund, sum(amount) as total_amount order by order_id asc;"


def test_anchor_needed_stays_partial(anchored):
    """The same pin beside a sales-only measure: the anchor is not dispensable,
    so the keys stay `~` and sales is still merged in."""
    sql = anchored.generate_sql(_ANCHOR_NEEDED)[-1]
    assert "50 as amount" in sql, sql
    assert _rows(anchored, _ANCHOR_NEEDED)[0] == (1, 5, 50)


@pytest.mark.xfail(
    strict=True,
    reason="pre-existing: the anchor merge renders INNER under the pin, dropping the saleless return",
)
def test_anchor_needed_keeps_saleless_return(anchored):
    """A return with no sale is a fact row of the `~` binding and must survive
    the pin with a NULL amount."""
    assert _rows(anchored, _ANCHOR_NEEDED) == [(1, 5, 50), (9, 9, None)]


# returns binds `returned` as a raw literal: the flag is true on a returns row
# and NULL only where the merge finds no returns row. Line (2, 10) is returned.
_FLAGGED = """
key order_id int;
key item_id int;
properties <order_id, item_id> (
    amount int?,
    returned bool?,
);

root datasource sales (
    order_id: order_id,
    item_id: item_id,
    amount: amount,
)
grain (order_id, item_id)
query '''
select 1 as order_id, 10 as item_id, 50 as amount union all
select 1, 20, 60 union all
select 2, 10, 70
''';

root datasource returns (
    order_id: ~order_id,
    item_id: ~item_id,
    raw(''' true '''): returned,
)
grain (order_id, item_id)
query '''
select 2 as order_id, 10 as item_id
''';
"""


@pytest.fixture(scope="module")
def flagged():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_FLAGGED)
    return executor


def test_absence_pin_on_raw_flag(flagged):
    """`returned is null` tests the ABSENCE of a returns row. That is a
    merge-level fact: pushed into the returns scan it renders `true is null`
    and empties the scan, so every line looks unreturned."""
    query = "where returned is null select order_id, sum(amount) as total order by order_id asc;"
    assert _rows(flagged, query) == [(1, 110)]


def test_presence_pin_on_raw_flag(flagged):
    query = "where returned is not null select order_id, sum(amount) as total order by order_id asc;"
    assert _rows(flagged, query) == [(2, 70)]


# A composite-grain fact: `~product_id` hangs off the whole grain, `~user_id`
# off `order_id` alone, so an aggregate at that grain peels the two extension
# families under different keys.
_COMPOSITE = """
key user_id int;
property user_id.state string;
key product_id int;
property product_id.brand string;
key order_id int;
key line_no int;
property <order_id, line_no>.qty int;

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
)
grain (order_id)
query '''
select 100 as order_id, 1 as user_id union all
select 101, 2 union all
select 102, 1
''';

root datasource lines (
    order_id: order_id,
    line_no: line_no,
    product_id: ~product_id,
    qty: qty,
)
grain (order_id, line_no)
query '''
select 100 as order_id, 1 as line_no, 10 as product_id, 5 as qty union all
select 100, 2, 20, 7 union all
select 101, 1, 10, 11 union all
select 102, 1, 20, 13
''';
"""

_COMPOSITE_ORDER = """ order by order_id asc nulls last, line_no asc nulls last,
    user_id asc nulls last, product_id asc nulls last;"""


@pytest.fixture(scope="module")
def composite():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_COMPOSITE)
    return executor


@pytest.mark.parametrize("metric", ["qty", "sum(qty) as total"])
def test_composite_grain_families_do_not_cross_pair(composite, metric):
    query = f"select order_id, line_no, product_id, user_id, state, {metric}"
    assert _rows(composite, query + _COMPOSITE_ORDER) == [
        (100, 1, 10, 1, "CA", 5),
        (100, 2, 20, 1, "CA", 7),
        (101, 1, 10, 2, "NY", 11),
        (102, 1, 20, 1, "CA", 13),
        (None, None, None, 3, "TX", None),
        (None, None, 30, None, None, None),
    ]


def test_composite_grain_families_with_both_attributes(composite):
    query = "select order_id, line_no, brand, state, sum(qty) as total"
    order = " order by order_id asc nulls last, line_no asc nulls last, state asc nulls last;"
    assert _rows(composite, query + order) == [
        (100, 1, "A", "CA", 5),
        (100, 2, "B", "CA", 7),
        (101, 1, "A", "NY", 11),
        (102, 1, "B", "CA", 13),
        (None, None, None, "TX", None),
        (None, None, "C", None, None),
    ]


def test_composite_grain_families_pinned(composite):
    query = (
        f"{_PIN}select order_id, line_no, product_id, user_id, state, sum(qty) as total"
    )
    assert _rows(composite, query + _COMPOSITE_ORDER) == [
        (100, 1, 10, 1, "CA", 5),
        (100, 2, 20, 1, "CA", 7),
        (101, 1, 10, 2, "NY", 11),
        (102, 1, 20, 1, "CA", 13),
    ]


_COMPOSITE_STATUS = """
auto user_first_qty <- min(qty) by user_id;
auto line_status <- case when qty = user_first_qty then 'FIRST' else 'LATER' end;
"""


def test_composite_grain_families_with_by_span_aggregate():
    """`user_id` as a grouping key keeps its family on the fact bucket while
    `~product_id` peels: two sides padded for different spans never pair."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_COMPOSITE + _COMPOSITE_STATUS)
    query = (
        "select order_id, line_no, product_id, user_id, line_status, sum(qty) as total"
    )
    assert _rows(executor, query + _COMPOSITE_ORDER) == [
        (100, 1, 10, 1, "FIRST", 5),
        (100, 2, 20, 1, "LATER", 7),
        (101, 1, 10, 2, "FIRST", 11),
        (102, 1, 20, 1, "LATER", 13),
        (None, None, None, 3, "LATER", None),
        (None, None, 30, None, "LATER", None),
    ]


def test_status_on_extension_rows_is_null_without_an_aggregate(forked):
    query = """select order_id, user_id, order_status
        order by order_id asc nulls last, user_id asc nulls last;"""
    assert _rows(forked, query) == [
        (100, 1, "FIRST"),
        (101, 2, "FIRST"),
        (102, 1, "LATER"),
        (None, 3, None),
    ]
