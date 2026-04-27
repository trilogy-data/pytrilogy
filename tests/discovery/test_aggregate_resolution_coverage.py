"""Coverage tests for aggregate-resolution patterns.

These verify the planner picks pre-computed aggregate datasources (rather
than rescanning the granular primary table) for common BI shapes:
exact-grain match, rollup, filter pushdown, and dimension-table upgrade.

Each test asserts on the generated SQL — the aggregate's identifier must
appear and the granular `orders` table must NOT, when an aggregate could
satisfy the query. A tightened row assertion is added where correctness
(not just plan choice) needs to be locked in.

A second block of tests runs each query against both `SALES_MODEL` (which
has the pre-computed aggregates) and `SALES_MODEL_PRIMARY_ONLY` (which
strips them, forcing the planner onto the granular `orders` path) and
asserts the rows match. This is the empirical correctness lock — the
fast path must produce the exact same data as the slow path.

A third block checks that non-additive aggregates (`avg`, `count_distinct`)
are NOT rolled up through finer-grain aggregates (which would be wrong),
and verifies the result is correct against the primary path.
"""

import pytest

from trilogy import Dialects

# A small "sales" model with one fact table, two dim tables, and (in
# SALES_MODEL) several pre-computed aggregate datasources at different
# grains. SALES_MODEL_PRIMARY_ONLY omits the aggregate sources so the
# planner is forced onto the granular `orders` path — used by the
# "matches primary path" empirical-correctness checks below.
SALES_MODEL_PRIMARY_ONLY = """
key order_id int;
key customer_id int;
key product_id int;
key order_date date;

property order_id.amount float;
property customer_id.region string;
property product_id.category string;

auto order_count <- count(order_id);
auto total_amount <- sum(amount);

datasource orders (
    order_id: order_id,
    customer_id: ~customer_id,
    product_id: ~product_id,
    order_date: order_date,
    amount: amount,
)
grain (order_id)
query '''
select 1 as order_id, 101 as customer_id, 201 as product_id, DATE '2024-01-15' as order_date, 10.0 as amount
union all
select 2 as order_id, 101 as customer_id, 202 as product_id, DATE '2024-01-16' as order_date, 20.0 as amount
union all
select 3 as order_id, 102 as customer_id, 201 as product_id, DATE '2024-01-15' as order_date, 30.0 as amount
union all
select 4 as order_id, 102 as customer_id, 202 as product_id, DATE '2024-01-17' as order_date, 40.0 as amount
union all
select 5 as order_id, 103 as customer_id, 203 as product_id, DATE '2024-01-17' as order_date, 50.0 as amount
''';

datasource customers (
    customer_id: customer_id,
    region: region,
)
grain (customer_id)
query '''
select 101 as customer_id, 'east' as region
union all
select 102 as customer_id, 'west' as region
union all
select 103 as customer_id, 'east' as region
union all
select 104 as customer_id, 'west' as region
''';

datasource products (
    product_id: product_id,
    category: category,
)
grain (product_id)
query '''
select 201 as product_id, 'A' as category
union all
select 202 as product_id, 'B' as category
union all
select 203 as product_id, 'A' as category
''';
"""


SALES_AGGREGATES = """
# Pre-computed aggregates at various grains.
datasource agg_by_customer (
    customer_id: ~customer_id,
    order_count: order_count,
    total_amount: total_amount,
)
grain (customer_id)
query '''
select 101 as customer_id, 2 as order_count, 30.0 as total_amount
union all
select 102 as customer_id, 2 as order_count, 70.0 as total_amount
union all
select 103 as customer_id, 1 as order_count, 50.0 as total_amount
''';

datasource agg_by_product (
    product_id: ~product_id,
    order_count: order_count,
    total_amount: total_amount,
)
grain (product_id)
query '''
select 201 as product_id, 2 as order_count, 40.0 as total_amount
union all
select 202 as product_id, 2 as order_count, 60.0 as total_amount
union all
select 203 as product_id, 1 as order_count, 50.0 as total_amount
''';

datasource agg_by_date (
    order_date: order_date,
    order_count: order_count,
    total_amount: total_amount,
)
grain (order_date)
query '''
select DATE '2024-01-15' as order_date, 2 as order_count, 40.0 as total_amount
union all
select DATE '2024-01-16' as order_date, 1 as order_count, 20.0 as total_amount
union all
select DATE '2024-01-17' as order_date, 2 as order_count, 90.0 as total_amount
''';

datasource agg_by_customer_date (
    customer_id: ~customer_id,
    order_date: order_date,
    order_count: order_count,
    total_amount: total_amount,
)
grain (customer_id, order_date)
query '''
select 101 as customer_id, DATE '2024-01-15' as order_date, 1 as order_count, 10.0 as total_amount
union all
select 101 as customer_id, DATE '2024-01-16' as order_date, 1 as order_count, 20.0 as total_amount
union all
select 102 as customer_id, DATE '2024-01-15' as order_date, 1 as order_count, 30.0 as total_amount
union all
select 102 as customer_id, DATE '2024-01-17' as order_date, 1 as order_count, 40.0 as total_amount
union all
select 103 as customer_id, DATE '2024-01-17' as order_date, 1 as order_count, 50.0 as total_amount
''';
"""


SALES_MODEL = SALES_MODEL_PRIMARY_ONLY + SALES_AGGREGATES


def _exec(model: str = SALES_MODEL):
    e = Dialects.DUCK_DB.default_executor()
    e.parse_text(model)
    return e


def _rows(model: str, query: str) -> list[tuple]:
    return sorted(tuple(r) for r in _exec(model).execute_text(query)[-1].fetchall())


# ---------- 1. Exact-grain match ----------


def test_exact_grain_match_customer():
    """Query at the exact grain of agg_by_customer should use it."""
    sql = _exec().generate_sql("SELECT customer_id, order_count, total_amount;")[-1]
    assert "agg_by_customer" in sql, sql
    assert '"orders"' not in sql, sql


def test_exact_grain_match_date():
    """Query at the exact grain of agg_by_date should use it."""
    sql = _exec().generate_sql("SELECT order_date, order_count;")[-1]
    assert "agg_by_date" in sql, sql
    assert '"orders"' not in sql, sql


def test_exact_grain_match_customer_date():
    """Query at the exact grain of agg_by_customer_date should use it."""
    sql = _exec().generate_sql(
        "SELECT customer_id, order_date, order_count, total_amount;"
    )[-1]
    assert "agg_by_customer_date" in sql, sql
    assert '"orders"' not in sql, sql


# ---------- 2. Rollup to coarser grain ----------


def test_rollup_from_finer_to_grand_total():
    """Grand-total query (no group by) should roll up from any agg via SUM."""
    e = _exec()
    sql = e.generate_sql("SELECT order_count;")[-1]
    assert '"orders"' not in sql, sql
    rows = e.execute_text("SELECT order_count;")[-1].fetchall()
    assert rows == [(5,)], rows


def test_rollup_customer_date_to_customer():
    """customer_id grain should be served by agg_by_customer (exact) — but
    could also roll up from agg_by_customer_date. Either is acceptable; what
    we lock in is the granular orders table is NOT used."""
    sql = _exec().generate_sql("SELECT customer_id, order_count;")[-1]
    assert '"orders"' not in sql, sql


def test_rollup_customer_date_to_date():
    """order_date grain should be served by agg_by_date (exact) without
    rescanning orders."""
    sql = _exec().generate_sql("SELECT order_date, total_amount;")[-1]
    assert '"orders"' not in sql, sql


# ---------- 3. Filter on grain component ----------


def test_filter_on_grain_in_select():
    """Filter on a grain key that's also in the SELECT — exact-grain
    aggregate should still be used with the filter pushed into it."""
    e = _exec()
    sql = e.generate_sql(
        "WHERE order_date >= '2024-01-16'::date " "SELECT order_date, order_count;"
    )[-1]
    assert "agg_by_date" in sql, sql
    assert '"orders"' not in sql, sql
    rows = sorted(
        e.execute_text(
            "WHERE order_date >= '2024-01-16'::date " "SELECT order_date, order_count;"
        )[-1].fetchall()
    )
    # Two dates >= 2024-01-16: 1 + 2 orders.
    assert len(rows) == 2


def test_filter_on_grain_not_in_select():
    """Filter on a grain key that's NOT in the SELECT — must use the finer
    aggregate, push the filter down, then SUM-rollup to the SELECT grain.
    (This is the case where the recently-added force-INNER-join logic
    matters — joins to dimension upgrades must not leak NULL rows.)"""
    e = _exec()
    sql = e.generate_sql(
        "WHERE order_date >= '2024-01-16'::date " "SELECT customer_id, order_count;"
    )[-1]
    assert "agg_by_customer_date" in sql, sql
    assert '"orders"' not in sql, sql
    rows = sorted(
        e.execute_text(
            "WHERE order_date >= '2024-01-16'::date " "SELECT customer_id, order_count;"
        )[-1].fetchall()
    )
    # Customers with orders on/after 2024-01-16: 101 (1), 102 (1), 103 (1).
    assert rows == [(101, 1), (102, 1), (103, 1)], rows


# ---------- 4. Partial-key dimension upgrade ----------


def test_partial_key_upgrade_via_dimension_table():
    """customer_id is partial in agg_by_customer; customers table provides
    the full enumeration. Joining the two should serve the query without
    scanning orders."""
    e = _exec()
    sql = e.generate_sql("SELECT customer_id, order_count;")[-1]
    assert "agg_by_customer" in sql, sql
    assert '"orders"' not in sql, sql


def test_partial_key_upgrade_with_filter():
    """Same partial-key upgrade, plus a WHERE on a grain column not in the
    SELECT. The dimension upgrade joins must be INNER under this path so
    we don't get spurious customer_ids with NULL counts."""
    e = _exec()
    sql = e.generate_sql(
        "WHERE order_date >= '2024-01-16'::date " "SELECT customer_id, order_count;"
    )[-1]
    assert "agg_by_customer_date" in sql, sql
    assert '"orders"' not in sql, sql
    rows = sorted(
        e.execute_text(
            "WHERE order_date >= '2024-01-16'::date " "SELECT customer_id, order_count;"
        )[-1].fetchall()
    )
    # Strictly the customers with orders matching the filter — no NULLs.
    assert rows == [(101, 1), (102, 1), (103, 1)], rows


# ---------- 5. Dimension-attribute join ----------


def test_dimension_attribute_with_aggregate():
    """region is a property of customer_id (in customers); order_count is
    in agg_by_customer at customer_id grain. Joining on customer_id lets
    the query land on the aggregate plus the dim table — never on orders.
    The merge SUM-rolls customer_id-grain counts up to region grain."""
    e = _exec()
    sql = e.generate_sql("SELECT region, order_count;")[-1]
    assert "agg_by_customer" in sql, sql
    assert '"orders"' not in sql, sql
    rows = sorted(e.execute_text("SELECT region, order_count;")[-1].fetchall())
    # east: customers 101 (2) + 103 (1) = 3.
    # west: customers 102 (2) + 104 (no orders, NULL summed as 0) = 2.
    assert rows == [("east", 3), ("west", 2)], rows


def test_dimension_filter_with_aggregate():
    """WHERE on a dim attribute (region), aggregate at customer grain. The
    customer_id join keys connect them; orders should not be scanned. The
    WHERE is applied at the customers dim, then joined, then SUM-rolled."""
    e = _exec()
    sql = e.generate_sql("WHERE region = 'east' SELECT region, order_count;")[-1]
    assert "agg_by_customer" in sql, sql
    assert '"orders"' not in sql, sql
    rows = sorted(
        e.execute_text("WHERE region = 'east' SELECT region, order_count;")[
            -1
        ].fetchall()
    )
    assert rows == [("east", 3)], rows


# ---------- 6. Multiple aggregates from same materialization ----------


def test_multiple_aggregates_at_same_grain():
    """order_count and total_amount are both materialized at customer_id
    grain in the same datasource. One scan should serve both."""
    e = _exec()
    sql = e.generate_sql("SELECT customer_id, order_count, total_amount;")[-1]
    assert "agg_by_customer" in sql, sql
    assert '"orders"' not in sql, sql
    assert sql.count("agg_by_customer") < 5, "expected single read of agg_by_customer"


# ---------- 7. Granular fall-back when no aggregate fits ----------


def test_falls_back_to_orders_for_order_id_grain():
    """No aggregate exists at order_id grain; the planner must fall back
    to the granular orders table for an order-level projection."""
    sql = _exec().generate_sql("SELECT order_id, amount;")[-1]
    assert '"orders"' in sql, sql


# ---------- 8. Multi-dimension rollup ----------


def test_rollup_with_multiple_grain_keys_dropped():
    """Drop multiple grain components: query at customer alone should be
    served by an aggregate one or more grain components beyond the query
    grain (e.g. agg_by_customer_date) — not orders."""
    sql = _exec().generate_sql("SELECT customer_id, total_amount;")[-1]
    assert '"orders"' not in sql, sql


# ---------- 9. Order-by / limit / output ordering ----------


def test_aggregate_with_order_by_and_limit():
    e = _exec()
    sql = e.generate_sql(
        "SELECT customer_id, order_count ORDER BY order_count desc LIMIT 2;"
    )[-1]
    assert "agg_by_customer" in sql, sql
    assert '"orders"' not in sql, sql


# ---------- 10. Aggregate path produces same rows as primary path ----------


# Each query must produce identical result rows whether the planner uses
# the pre-computed aggregate or falls back to scanning the granular
# `orders` table. The aggregate path is just an optimization — never a
# semantic change. We exclude order-sensitive queries (LIMIT changes which
# rows you get when there are ties) since the agg vs primary plans can
# tie-break differently.
EQUIVALENT_QUERIES = [
    # Exact-grain matches.
    "SELECT customer_id, order_count, total_amount;",
    "SELECT order_date, order_count;",
    "SELECT customer_id, order_date, order_count, total_amount;",
    # Rollup.
    "SELECT order_count;",
    "SELECT customer_id, order_count;",
    "SELECT order_date, total_amount;",
    "SELECT customer_id, total_amount;",
    # Filter pushdown.
    "WHERE order_date >= '2024-01-16'::date " "SELECT order_date, order_count;",
    "WHERE order_date >= '2024-01-16'::date " "SELECT customer_id, order_count;",
    # Property-of-key rollup via dim join.
    "SELECT region, order_count;",
    "WHERE region = 'east' SELECT region, order_count;",
]


@pytest.mark.parametrize("query", EQUIVALENT_QUERIES)
def test_aggregate_path_matches_primary_path(query):
    """The pre-aggregated path must be byte-equivalent to the granular
    path. Runs each query against SALES_MODEL (planner picks aggregate)
    and SALES_MODEL_PRIMARY_ONLY (planner falls back to orders) and
    asserts the rows are identical."""
    rows_with = _rows(SALES_MODEL, query)
    rows_without = _rows(SALES_MODEL_PRIMARY_ONLY, query)
    assert rows_with == rows_without, (rows_with, rows_without)


# ---------- 11. Non-additive aggregates must NOT be rolled up ----------


# `count_distinct` and `avg` aren't additive: SUM-rolling per-bucket
# values gives the wrong answer. The planner must either compute fresh
# from primary or pick a same-grain materialization, never SUM-roll.
SALES_MODEL_NON_ADDITIVE = SALES_MODEL_PRIMARY_ONLY + SALES_AGGREGATES + """
auto avg_amount <- avg(amount);
auto distinct_customers <- count_distinct(customer_id);
"""


def test_avg_not_rolled_up_from_finer_grain():
    """avg(amount) at region grain cannot be rolled up by SUM from a
    customer-grain aggregate (would double-count or weight wrongly).
    Result must match the primary computation."""
    e = Dialects.DUCK_DB.default_executor()
    e.parse_text(SALES_MODEL_NON_ADDITIVE)
    sql = e.generate_sql("SELECT region, avg_amount;")[-1]
    # The agg-by-customer materialization is on `total_amount`, not avg —
    # the planner must NOT pretend it can serve avg via rollup.
    assert (
        "agg_by_customer" not in sql
    ), f"avg must not roll up through agg_by_customer: {sql}"
    rows_agg = _rows(SALES_MODEL_NON_ADDITIVE, "SELECT region, avg_amount;")
    rows_primary = _rows(
        SALES_MODEL_PRIMARY_ONLY + "auto avg_amount <- avg(amount);",
        "SELECT region, avg_amount;",
    )
    assert rows_agg == rows_primary, (rows_agg, rows_primary)


def test_count_distinct_not_rolled_up_from_finer_grain():
    """count_distinct(customer_id) at region grain cannot be SUM-rolled
    through a per-customer aggregate (DISTINCT doesn't compose under SUM).
    Result must match the primary computation."""
    e = Dialects.DUCK_DB.default_executor()
    e.parse_text(SALES_MODEL_NON_ADDITIVE)
    sql = e.generate_sql("SELECT region, distinct_customers;")[-1]
    assert (
        "agg_by_customer" not in sql
    ), f"count_distinct must not roll up through agg_by_customer: {sql}"
    rows_agg = _rows(SALES_MODEL_NON_ADDITIVE, "SELECT region, distinct_customers;")
    rows_primary = _rows(
        SALES_MODEL_PRIMARY_ONLY
        + "auto distinct_customers <- count_distinct(customer_id);",
        "SELECT region, distinct_customers;",
    )
    assert rows_agg == rows_primary, (rows_agg, rows_primary)


def test_count_distinct_does_not_borrow_signature_from_count():
    """`count_distinct(customer_id)` and `count(customer_id)` share the
    same arguments but are different functions — the planner must not
    treat a `count(...)`-materialized aggregate as a stand-in for
    `count_distinct(...)`."""
    e = Dialects.DUCK_DB.default_executor()
    e.parse_text(SALES_MODEL_NON_ADDITIVE)
    sql = e.generate_sql("SELECT distinct_customers;")[-1]
    # Must not silently SUM the per-customer agg's order_count and call it
    # distinct_customers.
    assert (
        'sum("agg_by_customer"."order_count")' not in sql
    ), f"count_distinct can't be served by SUM-ing count(order_id): {sql}"
    rows_agg = _rows(SALES_MODEL_NON_ADDITIVE, "SELECT distinct_customers;")
    rows_primary = _rows(
        SALES_MODEL_PRIMARY_ONLY
        + "auto distinct_customers <- count_distinct(customer_id);",
        "SELECT distinct_customers;",
    )
    assert rows_agg == rows_primary, (rows_agg, rows_primary)
