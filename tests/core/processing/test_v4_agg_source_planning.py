"""Source-planning for additive-rollup aggregates under a filter, and for
`partial ... complete where` datasources whose predicate the query implies.

These drive `plan_source` end-to-end (so the `_plan_complete_where_source` /
`_plan_finer_filter_rollup` short-circuits and their helpers all run) against
small inline models with summary tables — no SQL execution.
"""

from __future__ import annotations

from trilogy import Environment
from trilogy.core.enums import JoinType
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.build import BuildConcept, BuildDatasource, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.concept_strategies_v4 import V4History
from trilogy.core.processing.nodes import MergeNode
from trilogy.core.processing.v4_helper.source_planning import (
    SourceRequest,
    _finer_filter_rollup_source,
    _plan_complete_where_source,
    _plan_finer_filter_rollup,
    plan_source,
)
from trilogy.parser import parse

# Base orders fact + a per-customer summary (exact customer grain) + a
# per-customer-per-date summary (finer than customer grain).
ROLLUP_MODEL = """
key order_id int;
key customer_id int;
key order_date date;
property order_id.amount float;
auto order_count <- count(order_id);
auto total_amount <- sum(amount);
auto distinct_customers <- count_distinct(customer_id);

datasource orders (
    order_id: order_id, customer_id: customer_id,
    order_date: order_date, amount: amount,
)
grain (order_id)
query '''select 1 as order_id, 101 as customer_id, date '2024-01-01' as order_date, 10.0 as amount''';

datasource agg_by_customer (
    customer_id: customer_id, order_count: order_count, total_amount: total_amount,
)
grain (customer_id)
query '''select 101 as customer_id, 1 as order_count, 10.0 as total_amount''';

datasource agg_by_customer_date (
    customer_id: customer_id, order_date: order_date,
    order_count: order_count, total_amount: total_amount,
)
grain (customer_id, order_date)
query '''select 101 as customer_id, date '2024-01-01' as order_date, 1 as order_count, 10.0 as total_amount''';

datasource agg_by_date (
    order_date: order_date, order_count: order_count,
)
grain (order_date)
query '''select date '2024-01-01' as order_date, 1 as order_count''';
"""

# Only the coarse per-customer summary exists; a filter on order_date can't be
# pushed onto any precomputed table.
ROLLUP_MODEL_COARSE_ONLY = """
key order_id int;
key customer_id int;
key order_date date;
property order_id.amount float;
auto order_count <- count(order_id);

datasource orders (
    order_id: order_id, customer_id: customer_id,
    order_date: order_date, amount: amount,
)
grain (order_id)
query '''select 1 as order_id, 101 as customer_id, date '2024-01-01' as order_date, 10.0 as amount''';

datasource agg_by_customer (
    customer_id: customer_id, order_count: order_count,
)
grain (customer_id)
query '''select 101 as customer_id, 1 as order_count''';
"""

# Finer summary whose grain key is a partial join column (`~customer_id`), so
# the rollup must INNER-join an authoritative `customers` dimension to complete
# the surviving keys (the `_plan_finer_filter_rollup` MergeNode path).
ROLLUP_MODEL_PARTIAL_KEY = """
key order_id int;
key customer_id int;
key order_date date;
property order_id.amount float;
auto order_count <- count(order_id);

datasource orders (
    order_id: order_id, customer_id: customer_id,
    order_date: order_date, amount: amount,
)
grain (order_id)
query '''select 1 as order_id, 101 as customer_id, date '2024-01-01' as order_date, 10.0 as amount''';

datasource customers (
    customer_id: customer_id,
)
grain (customer_id)
query '''select 101 as customer_id''';

datasource agg_by_customer_date (
    customer_id: ~customer_id, order_date: order_date, order_count: order_count,
)
grain (customer_id, order_date)
query '''select 101 as customer_id, date '2024-01-01' as order_date, 1 as order_count''';
"""

# A partial summary pre-filtered to high-revenue customers via an aggregate
# predicate (`complete where customer_revenue > 100`).
COMPLETE_WHERE_MODEL = """
key customer_id int;
property customer_id.amount float;
auto customer_revenue <- sum(amount) by customer_id;

datasource orders (
    customer_id: customer_id, amount: amount,
)
grain (customer_id)
query '''select 101 as customer_id, 250.0 as amount''';

partial datasource high_value_customers (
    customer_id: customer_id, customer_revenue: customer_revenue,
)
grain (customer_id)
complete where customer_revenue > 100
query '''select 101 as customer_id, 250.0 as customer_revenue''';
"""


def _build(
    select: str, model: str = ROLLUP_MODEL
) -> tuple[Environment, BuildEnvironment, list[BuildConcept], BuildWhereClause | None]:
    env = Environment()
    penv, stmts = parse(model + "\n" + select, env)
    from trilogy.core.models.build import Factory

    built = Factory(environment=penv).build(stmts[-1].as_lineage(penv))
    be = penv.materialize_for_select(built.local_concepts)
    mandatory = [be.concepts.get(c.address, c) for c in built.output_components]
    return env, be, mandatory, built.where_clause


def _request(select: str, model: str = ROLLUP_MODEL) -> SourceRequest:
    env, be, mandatory, where = _build(select, model)
    return SourceRequest(
        outputs=mandatory,
        environment=be,
        graph=generate_graph(be),
        history=V4History(base_environment=env),
        conditions=where,
    )


def _names(node) -> set[str]:
    found: set[str] = set()
    stack = [node]
    while stack:
        cur = stack.pop()
        for ds in cur.resolve().datasources:
            found.add(ds.name)
        stack.extend(cur.parents)
    return found


# ---------- _finer_filter_rollup_source ----------


def test_finer_filter_source_picks_finer_summary():
    request = _request(
        "WHERE order_date >= '2024-01-01'::date SELECT customer_id, order_count;"
    )
    ds = _finer_filter_rollup_source(request)
    assert isinstance(ds, BuildDatasource)
    assert ds.name == "agg_by_customer_date"


def test_finer_filter_source_none_without_conditions():
    request = _request("SELECT customer_id, order_count;")
    assert request.conditions is None
    assert _finer_filter_rollup_source(request) is None


def test_finer_filter_source_none_without_additive_aggregate():
    request = _request(
        "WHERE order_date >= '2024-01-01'::date "
        "SELECT customer_id, distinct_customers;"
    )
    assert _finer_filter_rollup_source(request) is None


def test_finer_filter_source_none_when_output_not_key_or_aggregate():
    # order_id is neither a target-grain key (customer_id) nor an aggregate.
    request = _request(
        "WHERE order_date >= '2024-01-01'::date "
        "SELECT customer_id, order_id, order_count;"
    )
    assert _finer_filter_rollup_source(request) is None


def test_finer_filter_source_none_when_filter_at_grain():
    # A predicate on the requested grain (customer_id) is not *finer*, so there
    # is nothing to push pre-aggregation — no pinned source.
    request = _request("WHERE customer_id = 101 SELECT customer_id, order_count;")
    assert _finer_filter_rollup_source(request) is None


def test_finer_filter_source_none_without_finer_summary():
    request = _request(
        "WHERE order_date >= '2024-01-01'::date SELECT customer_id, order_count;",
        ROLLUP_MODEL_COARSE_ONLY,
    )
    assert _finer_filter_rollup_source(request) is None


# ---------- _plan_finer_filter_rollup ----------


def test_plan_finer_filter_rollup_rolls_scan_to_target_grain():
    # The summary's grain key is complete here, so the rollup is a single pinned
    # scan SUM-rolled to the requested grain (no key-completion join needed).
    request = _request(
        "WHERE order_date >= '2024-01-01'::date SELECT customer_id, order_count;"
    )
    node = _plan_finer_filter_rollup(request)
    assert node is not None
    assert {c.address for c in node.output_concepts} == {
        "local.customer_id",
        "local.order_count",
    }
    assert "agg_by_customer_date" in _names(node)


def test_plan_finer_filter_rollup_merges_partial_keys_with_dimension():
    # Partial grain key forces the INNER-join key-completion path (MergeNode).
    request = _request(
        "WHERE order_date >= '2024-01-01'::date SELECT customer_id, order_count;",
        ROLLUP_MODEL_PARTIAL_KEY,
    )
    node = _plan_finer_filter_rollup(request)
    assert isinstance(node, MergeNode)
    assert node.force_join_type == JoinType.INNER
    names = _names(node)
    assert "agg_by_customer_date" in names
    assert "customers" in names


def test_plan_finer_filter_rollup_none_when_no_source():
    request = _request("SELECT customer_id, order_count;")
    assert _plan_finer_filter_rollup(request) is None


def test_plan_source_routes_to_finer_filter_rollup():
    request = _request(
        "WHERE order_date >= '2024-01-01'::date SELECT customer_id, order_count;"
    )
    node = plan_source(request)
    assert node is not None
    assert "agg_by_customer_date" in _names(node)


# ---------- _plan_complete_where_source ----------


def test_complete_where_pins_partial_when_predicate_implied():
    request = _request(
        "WHERE customer_revenue > 100 SELECT customer_id, customer_revenue;",
        COMPLETE_WHERE_MODEL,
    )
    node = _plan_complete_where_source(request)
    assert node is not None
    assert "high_value_customers" in _names(node)
    # The pinned scan is treated as non-partial (predicate already applied).
    assert "local.customer_revenue" not in {
        c.address for c in (node.partial_concepts or [])
    }


def test_complete_where_none_when_predicate_not_implied():
    # A filter on customer_id does NOT imply the datasource's revenue predicate.
    request = _request(
        "WHERE customer_id = 101 SELECT customer_id, customer_revenue;",
        COMPLETE_WHERE_MODEL,
    )
    assert _plan_complete_where_source(request) is None


def test_complete_where_none_without_conditions():
    request = _request("SELECT customer_id, customer_revenue;", COMPLETE_WHERE_MODEL)
    assert _plan_complete_where_source(request) is None
