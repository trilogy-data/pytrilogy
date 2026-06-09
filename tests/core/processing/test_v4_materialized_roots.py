"""Unit coverage for v4 materialized-source selection (stage 1).

Exercises `_materialized_root_addresses` (which demanded concepts get sourced
directly from a precomputed / summary datasource instead of re-derived) and its
`_combine_conditions` helper, plus the `materialized_roots` branch of
`build_concept_graph`, against small inline models — no SQL execution.
"""

from __future__ import annotations

import pytest

from trilogy import Environment
from trilogy.core.enums import BooleanOperator, Derivation
from trilogy.core.models.build import BuildConcept, BuildWhereClause, Factory
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.concept_strategies_v4 import (
    _combine_conditions,
    _materialized_root_addresses,
)
from trilogy.core.processing.v4_helper.concept_graph import build_concept_graph
from trilogy.parser import parse

# Base orders fact + a per-customer summary (exact customer grain) + a
# per-customer-per-date summary (finer than customer grain). region_upper is a
# BASIC concept materialized on the customer dimension.
MODEL = """
key order_id int;
key customer_id int;
key order_date date;
property order_id.amount float;
property customer_id.region string;
auto order_count <- count(order_id);
auto total_amount <- sum(amount);
auto region_upper <- upper(region);
auto distinct_customers <- count_distinct(customer_id);

datasource orders (
    order_id: order_id, customer_id: customer_id,
    order_date: order_date, amount: amount,
)
grain (order_id)
query '''select 1 as order_id, 101 as customer_id, date '2024-01-01' as order_date, 10.0 as amount''';

datasource customers (
    customer_id: customer_id, region: region, region_upper: region_upper,
)
grain (customer_id)
query '''select 101 as customer_id, 'east' as region, 'EAST' as region_upper''';

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
"""

# Facts + only a per-customer summary (no per-date table): a filter on
# order_date can't be expressed on any precomputed table.
MODEL_COARSE_SUMMARY_ONLY = """
key order_id int;
key customer_id int;
key order_date date;
property order_id.amount float;
auto order_count <- count(order_id);
auto total_amount <- sum(amount);

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

datasource agg_by_customer (
    customer_id: customer_id, order_count: order_count, total_amount: total_amount,
)
grain (customer_id)
query '''select 101 as customer_id, 1 as order_count, 10.0 as total_amount''';
"""

# Same facts, no summary tables: nothing is materialized to short-circuit to.
MODEL_NO_SUMMARY = """
key order_id int;
key customer_id int;
key order_date date;
property order_id.amount float;
auto order_count <- count(order_id);
auto total_amount <- sum(amount);

datasource orders (
    order_id: order_id, customer_id: customer_id,
    order_date: order_date, amount: amount,
)
grain (order_id)
query '''select 1 as order_id, 101 as customer_id, date '2024-01-01' as order_date, 10.0 as amount''';
"""


def _build(
    select: str, model: str = MODEL
) -> tuple[BuildEnvironment, list[BuildConcept], list[BuildWhereClause]]:
    env = Environment()
    penv, stmts = parse(model + "\n" + select, env)
    built = Factory(environment=penv).build(stmts[-1].as_lineage(penv))
    be = penv.materialize_for_select(built.local_concepts)
    mandatory = [be.concepts.get(c.address, c) for c in built.output_components]
    conditions = [built.where_clause] if built.where_clause else []
    return be, mandatory, conditions


def _roots(select: str, model: str = MODEL) -> set[str]:
    be, mandatory, conditions = _build(select, model)
    return set(_materialized_root_addresses(mandatory, be, conditions))


# ---------- _materialized_root_addresses ----------


def test_empty_mandatory_list():
    env = Environment().materialize_for_select()
    assert _materialized_root_addresses([], env, []) == frozenset()


def test_exact_aggregate_uses_summary_table():
    assert _roots("SELECT customer_id, order_count;") == {"local.order_count"}


def test_inline_aggregate_matches_named_by_canonical():
    # `sum(amount)` canonicalizes to the same address as the bound `total_amount`.
    assert _roots("SELECT customer_id, sum(amount) -> t;") == {"local.t"}


def test_exact_basic_matches_by_address():
    # region_upper is a BASIC concept materialized on the customers dim.
    assert "local.region_upper" in _roots("SELECT customer_id, region_upper;")


def test_plain_key_not_flagged():
    # customer_id is a ROOT key, never a materialized short-circuit candidate.
    assert _roots("SELECT customer_id;") == set()


def test_no_summary_table_nothing_flagged():
    assert _roots("SELECT customer_id, order_count;", MODEL_NO_SUMMARY) == set()


def test_grand_total_rolls_up_from_finer_table():
    # No grand-total table exists, but agg_by_customer (finer) binds order_count
    # and count is additive -> rollup root.
    assert _roots("SELECT order_count;") == {"local.order_count"}


def test_non_additive_not_rolled_up():
    # count_distinct isn't additive and isn't materialized -> no short-circuit.
    assert _roots("SELECT customer_id, distinct_customers;") == set()


def test_filter_below_grain_uses_finer_summary():
    # A predicate on order_date can't be expressed on the customer-grain summary
    # (exact match rejected), but the finer agg_by_customer_date carries the
    # column, so order_count is flagged a rollup root — source planning then pins
    # that table, pushes the filter pre-aggregation, and SUM-rolls to customer.
    roots = _roots(
        "WHERE order_date >= '2024-01-01'::date SELECT customer_id, order_count;"
    )
    assert "local.order_count" in roots


def test_filter_below_grain_without_finer_summary_stays_derived():
    # No summary carries order_date, so the filter can't be pushed pre-aggregation
    # on any precomputed table; order_count must be derived from base orders.
    roots = _roots(
        "WHERE order_date >= '2024-01-01'::date SELECT customer_id, order_count;",
        MODEL_COARSE_SUMMARY_ONLY,
    )
    assert "local.order_count" not in roots


def test_grain_level_filter_still_uses_summary():
    # A predicate on the summary's own grain (customer_id) is expressible there.
    roots = _roots("WHERE customer_id = 101 SELECT customer_id, order_count;")
    assert "local.order_count" in roots


# ---------- _combine_conditions ----------


def test_combine_conditions_empty_is_none():
    assert _combine_conditions([]) is None


def test_combine_conditions_single_passthrough():
    _, _, conditions = _build(
        "WHERE customer_id = 101 SELECT customer_id, order_count;"
    )
    assert len(conditions) == 1
    combined = _combine_conditions(conditions)
    assert combined is not None
    assert combined.conditional == conditions[0].conditional


def test_combine_conditions_ands_multiple():
    _, _, conditions = _build(
        "WHERE customer_id = 101 SELECT customer_id, order_count;"
    )
    combined = _combine_conditions(conditions + conditions)
    assert combined is not None
    assert combined.conditional.operator == BooleanOperator.AND


# ---------- build_concept_graph materialized_roots branch ----------


def test_concept_graph_materialized_root_is_leaf():
    be, mandatory, _ = _build("SELECT customer_id, order_count;")
    roots = frozenset({"local.order_count"})

    _, attrs, edges = build_concept_graph(mandatory, be, [], roots)
    assert attrs["local.order_count"].derivation == Derivation.ROOT
    # A materialized root stops the lineage walk: order_id (count's argument) is
    # never added as an upstream node.
    assert "local.order_id" not in attrs

    # Without the materialized hint the same concept is a derived AGGREGATE whose
    # argument is walked in.
    _, plain_attrs, _ = build_concept_graph(mandatory, be, [])
    assert plain_attrs["local.order_count"].derivation == Derivation.AGGREGATE
    assert "local.order_id" in plain_attrs


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
