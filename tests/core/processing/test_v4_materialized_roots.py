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
    _datasource_materializes,
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

# A second BASIC concept (region_caps) with the SAME underlying expression as the
# materialized region_upper but a different name — a canonical-address match must
# let the customers dim satisfy it even though no column is literally named it.
MODEL_CANONICAL_ALIAS = MODEL.rstrip() + "\nauto region_caps <- upper(region);\n"

# Merge-onto-key: orid_2 is an unnest of a constant, merged onto a key the orders
# table binds as a PARTIAL (`~orid`) column. The table holds one row per key, not
# per unnest value, so it must NOT be a materialized source for orid_2.
MODEL_MERGE_ONTO_KEY = """
key orid int;
auto orid_2 <- unnest([1,2,3,4,5]);
property orid.val int;

datasource orders (
    orid: ~orid,
    val: val,
)
grain (orid)
query '''select 1 as orid, 10 as val union all select 2 as orid, 20 as val''';

merge orid into ~orid_2;
"""

# A genuinely persisted unnest column: dim_split binds `split` as a complete
# column at one row per unnest value, alongside the base array source.
MODEL_PERSISTED_UNNEST = """
key scalar int;
property scalar.int_array list<int>;
auto split <- unnest(int_array);

datasource avalues (
    int_array: int_array, scalar: scalar,
)
grain (scalar)
query '''select [1,2,3,4] as int_array, 2 as scalar''';

datasource dim_split (
    scalar: scalar, split: split,
)
grain (scalar)
query '''select 2 as scalar, 1 as split''';
"""

# A partial summary complete only where revenue_band = 'high': its bound aggregate
# is a usable materialized source iff the query's conditions imply that predicate.
# The key is marked partial (`~customer_id`) — a `complete where` source is partial
# outside its predicate, so the parser requires that marker.
MODEL_COMPLETE_WHERE = """
key customer_id int;
property customer_id.revenue_band string;
property customer_id.revenue float;
auto total_revenue <- sum(revenue);

datasource customers (
    customer_id: customer_id, revenue_band: revenue_band, revenue: revenue,
)
grain (customer_id)
query '''select 101 as customer_id, 'high' as revenue_band, 10.0 as revenue''';

datasource high_band_summary (
    customer_id: ~customer_id, total_revenue: total_revenue,
)
grain (customer_id)
complete where revenue_band = 'high'
query '''select 101 as customer_id, 10.0 as total_revenue''';
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


def test_exact_basic_matches_by_canonical():
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


# ---------- sourcing eligibility: canonical, completeness, partial ----------


def test_basic_matches_differently_named_same_expression():
    # region_caps shares region_upper's canonical expression (upper(region)) but no
    # column is named region_caps; the canonical match satisfies it from customers.
    roots = _roots("SELECT customer_id, region_caps;", MODEL_CANONICAL_ALIAS)
    assert "local.region_caps" in roots


def test_merge_onto_key_unnest_not_flagged():
    # orders binds orid_2 only as a PARTIAL (~orid) merge column — one row per key,
    # not per unnest value — so it is not a complete materialized source.
    roots = _roots("SELECT orid_2, val;", MODEL_MERGE_ONTO_KEY)
    assert "local.orid_2" not in roots


def test_persisted_unnest_column_flagged():
    # dim_split binds `split` as a complete column at the post-unnest grain.
    roots = _roots("SELECT scalar, split;", MODEL_PERSISTED_UNNEST)
    assert "local.split" in roots


def test_complete_where_summary_used_when_condition_implied():
    roots = _roots(
        "WHERE revenue_band = 'high' SELECT customer_id, total_revenue;",
        MODEL_COMPLETE_WHERE,
    )
    assert "local.total_revenue" in roots


def test_complete_where_summary_skipped_without_condition():
    # No filter implies the summary's complete-where predicate, so its partial
    # aggregate column can't be trusted; total_revenue stays derived from base.
    roots = _roots("SELECT customer_id, total_revenue;", MODEL_COMPLETE_WHERE)
    assert "local.total_revenue" not in roots


# ---------- _datasource_materializes (predicate, unit) ----------


def test_datasource_materializes_complete_column():
    be, _, _ = _build("SELECT customer_id, region_upper;")
    assert _datasource_materializes(
        be.concepts["local.region_upper"], be.datasources["customers"], None, be
    )


def test_datasource_materializes_rejects_partial_merge_column():
    be, _, _ = _build("SELECT orid_2, val;", MODEL_MERGE_ONTO_KEY)
    assert not _datasource_materializes(
        be.concepts["local.orid_2"], be.datasources["orders"], None, be
    )


def test_datasource_materializes_partial_covered_by_condition():
    be, _, conditions = _build(
        "WHERE revenue_band = 'high' SELECT customer_id, total_revenue;",
        MODEL_COMPLETE_WHERE,
    )
    where = _combine_conditions(conditions)
    summary = be.datasources["high_band_summary"]
    total = be.concepts["local.total_revenue"]
    assert _datasource_materializes(total, summary, where, be)
    # Without the implying condition the partial summary is not a complete source.
    assert not _datasource_materializes(total, summary, None, be)


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
