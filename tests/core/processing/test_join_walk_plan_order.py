from dataclasses import replace

import pytest

from trilogy.core.processing.join_resolution import (
    JoinFacts,
    PlannedPairs,
    SideFacts,
    resolve_join_order_v2,
    scan_precedence,
)
from trilogy.core.processing.v4_helper.join_plan import JoinPlan, Pairing, PairingKind


def test_a_star_ranks_its_hub_first():
    hub, left, right = object(), object(), object()
    plan = PlannedPairs.star(hub, [(left, frozenset({"k"})), (right, frozenset({"k"}))])
    assert plan.rank(frozenset({"hub"})) == 0
    assert plan.rank(frozenset({"spoke0"})) == plan.rank(frozenset({"spoke1"})) == 1


def test_a_final_plan_states_row_streams_before_their_lookups_and_bridges():
    plan = JoinPlan(
        contributors=("bridge", "dims", "facts", "totals"),
        pins={},
        identity={
            "bridge": frozenset({"k"}),
            "dims": frozenset({"d"}),
            "facts": frozenset({"d", "k", "line"}),
            "totals": frozenset({"d", "k"}),
        },
        carry_demands={},
        pairings=(
            Pairing("bridge", "facts", frozenset({"k"}), PairingKind.LOOKUP),
            Pairing("dims", "facts", frozenset({"d"}), PairingKind.LOOKUP),
            Pairing("dims", "totals", frozenset({"d"}), PairingKind.LOOKUP),
            Pairing("facts", "totals", frozenset({"d", "k"}), PairingKind.LOOKUP),
        ),
        suppliers=frozenset({"dims", "facts", "totals"}),
    )
    assert plan.looked_up == frozenset({"bridge", "dims", "totals"})
    assert plan.precedence == ("facts", "totals", "dims", "bridge")


def test_a_bridge_holding_the_rows_comes_before_the_suppliers_it_looks_up():
    plan = JoinPlan(
        contributors=("by_id", "by_gh", "rows"),
        pins={},
        identity={
            "by_id": frozenset({"id"}),
            "by_gh": frozenset({"g", "h"}),
            "rows": frozenset(),
        },
        carry_demands={},
        pairings=(
            Pairing("by_id", "rows", frozenset({"id"}), PairingKind.LOOKUP),
            Pairing("by_gh", "rows", frozenset({"g", "h"}), PairingKind.LOOKUP),
        ),
        suppliers=frozenset({"by_id", "by_gh"}),
    )
    assert plan.looked_up == frozenset({"by_id", "by_gh"})
    assert plan.precedence == ("rows", "by_gh", "by_id")


def test_a_subset_member_comes_after_the_side_holding_its_anchor():
    plan = JoinPlan(
        contributors=("buyers_a", "buyers_b", "orders"),
        pins={},
        identity={
            "buyers_a": frozenset({"a.cust"}),
            "buyers_b": frozenset({"b.cust"}),
            "orders": frozenset({"id"}),
        },
        carry_demands={},
        pairings=(
            Pairing("buyers_a", "orders", frozenset({"id"}), PairingKind.LOOKUP),
            Pairing("buyers_b", "orders", frozenset({"id"}), PairingKind.LOOKUP),
            Pairing("buyers_a", "buyers_b", frozenset({"id"}), PairingKind.BLEND),
        ),
        suppliers=frozenset({"buyers_a", "buyers_b", "orders"}),
    )
    assert plan.precedence == ("buyers_a", "buyers_b", "orders")
    anchored = replace(plan, subset_sides=frozenset({"buyers_a", "buyers_b"}))
    assert anchored.precedence == ("orders", "buyers_a", "buyers_b")


def test_a_scan_plan_states_rows_before_their_lookups_finer_first():
    keys = {
        frozenset({"sales", "item"}): frozenset({"item.id"}),
        frozenset({"sales", "date"}): frozenset({"date.id"}),
        frozenset({"sales", "returns"}): frozenset({"item.id", "order.id"}),
        frozenset({"returns", "item"}): frozenset({"item.id"}),
    }
    grains = {
        "date": frozenset({"date.id"}),
        "item": frozenset({"item.id"}),
        "returns": frozenset({"item.id", "order.id", "return.line"}),
        "sales": frozenset({"item.id", "order.id", "sale.line", "date.id"}),
        "connector": frozenset(),
    }
    assert scan_precedence(keys, grains) == (
        "sales",
        "returns",
        "connector",
        "date",
        "item",
    )


def test_a_scan_binding_its_keys_completely_comes_before_a_subset_scan():
    keys = {
        frozenset({"returns", "sales"}): frozenset({"item", "order"}),
        frozenset({"sales", "items"}): frozenset({"item"}),
        frozenset({"returns", "items"}): frozenset({"item"}),
        # a group split into projections pairs with itself: no lookup
        frozenset({"sales"}): frozenset({"item", "order"}),
    }
    grains = {
        "items": frozenset({"item"}),
        "returns": frozenset({"item", "order"}),
        "sales": frozenset({"item", "order"}),
    }
    partials = {"returns": frozenset({"item", "order"})}
    assert scan_precedence(keys, grains, partials) == ("sales", "items", "returns")
    assert scan_precedence(keys, grains) == ("returns", "sales", "items")


def test_a_subset_scan_comes_after_its_anchor_even_when_the_anchor_is_a_lookup():
    keys = {
        frozenset({"fact1", "fact2"}): frozenset({"cust"}),
        frozenset({"fact1", "customers"}): frozenset({"cust"}),
        frozenset({"fact2", "customers"}): frozenset({"cust"}),
    }
    grains = {
        "customers": frozenset({"cust"}),
        "fact1": frozenset({"f1"}),
        "fact2": frozenset({"f2"}),
    }
    partials = {"fact1": frozenset({"cust"}), "fact2": frozenset({"cust"})}
    assert scan_precedence(keys, grains, partials) == ("customers", "fact1", "fact2")
    assert scan_precedence(keys, grains) == ("fact1", "fact2", "customers")


class _Scan:
    def __init__(self, grain: set[str], partials: frozenset[str] = frozenset()) -> None:
        self.grain = type("G", (), {"components": grain})()
        self.partial_concepts = [type("C", (), {"address": a})() for a in partials]


def test_a_scan_plan_orders_only_the_bindings_its_datasources_declare():
    keys = {frozenset({"a", "b"}): frozenset({"k", "derived"})}
    scans = {"a": _Scan({"k"}), "b": _Scan({"j"}, {"k"})}
    sides = {1: frozenset({"a"}), 2: frozenset({"b"})}
    plan = PlannedPairs.over_scans(sides, keys, scans)  # type: ignore[arg-type]
    assert plan.orders_bindings and plan.order == ("a", "b")
    scoped = PlannedPairs.over_scans(
        sides, keys, scans, scoped_partials={"derived"}  # type: ignore[arg-type]
    )
    assert not scoped.orders_bindings


@pytest.mark.parametrize("orders_bindings,seed", [(False, "ds~b"), (True, "ds~a")])
def test_the_walk_reads_no_binding_a_plan_orders(orders_bindings, seed):
    facts = replace(
        _star_facts({"ds~a": 0, "ds~b": 1, "ds~c": 2}),
        orders_bindings=orders_bindings,
    )
    facts = replace(
        facts,
        sides={
            **facts.sides,
            "ds~a": SideFacts(columns=frozenset({"c~k"}), partials=frozenset({"c~k"})),
        },
    )
    assert resolve_join_order_v2(facts)[0].lefts.pop() == seed


def _star_facts(ranks: dict[str, int]) -> JoinFacts:
    sides = ["ds~a", "ds~b", "ds~c"]
    return JoinFacts(
        sides={side: SideFacts(columns=frozenset({"c~k"})) for side in sides},
        pair_keys={
            frozenset({x, y}): frozenset({"c~k"})
            for i, x in enumerate(sides)
            for y in sides[i + 1 :]
        },
        side_ranks=ranks,
    )


@pytest.mark.parametrize(
    "ranks,walk",
    [
        ({"ds~a": 0, "ds~b": 1, "ds~c": 2}, ["ds~a", "ds~b", "ds~c"]),
        ({"ds~a": 2, "ds~b": 0, "ds~c": 1}, ["ds~b", "ds~c", "ds~a"]),
    ],
)
def test_sides_alike_join_in_the_plans_order(ranks, walk):
    joins = resolve_join_order_v2(_star_facts(ranks))
    assert [joins[0].lefts.pop(), *(j.right for j in joins)] == walk


def test_keys_alike_rank_by_where_the_plan_puts_their_holders():
    facts = JoinFacts(
        sides={
            "ds~first": SideFacts(columns=frozenset({"c~long_key"})),
            "ds~second": SideFacts(columns=frozenset({"c~long_key", "c~k"})),
            "ds~third": SideFacts(columns=frozenset({"c~k"})),
        },
        pair_keys={
            frozenset({"ds~first", "ds~second"}): frozenset({"c~long_key"}),
            frozenset({"ds~second", "ds~third"}): frozenset({"c~k"}),
        },
        side_ranks={"ds~first": 0, "ds~second": 1, "ds~third": 2},
    )
    joins = resolve_join_order_v2(facts)
    assert joins[0].keys == {"ds~first": {"c~long_key"}}
    assert joins[1].keys == {"ds~second": {"c~k"}}


def _hosted_star(ranks: dict[str, int]) -> JoinFacts:
    facts = _star_facts(ranks)
    return JoinFacts(
        sides={
            **facts.sides,
            "ds~c": SideFacts(columns=frozenset({"c~k"}), hosts=True),
        },
        pair_keys=facts.pair_keys,
        side_ranks=ranks,
    )


@pytest.mark.parametrize(
    "ranks,seed",
    [
        ({"ds~a": 0, "ds~b": 1, "ds~c": 2}, "ds~a"),
        ({"ds~a": 0, "ds~b": 0, "ds~c": 0}, "ds~c"),
    ],
)
def test_hosting_breaks_only_ties_the_plan_leaves(ranks, seed):
    joins = resolve_join_order_v2(_hosted_star(ranks))
    assert joins[0].lefts.pop() == seed
