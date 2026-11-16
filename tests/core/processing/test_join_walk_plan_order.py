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
