from dataclasses import replace

import pytest

from tests.helpers.walk_order import flip_walk_spelling
from trilogy.core.enums import JoinType
from trilogy.core.processing.join_resolution import (
    JoinFacts,
    PlannedPairs,
    SideFacts,
    _shape_terms,
    _stated_order,
    resolve_join_order_v2,
    scan_precedence,
)
from trilogy.core.processing.nodes.merge_node import _forced_seed
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


def _bound(partials: dict[str, set[str]], arm: bool = False) -> dict[str, SideFacts]:
    return {
        side: SideFacts(
            columns=frozenset({"cust", side}),
            partials=frozenset(partials.get(side, ())),
            arm=arm and side in partials,
        )
        for side in ("customers", "fact1", "fact2")
    }


_CUST_PAIRS = {
    frozenset(pair): frozenset({"cust"})
    for pair in (("fact1", "fact2"), ("fact1", "customers"), ("fact2", "customers"))
}


def _order(ranks: dict[str, int]) -> list[str]:
    return sorted(ranks, key=lambda side: (ranks[side], side))


def test_a_subset_side_comes_after_its_anchor_wherever_the_plan_put_it():
    plan = {"fact1": 0, "fact2": 1, "customers": 2}
    facts = _bound({"fact1": {"cust"}, "fact2": {"cust"}})
    ranks = _stated_order(
        plan, JoinFacts(sides=facts, pair_keys=_CUST_PAIRS), frozenset()
    )
    assert _order(ranks) == ["customers", "fact1", "fact2"]
    # a side the plan left out keeps its place after the stated ones, however
    # far into its order the plan put them
    ranks = _stated_order(
        {"fact1": 7}, JoinFacts(sides=facts, pair_keys=_CUST_PAIRS), frozenset()
    )
    assert _order(ranks) == ["customers", "fact1", "fact2"]


def test_sides_partial_on_a_pairing_key_come_after_the_rest_in_the_plans_order():
    plan = {"fact2": 0, "fact1": 1, "customers": 2}
    pairs = {frozenset({"fact1", "fact2"}): frozenset({"cust"})}
    sides = _bound({"fact1": {"cust"}, "fact2": {"cust"}})
    shape = _shape_terms(plan, sides, pairs, frozenset())
    assert sorted(shape, key=shape.__getitem__) == ["customers", "fact2", "fact1"]


def test_a_lookup_comes_with_the_rows_it_hangs_off():
    facts = _keyed(
        {
            ("sales", "returns"): {"ticket"},
            ("sales", "item"): {"item"},
            ("returns", "reason"): {"reason"},
        }
    )
    plan = {"sales": 0, "returns": 1, "item": 2, "reason": 3}
    ranks = _stated_order(plan, facts, frozenset())
    assert _order(ranks) == ["sales", "returns", "item", "reason"]
    ranks = _stated_order({**plan, "sales": 1, "returns": 0}, facts, frozenset())
    assert _order(ranks) == ["returns", "sales", "reason", "item"]


def test_an_arm_of_a_union_anchors_only_on_a_declared_key():
    plan = {"fact1": 0, "fact2": 1, "customers": 2}
    arms = _bound({"fact1": {"cust"}}, arm=True)
    member = _bound({"fact2": {"cust"}})
    sides = {**member, "fact1": arms["fact1"]}
    ranks = _stated_order(
        plan, JoinFacts(sides=sides, pair_keys=_CUST_PAIRS), frozenset()
    )
    assert _order(ranks) == ["customers", "fact1", "fact2"]
    ranks = _stated_order(
        plan, JoinFacts(sides=sides, pair_keys=_CUST_PAIRS), frozenset({"cust"})
    )
    assert _order(ranks) == ["customers", "fact1", "fact2"]
    assert ranks["fact2"] - ranks["fact1"] == 1


def test_the_walk_reads_the_stated_order_and_no_fact():
    facts = _star_facts({"ds~a": 2, "ds~b": 1, "ds~c": 0})
    first = SideFacts(
        columns=frozenset({"c~k"}), hosts=True, held_spans=frozenset({"c~k"})
    )
    facts = replace(
        facts,
        sides={**facts.sides, "ds~a": first},
        authored_join_keys=frozenset({"c~k"}),
    )
    joins = resolve_join_order_v2(facts)
    assert [joins[0].lefts.pop(), *(j.right for j in joins)] == [
        "ds~c",
        "ds~b",
        "ds~a",
    ]


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
    facts = _hosted_star(ranks)
    stated = _stated_order(ranks, facts, frozenset())
    joins = resolve_join_order_v2(replace(facts, side_ranks=stated))
    assert joins[0].lefts.pop() == seed


def test_a_stated_seed_starts_the_walk_whatever_the_order_says():
    facts = replace(_star_facts({"ds~a": 0, "ds~b": 1, "ds~c": 2}), seed="ds~c")
    joins = resolve_join_order_v2(facts)
    assert [joins[0].lefts.pop(), *(j.right for j in joins)] == [
        "ds~c",
        "ds~a",
        "ds~b",
    ]


@pytest.mark.parametrize(
    "forced,ranks,seed",
    [
        (JoinType.LEFT_OUTER, {"hub": 0, "spoke0": 1, "spoke1": 1}, "hub"),
        (JoinType.LEFT_OUTER, {"a": 0, "b": 0}, None),
        (JoinType.FULL, {"hub": 0, "spoke0": 1}, None),
        (None, {"hub": 0, "spoke0": 1}, None),
        (JoinType.LEFT_OUTER, {}, None),
    ],
)
def test_a_forced_left_seeds_the_side_the_plan_states_first(forced, ranks, seed):
    assert _forced_seed(forced, ranks) == seed


def _keyed(pairs: dict[tuple[str, str], set[str]], **sides: SideFacts) -> JoinFacts:
    names = sorted({side for pair in pairs for side in pair})
    return JoinFacts(
        sides={side: sides.get(side, SideFacts()) for side in names},
        pair_keys={frozenset(pair): frozenset(keys) for pair, keys in pairs.items()},
    )


def test_an_authored_key_states_its_holders_before_the_plans_order():
    facts = replace(
        _keyed(
            {
                ("fact_a", "fact_b"): {"sku"},
                ("fact_a", "dim_a"): {"a_sk"},
                ("fact_b", "dim_b"): {"b_sk"},
                ("dim_a", "dim_b"): {"cust"},
            }
        ),
        authored_join_keys=frozenset({"cust"}),
    )
    plan = {"fact_a": 0, "fact_b": 1, "dim_a": 2, "dim_b": 3}
    ranks = _stated_order(plan, facts, frozenset())
    assert _order(ranks) == ["dim_a", "dim_b", "fact_a", "fact_b"]


def test_of_two_authored_keys_the_one_pairing_fewer_sides_states_its_holders_first():
    facts = replace(
        _keyed(
            {
                ("returns", "sales"): {"cust", "ticket"},
                ("catalog", "sales"): {"cust"},
                ("catalog", "returns"): {"cust"},
            }
        ),
        authored_join_keys=frozenset({"cust", "ticket"}),
    )
    ranks = _stated_order({"catalog": 0, "returns": 1, "sales": 2}, facts, frozenset())
    assert _order(ranks) == ["returns", "sales", "catalog"]


def test_region_free_holders_of_a_value_null_key_come_before_the_region():
    facts = _keyed(
        {
            ("region", "by_span"): {"span"},
            ("by_span", "by_value"): {"value"},
            ("region", "by_value"): {"value"},
        },
        region=SideFacts(held_spans=frozenset({"span"})),
        by_span=SideFacts(value_nullables=frozenset({"value"})),
    )
    ranks = _stated_order(
        {"region": 0, "by_span": 1, "by_value": 2}, facts, frozenset()
    )
    assert _order(ranks) == ["by_span", "by_value", "region"]


def test_a_side_holding_several_regions_comes_after_they_are_stitched():
    both = SideFacts(held_spans=frozenset({"cust", "bucket"}))
    facts = _keyed(
        {
            ("customers", "orders"): {"cust"},
            ("buckets", "orders"): {"bucket"},
            ("by_bucket", "buckets"): {"bucket"},
            ("by_bucket", "orders"): {"bucket"},
        },
        customers=SideFacts(held_spans=frozenset({"cust"})),
        buckets=SideFacts(held_spans=frozenset({"bucket"})),
        by_bucket=both,
    )
    ranks = _stated_order({"by_bucket": 0}, facts, frozenset())
    assert _order(ranks)[:2] == ["customers", "orders"]
    assert ranks["by_bucket"] > ranks["customers"]


def test_a_flipped_shape_keeps_the_row_terms(monkeypatch):
    facts = replace(
        _keyed({("a", "b"): {"k"}, ("b", "c"): {"k"}, ("a", "c"): {"k", "x"}}),
        authored_join_keys=frozenset({"x"}),
    )
    plan = {"a": 0, "b": 1, "c": 2}
    assert _order(_stated_order(plan, facts, frozenset())) == ["a", "c", "b"]
    flip_walk_spelling(monkeypatch, "shape_flipped")
    assert _order(_stated_order(plan, facts, frozenset())) == ["c", "a", "b"]


def _stitch_facts(count_rank: int) -> JoinFacts:
    return JoinFacts(
        sides={
            "customers": SideFacts(held_spans=frozenset({"cust"})),
            "orders": SideFacts(),
            "count": SideFacts(held_spans=frozenset({"cust", "bucket"})),
        },
        pair_keys={
            frozenset({"customers", "orders"}): frozenset({"cust"}),
            frozenset({"orders", "count"}): frozenset({"bucket"}),
        },
        side_ranks={"count": count_rank, "orders": 1, "customers": 2},
    )


@pytest.mark.parametrize("count_rank", [0, 3])
def test_a_side_holding_a_span_it_pairs_on_with_no_one_joins_after_its_stitch(
    count_rank: int,
):
    facts = _stitch_facts(count_rank)
    assert facts.stitches == {"count": frozenset({"customers", "orders"})}
    walked = [j.right for j in resolve_join_order_v2(facts)]
    assert walked[-1] == "count"


def _returns_facts(**returns) -> JoinFacts:
    keys = frozenset({"c~order", "c~item"})
    order = frozenset({"c~order"})
    return JoinFacts(
        sides={
            "ds~lines": SideFacts(columns=keys),
            "ds~returns": SideFacts(columns=keys, **returns),
            "ds~orders": SideFacts(columns=order),
        },
        pair_keys={
            frozenset({"ds~lines", "ds~returns"}): keys,
            frozenset({"ds~lines", "ds~orders"}): order,
            frozenset({"ds~returns", "ds~orders"}): order,
        },
        side_ranks={"ds~returns": 0, "ds~orders": 1, "ds~lines": 2},
    )


def test_a_subset_side_meets_a_coarser_one_only_through_its_anchor():
    facts = _returns_facts(partials=frozenset({"c~order", "c~item"}))
    assert facts.transits == {
        frozenset({"ds~returns", "ds~orders"}): frozenset({"ds~lines"})
    }
    joins = resolve_join_order_v2(facts)
    assert [j.right for j in joins] == ["ds~lines", "ds~orders"]
    assert set(joins[1].keys) == {"ds~lines", "ds~returns"}


@pytest.mark.parametrize(
    "returns",
    [
        {},
        {
            "partials": frozenset({"c~order", "c~item"}),
            "nullables": frozenset({"c~order"}),
        },
    ],
)
def test_a_complete_or_nullable_side_pairs_a_coarser_one_directly(returns: dict):
    facts = _returns_facts(**returns)
    assert not facts.transits
    assert resolve_join_order_v2(facts)[0].right == "ds~orders"
