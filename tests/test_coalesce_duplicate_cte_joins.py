"""Merging two copies of one logical CTE (``CTE.__add__``) must not keep two
joins to the same right CTE whose key-pair sets differ — CTE-level joins carry
no aliases, so the second join re-joins the same rows (row multiplication) and
renders a duplicate table reference (q11: FULL JOIN "abhorrent" twice, once
with a redundant measure column as a join key; 25GiB OOM at sf=1)."""

from types import SimpleNamespace

from trilogy.core.enums import JoinType, Modifier
from trilogy.core.models.execute import (
    Join,
    coalesce_duplicate_joins,
    coalesced_key_groups,
)


def _pair(cte_name: str, left: str, right: str) -> SimpleNamespace:
    return SimpleNamespace(
        node=SimpleNamespace(name=cte_name),
        left=SimpleNamespace(address=left),
        right=SimpleNamespace(address=right),
        modifiers=[],
    )


def _join(right: str, pairs, jointype=JoinType.FULL, left=None, modifiers=None):
    return Join(
        right=SimpleNamespace(name=right),  # type: ignore[arg-type]
        join_type=jointype,
        left=SimpleNamespace(name=left) if left else None,  # type: ignore[arg-type]
        pairs=pairs,
        modifiers=modifiers or [],
    )


def test_same_target_joins_merge_to_union_of_pairs():
    a = _join(
        "abhorrent",
        [
            _pair("sparkling", "w01.cid", "w02.cid"),
            _pair("sparkling", "w01.fname", "w01.fname"),
        ],
    )
    b = _join(
        "abhorrent",
        [
            _pair("sparkling", "w01.cid", "w02.cid"),
            _pair("sparkling", "w01.fname", "w01.fname"),
            _pair("sparkling", "w01.w_rev", "w01.w_rev"),
        ],
        modifiers=[Modifier.NULLABLE],
    )
    merged = coalesce_duplicate_joins([a, b])
    assert len(merged) == 1
    survivor = merged[0]
    assert isinstance(survivor, Join)
    assert survivor.pairs is not None
    pair_keys = {(p.node.name, p.left.address, p.right.address) for p in survivor.pairs}
    assert pair_keys == {
        ("sparkling", "w01.cid", "w02.cid"),
        ("sparkling", "w01.fname", "w01.fname"),
        ("sparkling", "w01.w_rev", "w01.w_rev"),
    }
    assert Modifier.NULLABLE in survivor.modifiers


def test_distinct_targets_and_types_are_untouched():
    a = _join("x", [_pair("l", "k", "k")])
    b = _join("y", [_pair("l", "k", "k")])
    c = _join("x", [_pair("l", "k", "k")], jointype=JoinType.INNER)
    merged = coalesce_duplicate_joins([a, b, c])
    assert merged == [a, b, c]


def test_keyless_and_conditioned_joins_pass_through():
    keyless = _join("x", None)
    keyless2 = _join("x", None)
    conditioned = _join("x", [_pair("l", "k", "k")])
    conditioned.condition = object()  # type: ignore[assignment]
    merged = coalesce_duplicate_joins([keyless, keyless2, conditioned])
    assert merged == [keyless, keyless2, conditioned]


def test_merged_join_keeps_either_copys_padding_guard():
    guard = (("o.order_id is not null",),)
    a = _join("t", [_pair("l", "k", "k")])
    b = _join("t", [_pair("l", "k", "k")])
    b.guard = guard  # type: ignore[assignment]
    merged = coalesce_duplicate_joins([a, b, _join("t", [_pair("l", "k", "k")])])
    assert merged == [a]
    assert a.guard == guard


def test_an_outer_join_merges_into_its_mirror():
    right_outer = _join(
        "customers", [_pair("orders", "cid", "cid")], jointype=JoinType.RIGHT_OUTER
    )
    left_outer = _join(
        "orders", [_pair("customers", "cid", "cid")], jointype=JoinType.LEFT_OUTER
    )
    assert coalesce_duplicate_joins([right_outer, left_outer]) == [right_outer]


def test_a_full_join_merges_into_its_mirror():
    ss_first = _join("sr", [_pair("ss", "k", "k")], jointype=JoinType.FULL)
    sr_first = _join("ss", [_pair("sr", "k", "k")], jointype=JoinType.FULL)
    assert coalesce_duplicate_joins([ss_first, sr_first]) == [ss_first]


def test_outer_joins_padding_different_sides_stay_apart():
    pads_orders = _join(
        "orders", [_pair("customers", "cid", "cid")], jointype=JoinType.LEFT_OUTER
    )
    pads_customers = _join(
        "customers", [_pair("orders", "cid", "cid")], jointype=JoinType.LEFT_OUTER
    )
    merged = coalesce_duplicate_joins([pads_orders, pads_customers])
    assert merged == [pads_orders, pads_customers]


def test_a_mirror_adds_its_extra_pairs_and_intersects_shared_modifiers():
    shared = _pair("orders", "cid", "cid")
    shared.modifiers = [Modifier.NULLABLE]
    right_outer = _join("customers", [shared], jointype=JoinType.RIGHT_OUTER)
    left_outer = _join(
        "orders",
        [_pair("customers", "cid", "cid"), _pair("customers", "reg", "reg")],
        jointype=JoinType.LEFT_OUTER,
    )
    assert coalesce_duplicate_joins([right_outer, left_outer]) == [right_outer]
    assert shared.modifiers == []
    added = right_outer.pairs[-1]
    assert (added.node.name, added.left.address, added.right.address) == (
        "orders",
        "reg",
        "reg",
    )


def test_inner_joins_onto_one_right_merge_their_pairs():
    first = _join("x", [_pair("l", "k", "k")], jointype=JoinType.INNER)
    second = _join(
        "x",
        [_pair("l", "k", "k"), _pair("l", "j", "j")],
        jointype=JoinType.INNER,
    )
    assert coalesce_duplicate_joins([first, second]) == [first]
    assert {p.left.address for p in first.pairs} == {"k", "j"}


def test_a_pair_that_orients_against_neither_right_keeps_both_joins():
    first = _join(
        "c", [_pair("a", "k", "k"), _pair("b", "k", "k")], jointype=JoinType.INNER
    )
    second = _join(
        "a", [_pair("b", "k", "k"), _pair("c", "k", "k")], jointype=JoinType.INNER
    )
    assert coalesce_duplicate_joins([first, second]) == [first, second]


def test_one_key_read_off_two_sides_is_one_group_however_spelled():
    catalog = _pair("catalog", "ck", "sk")
    web = _pair("web", "wk", "sk")
    own = _pair("web", "region", "region")
    second = _pair("web", "wk2", "region")
    groups = coalesced_key_groups([catalog, web, own, second], lambda p: p.node.name)
    assert groups == [
        [catalog, web],
        [own],
        [second],
    ]
