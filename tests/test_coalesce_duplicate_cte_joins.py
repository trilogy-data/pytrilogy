"""Merging two copies of one logical CTE (``CTE.__add__``) must not keep two
joins to the same right CTE whose key-pair sets differ — CTE-level joins carry
no aliases, so the second join re-joins the same rows (row multiplication) and
renders a duplicate table reference (q11: FULL JOIN "abhorrent" twice, once
with a redundant measure column as a join key; 25GiB OOM at sf=1)."""

from types import SimpleNamespace

from trilogy.core.enums import JoinType, Modifier
from trilogy.core.models.execute import Join, coalesce_duplicate_joins


def _pair(cte_name: str, left: str, right: str) -> SimpleNamespace:
    return SimpleNamespace(
        cte=SimpleNamespace(name=cte_name),
        left=SimpleNamespace(address=left),
        right=SimpleNamespace(address=right),
    )


def _join(right: str, pairs, jointype=JoinType.FULL, left=None, modifiers=None):
    return Join(
        right_cte=SimpleNamespace(name=right),  # type: ignore[arg-type]
        jointype=jointype,
        left_cte=SimpleNamespace(name=left) if left else None,  # type: ignore[arg-type]
        joinkey_pairs=pairs,
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
    assert survivor.joinkey_pairs is not None
    pair_keys = {
        (p.cte.name, p.left.address, p.right.address) for p in survivor.joinkey_pairs
    }
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
