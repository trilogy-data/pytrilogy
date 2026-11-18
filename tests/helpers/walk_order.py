"""The join walk with its shape order reversed.

The walk joins a merge's sides in one stated order (`_stated_order`): shape
(each key's and the plan's), then the side's name. Neither is a fact about
rows, so a plan walked the other way round on either must return the same
rows. `flip_walk_spelling` reverses one of them.
"""

from __future__ import annotations

from functools import total_ordering
from typing import Any

import pytest

from trilogy.core.processing import join_resolution as jr

_shape = jr._shape_terms
_rank = jr.JoinFacts.rank


@total_ordering
class _Rev:
    """Orders the reverse of the value it wraps."""

    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Rev) and self.value == other.value

    def __lt__(self, other: _Rev) -> bool:
        return self.value > other.value

    def __hash__(self) -> int:
        return hash(self.value)


def flipped_shape(ranks, sides, pair_keys, declared):
    return {
        node: _Rev(term)
        for node, term in _shape(ranks, sides, pair_keys, declared).items()
    }


def flipped_rank(facts, node):
    rank, name = _rank(facts, node)
    return (rank, _Rev(name))


_keys = jr._key_terms


def flipped_keys(facts, holders):
    return [(_Rev(shape), held) for shape, held in _keys(facts, holders)]


# what each flip replaces: (owner, attribute, replacement)
SHAPE_FLIPS = [
    (jr, "_shape_terms", flipped_shape),
    (jr, "_key_terms", flipped_keys),
]
NAME_FLIPS = [(jr.JoinFacts, "rank", flipped_rank)]
FLIPS = {
    "walked": [],
    "shape_flipped": SHAPE_FLIPS,
    "names_flipped": NAME_FLIPS,
}

# as walked, and with each reversed
WALKS = ["walked", "shape_flipped", "names_flipped"]


def flip_walk_spelling(monkeypatch: pytest.MonkeyPatch, walk: str) -> None:
    for owner, name, replacement in FLIPS[walk]:
        monkeypatch.setattr(owner, name, replacement)
