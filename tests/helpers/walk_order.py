"""The join walk with its shape order reversed.

The walk joins a merge's sides in one stated order (`_stated_order`): what
the sides' rows are, then the plan's shape, then the side's name. The last
two are no fact about rows, so a plan walked the other way round on either
must return the same rows. `flip_walk_spelling` reverses one of them; the
row terms stay. Of the row terms, three are typing that must hold in either
direction too (`ROW_WALKS`), so a plan walked with one held constant must
return the same rows.
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


_rows = jr._key_row_terms

# row term -> its index in `RowTerm`
ROW_TERMS = {"authored": 0, "solid": 1, "held": 2, "holders": 3}


def held_row(*names: str):
    """`_key_row_terms` with the named terms held constant; `solid` drops
    the solid holders, which are a term of their own."""
    indexes = {ROW_TERMS[name] for name in names}

    def rows(facts, holders):
        return [
            (tuple(0 if i in indexes else t for i, t in enumerate(term)), held_by)
            for term, held_by in _rows(facts, holders)
            if term[1] or ROW_TERMS["solid"] not in indexes
        ]

    return rows


# what each flip replaces: (owner, attribute, replacement)
SHAPE_FLIPS = [(jr, "_shape_terms", flipped_shape)]
NAME_FLIPS = [(jr.JoinFacts, "rank", flipped_rank)]
FLIPS = {
    "walked": [],
    "shape_flipped": SHAPE_FLIPS,
    "names_flipped": NAME_FLIPS,
    **{
        f"{name}_held": [(jr, "_key_row_terms", held_row(name))]
        for name in ("solid", "held", "holders")
    },
}

# as walked, and with each reversed
WALKS = ["walked", "shape_flipped", "names_flipped"]
# as walked, and with each row term that is typing held constant; `authored`
# and `max_held` are the left-deep tree's: a stitch must come first
ROW_WALKS = ["walked", "solid_held", "held_held", "holders_held"]


def flip_walk_spelling(monkeypatch: pytest.MonkeyPatch, walk: str) -> None:
    for owner, name, replacement in FLIPS[walk]:
        monkeypatch.setattr(owner, name, replacement)
