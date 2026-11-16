"""The join walk with its order tie-breaks reversed.

Below the plan's facts the walk breaks ties on the order the plan states
(`PlannedPairs.ranked`): which key's holders it puts first, and which side.
That order is no fact about rows, so a plan walked the other way round must
return the same rows. `flip_walk_spelling` reverses it; every other term of
the rank and the scorer stays.
"""

from __future__ import annotations

from functools import total_ordering
from itertools import combinations
from typing import Any

import pytest

from trilogy.core.processing import join_resolution as jr

_rank = jr._pivot_rank
_score = jr._score_join_candidate
_keyless = jr._keyless_order


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


def flipped_rank(pivot, holders, facts):
    out = _rank(pivot, holders, facts)
    return (*out[:-2], _Rev(out[-2]), _Rev(out[-1]))


def flipped_edges(pivot_map, facts):
    edges = [
        (pivot, a, b)
        for pivot, holders in pivot_map.items()
        for a, b in combinations(holders, 2)
        if pivot.key in facts.paired(a, b)
    ]
    return sorted(
        edges,
        key=lambda e: (
            jr._pivot_rank(e[0], pivot_map[e[0]], facts),
            _Rev(facts.rank(e[1])),
            _Rev(facts.rank(e[2])),
        ),
    )


def flipped_score(x, **kwargs):
    base, rank = _score(x, **kwargs)
    return (base, _Rev(rank))


def flipped_keyless(facts, ds):
    none, rank = _keyless(facts, ds)
    return (none, _Rev(rank))


KEY_FLIPS = {"_pivot_rank": flipped_rank, "_ranked_edges": flipped_edges}
SIDE_FLIPS = {"_score_join_candidate": flipped_score, "_keyless_order": flipped_keyless}

# as walked, and with each spelling reversed
WALKS = ["walked", "keys_flipped", "sides_flipped"]


def flip_walk_spelling(monkeypatch: pytest.MonkeyPatch, walk: str) -> None:
    flips = {"walked": {}, "keys_flipped": KEY_FLIPS, "sides_flipped": SIDE_FLIPS}
    for name, replacement in flips[walk].items():
        monkeypatch.setattr(jr, name, replacement)
