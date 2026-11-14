"""`formal/Trilogy/SelectGrain.lean` (`lookup_preserved_passes_padding`): a
lookup keyed off another lookup keeps no row of its own."""

from types import SimpleNamespace

import pytest

from trilogy.core.enums import JoinType
from trilogy.core.models.execute import ConceptPair, Join
from trilogy.core.processing.nodes.merge_node import _cap_lookup_joins


def _join(right: str, left: str, join_type: JoinType) -> Join:
    node = SimpleNamespace(identifier=left)
    pair = ConceptPair(left=None, right=None, node=node)  # type: ignore[arg-type]
    return Join(
        right=SimpleNamespace(identifier=right),  # type: ignore[arg-type]
        join_type=join_type,
        left=node,  # type: ignore[arg-type]
        pairs=[pair],
    )


@pytest.mark.parametrize(
    "left,given,expected",
    [
        ("lookup_a", JoinType.RIGHT_OUTER, JoinType.INNER),
        ("lookup_a", JoinType.FULL, JoinType.LEFT_OUTER),
        ("lookup_a", JoinType.LEFT_OUTER, JoinType.LEFT_OUTER),
        ("rows", JoinType.FULL, JoinType.FULL),
    ],
)
def test_lookup_off_lookup_keeps_no_row(left, given, expected):
    join = _join("lookup_b", left, given)
    _cap_lookup_joins([join], {"lookup_a", "lookup_b"})
    assert join.join_type == expected


def test_non_lookup_untouched():
    join = _join("rows", "lookup_a", JoinType.RIGHT_OUTER)
    _cap_lookup_joins([join], {"lookup_a"})
    assert join.join_type == JoinType.RIGHT_OUTER
