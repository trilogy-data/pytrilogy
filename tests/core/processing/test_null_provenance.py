"""`ProvenanceMemo` answers each null-provenance question once per source;
`plan_scope` shares one memo across a plan and nothing outside it."""

from tests.core.processing.test_join_padding_provenance import _concept, _qds
from trilogy.core.enums import JoinType
from trilogy.core.models.execute import (
    ConceptPair,
    Join,
    QueryDatasource,
)
from trilogy.core.processing import null_provenance
from trilogy.core.processing.null_provenance import (
    ProvenanceMemo,
    active_memo,
    plan_scope,
)

KEY = "local.k"
VALUE = "local.v"


def _padded_merge() -> QueryDatasource:
    """`left LEFT JOIN right` on `KEY`, which `right` carries nullable: the
    right side's columns are null-extended, its key NULLs are values."""
    left = _qds([KEY], [], grain={KEY})
    right = _qds([KEY, VALUE], [KEY], grain={VALUE})
    merged = _qds([KEY, VALUE], [KEY, VALUE], parents=[left, right])
    merged.source_map = {KEY: {left, right}, VALUE: {right}}
    merged.joins = [
        Join(
            left=left,
            right=right,
            join_type=JoinType.LEFT_OUTER,
            pairs=[ConceptPair(left=_concept(KEY), right=_concept(KEY), node=left)],
        )
    ]
    return merged


def test_active_memo_is_the_plans_inside_the_scope_and_fresh_outside():
    before = active_memo()
    with plan_scope() as memo:
        assert active_memo() is memo
        assert active_memo() is memo
    assert active_memo() is not memo
    assert active_memo() is not before


def test_values_are_answered_once_per_source_and_concept():
    merged = _padded_merge()
    memo = ProvenanceMemo()
    calls: list[str] = []
    original = null_provenance.nulls_are_values

    def counting(concept, side, _seen=frozenset()):
        if side is merged:
            calls.append(concept.address)
        return original(concept, side, _seen)

    null_provenance.nulls_are_values = counting
    try:
        first = memo.of(merged).values(_concept(VALUE))
        again = memo.of(merged).values(_concept(VALUE))
    finally:
        null_provenance.nulls_are_values = original
    assert first == again
    assert calls == [VALUE]


def test_memo_holds_its_sources_so_ids_stay_unique():
    memo = ProvenanceMemo()
    merged = _padded_merge()
    assert memo.of(merged).extent
    assert memo.sources[id(merged)] is merged


def test_extent_padding_is_read_through_the_memo():
    merged = _padded_merge()
    memo = ProvenanceMemo()
    provenance = memo.of(merged)
    assert VALUE in provenance.extent
    assert memo.extent[id(merged)] == provenance.extent
