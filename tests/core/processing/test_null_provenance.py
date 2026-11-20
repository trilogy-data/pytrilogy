"""A source owns its null provenance: answered once from its parents'
answers, and restamped when one of its fields is written."""

from tests.core.processing.test_join_padding_provenance import _concept, _qds
from trilogy.core.enums import JoinType
from trilogy.core.models.execute import (
    ConceptPair,
    Join,
    QueryDatasource,
)
from trilogy.core.processing import null_provenance
from trilogy.core.processing.null_provenance import provenance

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


def test_a_source_keeps_one_stamp():
    merged = _padded_merge()
    assert provenance(merged) is provenance(merged)


def test_values_are_answered_once_per_source_and_concept():
    merged = _padded_merge()
    calls: list[str] = []
    original = null_provenance.nulls_are_values

    def counting(concept, side, _seen=frozenset()):
        if side is merged:
            calls.append(concept.address)
        return original(concept, side, _seen)

    null_provenance.nulls_are_values = counting
    try:
        first = provenance(merged).values(_concept(VALUE))
        again = provenance(merged).values(_concept(VALUE))
    finally:
        null_provenance.nulls_are_values = original
    assert first == again
    assert calls == [VALUE]


def test_extent_padding_is_the_sources_own():
    merged = _padded_merge()
    stamp = provenance(merged)
    assert VALUE in stamp.extent
    assert stamp.extent is provenance(merged).extent


def test_writing_a_field_restamps_the_source():
    merged = _padded_merge()
    assert VALUE in provenance(merged).extent
    merged.joins = []
    assert VALUE not in provenance(merged).extent
