"""Result-row comparison semantics (shared trilogy/evals implementation).

Guards the tolerance that lets a float32 accumulation (e.g. a `0::float` money
placeholder) still match the reference's exact Decimal arithmetic, WITHOUT
merging genuinely-distinct integer row counts.
"""

from __future__ import annotations

from decimal import Decimal

from evals.common.scoring import _results_equal
from trilogy.core.enums import QueryComparison
from trilogy.core.validation.rows import (
    rows_equal,
    rows_equal_exact,
    rows_equal_ordered,
)


def test_float32_drift_matches_reference():
    # q05: float32-accumulated sum vs exact Decimal reference must compare equal.
    assert _results_equal([[112458735.48859596]], [[Decimal("112458734.70")]])
    assert _results_equal([[163753.9375]], [[Decimal("163753.94")]])


def test_whole_dollar_drift_matches_reference():
    # q66: the old asymmetric exact-integer carve-out false-failed money sums
    # whose reference landed on a whole dollar >= 1e6.
    assert _results_equal([[12492732.049]], [[Decimal("12492732.00")]])


def test_integer_counts_stay_exact():
    # Distinct small-integer row counts must NOT be merged by the tolerance.
    assert not _results_equal([[45689]], [[45690]])
    assert not _results_equal([[2521]], [[2522]])
    assert _results_equal([[107]], [[Decimal(107)]])


def test_genuinely_different_money_still_differs():
    # A real ~1% difference is far coarser than 6 sig figs — must stay distinct.
    assert not _results_equal([["store", 163753.94]], [["store", 165000.00]])


def test_bool_never_merges_with_numeric():
    assert not _results_equal([[True]], [[1]])
    assert _results_equal([[True]], [[True]])


def test_non_finite_compares_exactly():
    assert _results_equal([[float("inf")]], [[float("inf")]])
    assert not _results_equal([[float("inf")]], [[1.0]])


def test_tolerant_comparison_has_no_rounding_bucket_boundary():
    candidate = [["catalog", 82462.849999999991]]
    reference = [["catalog", Decimal("82462.85")]]
    assert _results_equal(candidate, reference)


def test_tolerant_comparison_ignores_row_and_column_order():
    candidate = [[1, "a", 10.000001], [2, "b", 20.000001]]
    reference = [[20.0, "b", 2], ["a", 10.0, 1]]
    assert _results_equal(candidate, reference)


def test_tolerant_comparison_preserves_multiset_cardinality():
    candidate = [["a", 1.01], ["a", 1.01]]
    reference = [["a", 1.01], ["b", 1.01]]
    assert not _results_equal(candidate, reference)


def test_tolerant_comparison_rejects_row_count_mismatch():
    assert not _results_equal([[1]], [[1], [1]])


def test_ordered_mode_enforces_row_order():
    assert rows_equal_ordered([(1, "a"), (2, "b")], [(1, "a"), (2, "b")])
    assert not rows_equal_ordered([(2, "b"), (1, "a")], [(1, "a"), (2, "b")])


def test_ordered_mode_keeps_numeric_tolerance():
    assert rows_equal_ordered([(112458735.49,)], [(Decimal("112458734.70"),)])


def test_exact_mode_rejects_drift_but_not_types():
    assert not rows_equal_exact([(12492732.0,)], [(12492732.049,)])
    assert rows_equal_exact([(1, "a")], [(Decimal(1), "a")])
    assert not rows_equal_exact([("a", 1)], [(1, "a")])  # positional columns


def test_rows_equal_dispatch():
    assert rows_equal([(1,)], [(1,)], QueryComparison.EXACT)
    assert rows_equal([(1,)], [(1,)], QueryComparison.ORDERED)
    assert rows_equal([(1,)], [(1,)], QueryComparison.TOLERANT)
    assert not rows_equal([(2, 1)], [(1, 2)], QueryComparison.EXACT)
    assert rows_equal([(2, 1)], [(1, 2)])  # tolerant sorts cells within a row
