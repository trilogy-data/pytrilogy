"""Numeric-cell canonicalization used by result-set scoring.

Guards the tolerance that lets a float32 accumulation (e.g. a `0::float` money
placeholder) still match the reference's exact Decimal arithmetic, WITHOUT
merging genuinely-distinct integer row counts.
"""

from __future__ import annotations

from decimal import Decimal

from evals.common.scoring import _results_equal, _round_cell, _sig_round


def test_sig_round_relative_precision():
    assert _sig_round(112458735.49, 6) == _sig_round(112458734.70, 6)
    assert _sig_round(163753.9375, 6) == _sig_round(163753.94, 6)
    assert _sig_round(0.0, 6) == 0.0


def test_float32_drift_matches_reference():
    # q05: float32-accumulated sum vs exact Decimal reference must compare equal.
    assert _round_cell(112458735.48859596) == _round_cell(Decimal("112458734.70"))
    assert _round_cell(163753.9375) == _round_cell(Decimal("163753.94"))


def test_integer_counts_stay_exact():
    # Distinct row counts must NOT be merged by the tolerance.
    assert _round_cell(45689) != _round_cell(45690)
    assert _round_cell(107) == _round_cell(Decimal(107))
    assert _round_cell(2521) != _round_cell(2522)


def test_genuinely_different_money_still_differs():
    # A real ~1% difference is far coarser than 6 sig figs — must stay distinct.
    assert _round_cell(163753.94) != _round_cell(165000.00)


def test_non_numeric_and_bool_untouched():
    assert _round_cell(True) is True
    assert _round_cell(False) is False
    assert _round_cell("store") == "store"
    assert _round_cell(None) is None


def test_non_finite_preserved():
    assert _round_cell(float("nan")) != _round_cell(float("nan"))  # nan != nan
    assert _round_cell(float("inf")) == float("inf")


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


def test_tolerant_comparison_keeps_integer_counts_exact():
    assert not _results_equal([[45689]], [[45690]])


def test_tolerant_comparison_rejects_real_numeric_difference():
    assert not _results_equal([["store", 163753.94]], [["store", 165000.00]])
