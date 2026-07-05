"""Numeric-cell canonicalization used by result-set scoring.

Guards the tolerance that lets a float32 accumulation (e.g. a `0::float` money
placeholder) still match the reference's exact Decimal arithmetic, WITHOUT
merging genuinely-distinct integer row counts.
"""

from __future__ import annotations

from decimal import Decimal

from evals.common.scoring import _round_cell, _sig_round


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
    assert _round_cell(107) == _round_cell(Decimal("107"))
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
