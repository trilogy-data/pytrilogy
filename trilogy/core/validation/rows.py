"""Result-row comparison for validation surfaces.

Single source of truth for "do these two result sets match": used by the
``validate ... matches`` agent-validation loop and imported by the eval
harness (``evals/common/scoring.py``) so the two surfaces cannot diverge.

Semantics (``TOLERANT``, the default): row order and column order are both
ignored — rows are bucketed by their non-numeric cells, then numeric cells are
maximum-matched under ``isclose``. Numeric cells all compare with the same
relative tolerance, on both sides symmetrically. Small integers stay effectively
exact (two distinct integers below ~1e5 can never be within the relative
tolerance), while large whole-number sums tolerate float accumulation drift —
an asymmetric exact-integer carve-out in an earlier version false-failed
whole-dollar sums >= 1e6.
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Sequence
from decimal import Decimal

from trilogy.core.enums import QueryComparison

COMPARISON_SIG_FIGS = 6
COMPARISON_REL_TOL = 10 ** (1 - COMPARISON_SIG_FIGS)
COMPARISON_ABS_TOL = 1e-9

ExactCell = tuple[str, str]
ComparisonRow = tuple[tuple[ExactCell, ...], tuple[float, ...]]


def _comparison_cell(value: object) -> tuple[str, object]:
    """Split a cell into ("numeric", float) or ("exact", repr)."""
    if isinstance(value, bool):
        return ("exact", repr(value))
    if isinstance(value, Decimal):
        if not value.is_finite():
            return ("exact", repr(value))
        return ("numeric", float(value))
    if isinstance(value, int):
        return ("numeric", float(value))
    if isinstance(value, float):
        if not math.isfinite(value):
            return ("exact", repr(value))
        return ("numeric", value)
    return ("exact", repr(value))


def _comparison_row(row: Sequence) -> ComparisonRow:
    cells = [_comparison_cell(value) for value in row]
    exact = tuple(sorted((cell for cell in cells if cell[0] == "exact"), key=repr))
    numeric = tuple(sorted(cell[1] for cell in cells if cell[0] == "numeric"))  # type: ignore[type-var]
    return exact, numeric  # type: ignore[return-value]


def _numeric_rows_close(left: tuple[float, ...], right: tuple[float, ...]) -> bool:
    if len(left) != len(right):
        return False
    return all(
        math.isclose(a, b, rel_tol=COMPARISON_REL_TOL, abs_tol=COMPARISON_ABS_TOL)
        for a, b in zip(left, right)
    )


def _assign(
    candidate: list[tuple[float, ...]],
    reference: list[tuple[float, ...]],
    matched: dict[int, int],
    reference_idx: int,
    seen: set[int],
) -> bool:
    """Augmenting-path step: claim a candidate row for ``reference_idx``,
    displacing an earlier claim only if that one can be rehomed."""
    for candidate_idx, candidate_row in enumerate(candidate):
        if candidate_idx in seen or not _numeric_rows_close(
            candidate_row, reference[reference_idx]
        ):
            continue
        seen.add(candidate_idx)
        previous = matched.get(candidate_idx)
        if previous is None or _assign(candidate, reference, matched, previous, seen):
            matched[candidate_idx] = reference_idx
            return True
    return False


def _bucket_matches(
    candidate: list[tuple[float, ...]],
    reference: list[tuple[float, ...]],
) -> bool:
    """Maximum-match one exact-value bucket under tolerant numeric equality."""
    if len(candidate) != len(reference):
        return False
    matched: dict[int, int] = {}
    return all(
        _assign(candidate, reference, matched, idx, set())
        for idx in range(len(reference))
    )


def rows_equal_tolerant(candidate: list, reference: list) -> bool:
    """Compare unordered rows/columns: exact non-numerics, tolerant numerics.

    Independent significant-figure rounding is not suitable for equality: two
    nearly identical values can land on opposite sides of a rounding boundary.
    Bucket rows by their exact cells, then maximum-match numeric cells with
    ``isclose`` so multiset cardinality is still enforced.
    """
    if len(candidate) != len(reference):
        return False
    candidate_buckets: dict[tuple[ExactCell, ...], list[tuple[float, ...]]] = (
        defaultdict(list)
    )
    reference_buckets: dict[tuple[ExactCell, ...], list[tuple[float, ...]]] = (
        defaultdict(list)
    )
    for row in candidate:
        exact, numeric = _comparison_row(row)
        candidate_buckets[exact].append(numeric)
    for row in reference:
        exact, numeric = _comparison_row(row)
        reference_buckets[exact].append(numeric)
    if candidate_buckets.keys() != reference_buckets.keys():
        return False
    return all(
        _bucket_matches(rows, reference_buckets[exact])
        for exact, rows in candidate_buckets.items()
    )


def _exact_key(row: Sequence) -> str:
    # Decimal/int/float distinctions collapse to the numeric value; everything
    # else compares by repr. Column order is preserved.
    return repr(tuple(_comparison_cell(value) for value in row))


def rows_equal_exact(candidate: list, reference: list) -> bool:
    """Unordered rows, positional columns, strictly equal cell values."""
    return Counter(_exact_key(r) for r in candidate) == Counter(
        _exact_key(r) for r in reference
    )


def rows_equal_ordered(candidate: list, reference: list) -> bool:
    """Tolerant cell semantics with row order enforced."""
    if len(candidate) != len(reference):
        return False
    for cand_row, ref_row in zip(candidate, reference):
        cand_exact, cand_numeric = _comparison_row(cand_row)
        ref_exact, ref_numeric = _comparison_row(ref_row)
        if cand_exact != ref_exact:
            return False
        if not _numeric_rows_close(cand_numeric, ref_numeric):
            return False
    return True


def rows_equal(
    candidate: list,
    reference: list,
    comparison: QueryComparison = QueryComparison.TOLERANT,
) -> bool:
    if comparison is QueryComparison.EXACT:
        return rows_equal_exact(candidate, reference)
    if comparison is QueryComparison.ORDERED:
        return rows_equal_ordered(candidate, reference)
    return rows_equal_tolerant(candidate, reference)
