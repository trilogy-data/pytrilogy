"""CI guard for the discovery correctness cases.

Promotes the `local_scripts/v4_evals` harness into pytest: each `cases/*.preql`
is generated + executed on DuckDB. A crash, hang, or render error is a
correctness regression — this is what keeps the graph invariants those repros
pin from silently rotting.

The harness itself (case discovery, run, row normalization) stays in
`run_parity.py` so the manual script and CI share one source of truth.
"""

import sys
from pathlib import Path

import pytest

_HARNESS_DIR = Path(__file__).resolve().parents[3] / "local_scripts" / "v4_evals"
if str(_HARNESS_DIR) not in sys.path:
    sys.path.insert(0, str(_HARNESS_DIR))

from run_parity import CASES_DIR, run_case

CASES = sorted(CASES_DIR.glob("*.preql"))


@pytest.mark.v4_parity
@pytest.mark.parametrize("case", CASES, ids=lambda p: p.stem)
def test_v4_parity(case: Path):
    r = run_case(case)
    assert r["status"] == "ok", f"{case.stem}: {r['error']}"


def test_v4_parity_cases_discovered():
    """Guard the guard: a broken glob / moved cases dir would otherwise make the
    parametrized suite vacuously pass."""
    assert len(CASES) >= 16, f"expected the curated discovery cases, found {CASES}"
