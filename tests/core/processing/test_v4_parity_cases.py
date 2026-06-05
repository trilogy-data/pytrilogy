"""CI guard for the v4 discovery parity cases.

Promotes the `local_scripts/v4_evals` harness into pytest: each `cases/*.preql`
is generated + executed under both the v3 and v4 planners on DuckDB and the
result rows are compared as a column-sorted, float-rounded multiset. A v4 crash,
hang, or row diff is a correctness regression — this is what keeps the graph
invariants those repros pin from silently rotting.

The harness itself (case discovery, dual-planner run, row normalization) stays in
`run_parity.py` so the manual script and CI share one source of truth.
"""

import sys
from pathlib import Path

import pytest

_HARNESS_DIR = Path(__file__).resolve().parents[3] / "local_scripts" / "v4_evals"
if str(_HARNESS_DIR) not in sys.path:
    sys.path.insert(0, str(_HARNESS_DIR))

from run_parity import CASES_DIR, run_case  # noqa: E402

CASES = sorted(CASES_DIR.glob("*.preql"))


@pytest.mark.v4_parity
@pytest.mark.parametrize("case", CASES, ids=lambda p: p.stem)
def test_v4_parity(case: Path):
    r = run_case(case)
    if r["status"] == "v3_error":
        pytest.fail(f"{case.stem}: v3 oracle errored\n{r['v3_err']}")
    if r["status"] == "v4_error":
        pytest.fail(f"{case.stem}: v4 planner crashed\n{r['v4_err']}")
    assert r["status"] == "match", (
        f"{case.stem}: v3/v4 row mismatch (v3={r['v3_rows']} v4={r['v4_rows']})\n"
        f"only in v3: {sorted(set(r['_v3'] or []) - set(r['_v4'] or []))[:5]}\n"
        f"only in v4: {sorted(set(r['_v4'] or []) - set(r['_v3'] or []))[:5]}"
    )


def test_v4_parity_cases_discovered():
    """Guard the guard: a broken glob / moved cases dir would otherwise make the
    parametrized suite vacuously pass."""
    assert len(CASES) >= 16, f"expected the curated v4 parity cases, found {CASES}"
