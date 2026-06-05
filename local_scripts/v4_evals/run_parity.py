"""Generic v3-vs-v4 result-parity eval.

Each case under `cases/*.preql` is a self-contained Trilogy program (inline
datasources / consts + a final SELECT). For each case we generate + execute the
final statement twice on an in-memory DuckDB — once with the v3 planner, once
with v4 (CONFIG.use_v4_discovery) — and compare result rows as a column-sorted,
float-rounded multiset. v4 crashing, hanging, or returning different rows is a
correctness regression.

This is the home for correctness cases surfaced by the V4 suite sweep: a case
here pins a v3/v4 behavioural difference so we can drive it to parity. Cases
whose SQL merely differs in shape (same rows) PASS here by design — those are
"structure" changes, inventoried separately, not parity bugs.

    python local_scripts/v4_evals/run_parity.py            # all cases
    python local_scripts/v4_evals/run_parity.py filter_past_unnest
"""

from __future__ import annotations

import sys
import traceback
from collections import Counter
from decimal import Decimal
from pathlib import Path
from typing import Any

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG

CASES_DIR = Path(__file__).resolve().parent / "cases"


def _round(v: Any) -> Any:
    if isinstance(v, (float, Decimal)):
        return round(v, 8)
    return v


def _normalize(columns: list[str], rows: list[tuple]) -> list[tuple]:
    if not columns:
        return [tuple(_round(v) for v in r) for r in rows]
    order = sorted(range(len(columns)), key=lambda i: columns[i])
    return [tuple(_round(row[i]) for i in order) for row in rows]


def _run(text: str, working_path: Path, v4: bool) -> tuple[list[tuple] | None, str]:
    """Generate + execute the final statement under the selected planner.
    Returns (normalized_rows, error). Fresh executor each call so the History
    cache never bleeds v3 nodes into the v4 run or vice-versa."""
    CONFIG.use_v4_discovery = v4
    try:
        env = Environment(working_path=working_path)
        ex = Dialects.DUCK_DB.default_executor(environment=env)
        sql = ex.generate_sql(text)[-1]
        cur = ex.execute_raw_sql(sql)
        columns = list(cur.keys())
        rows = [tuple(r) for r in cur.fetchall()]
        return _normalize(columns, rows), ""
    except Exception:
        return None, traceback.format_exc()
    finally:
        CONFIG.use_v4_discovery = False


def run_case(path: Path) -> dict:
    text = path.read_text()
    v3_rows, v3_err = _run(text, path.parent, v4=False)
    v4_rows, v4_err = _run(text, path.parent, v4=True)
    if v3_err:
        status = "v3_error"
    elif v4_err:
        status = "v4_error"
    elif Counter(v3_rows or []) == Counter(v4_rows or []):
        status = "match"
    else:
        status = "mismatch"
    return {
        "case": path.stem,
        "status": status,
        "v3_rows": None if v3_rows is None else len(v3_rows),
        "v4_rows": None if v4_rows is None else len(v4_rows),
        "v3_err": v3_err,
        "v4_err": v4_err,
        "_v3": v3_rows,
        "_v4": v4_rows,
    }


def _err_line(tb: str) -> str:
    """Pick the most informative traceback line — the exception class/message,
    not a trailing 'Background on this error' URL note."""
    lines = [ln.strip() for ln in tb.strip().splitlines() if ln.strip()]
    for ln in reversed(lines):
        if ("Error" in ln or "Exception" in ln) and "sqlalche.me" not in ln:
            return ln
    return lines[-1] if lines else ""


def _diff(v3: list[tuple], v4: list[tuple]) -> str:
    c3, c4 = Counter(v3), Counter(v4)
    only3 = list((c3 - c4).items())[:5]
    only4 = list((c4 - c3).items())[:5]
    out = []
    if only3:
        out.append("  only in v3: " + ", ".join(f"{n}x{r}" for r, n in only3))
    if only4:
        out.append("  only in v4: " + ", ".join(f"{n}x{r}" for r, n in only4))
    return "\n".join(out)


def main(argv: list[str]) -> int:
    cases = sorted(CASES_DIR.glob("*.preql"))
    if argv:
        want = set(argv)
        cases = [c for c in cases if c.stem in want]
    if not cases:
        print(f"no cases found in {CASES_DIR}")
        return 1
    bad = 0
    for path in cases:
        r = run_case(path)
        ok = r["status"] == "match"
        bad += 0 if ok else 1
        print(f"[{r['status']:>9}] {r['case']}  (v3={r['v3_rows']} v4={r['v4_rows']})")
        if r["status"] == "v4_error":
            print("  v4: " + _err_line(r["v4_err"]))
        elif r["status"] == "mismatch":
            print(_diff(r["_v3"], r["_v4"]))
        elif r["status"] == "v3_error":
            print("  v3: " + _err_line(r["v3_err"]))
    print(f"\n{len(cases) - bad}/{len(cases)} parity")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
