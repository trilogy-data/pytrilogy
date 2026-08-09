"""Discovery correctness cases.

Each case under `cases/*.preql` is a self-contained Trilogy program (inline
datasources / consts + a final SELECT) that pins a correctness fix. We generate
+ execute the final statement on an in-memory DuckDB; a crash, a hang, or a
render error is a regression.

Cases arrived here as v3-vs-v4 parity repros. With the legacy planner gone there
is no oracle to diff against, so what a case still guards is that the program
plans, renders and runs — the failure mode most of them originally caught.

    python local_scripts/v4_evals/run_parity.py            # all cases
    python local_scripts/v4_evals/run_parity.py filter_past_unnest
"""

from __future__ import annotations

import sys
import traceback
from decimal import Decimal
from pathlib import Path
from typing import Any

from trilogy import Dialects, Environment

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


def _run(text: str, working_path: Path) -> tuple[list[tuple] | None, str]:
    """Generate + execute the final statement. Returns (normalized_rows, error).
    Fresh executor each call so no History cache bleeds between cases."""
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


def run_case(path: Path) -> dict:
    rows, err = _run(path.read_text(), path.parent)
    return {
        "case": path.stem,
        "status": "error" if err else "ok",
        "rows": None if rows is None else len(rows),
        "error": err,
        "_rows": rows,
    }


def _err_line(tb: str) -> str:
    """Pick the most informative traceback line — the exception class/message,
    not a trailing 'Background on this error' URL note."""
    lines = [ln.strip() for ln in tb.strip().splitlines() if ln.strip()]
    for ln in reversed(lines):
        if ("Error" in ln or "Exception" in ln) and "sqlalche.me" not in ln:
            return ln
    return lines[-1] if lines else ""


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
        bad += 0 if r["status"] == "ok" else 1
        print(f"[{r['status']:>5}] {r['case']}  (rows={r['rows']})")
        if r["status"] == "error":
            print("  " + _err_line(r["error"]))
    print(f"\n{len(cases) - bad}/{len(cases)} ok")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
