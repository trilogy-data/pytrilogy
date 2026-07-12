"""Cross-suite summary of eval performance.

One row per full benchmark run: live results dirs (report.json) merged with the
longitudinal history db, so runs whose raw logs were cleaned up still show in
the trend. Repeat-harness runs are excluded — they re-run one question many
times and would skew a variant's trend.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path

from common import archive

from .suites import Suite

_TS_RE = re.compile(r"\d{8}-\d{6}")


def _norm_ts(*candidates: str | None) -> str:
    """First candidate that yields a sortable ``YYYYMMDD-HHMMSS`` stamp."""
    for c in candidates:
        if not c:
            continue
        m = _TS_RE.search(c)
        if m:
            return m.group(0)
        digits = re.sub(r"\D", "", c)
        if len(digits) >= 14:
            return f"{digits[:8]}-{digits[8:14]}"
    return ""


def _live_row(suite: Suite, run_dir: Path) -> dict | None:
    rp = run_dir / "report.json"
    if not rp.exists():
        return None
    try:
        report = json.loads(rp.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    meta = report.get("meta", {})
    total = len(report.get("queries", [])) or meta.get("num_queries") or 0
    prompt_tokens = sum(
        m.get("prompt_tokens") or 0 for m in report.get("per_query_metrics", [])
    )
    mtime_ts = time.strftime("%Y%m%d-%H%M%S", time.localtime(run_dir.stat().st_mtime))
    return {
        "suite": suite.key,
        "suite_label": suite.label,
        "variant": meta.get("category") or meta.get("mode") or "unknown",
        "run": run_dir.name,
        "model": meta.get("model"),
        "provider": meta.get("provider"),
        "passed": report.get("summary", {}).get("pass_count", 0),
        "total": total,
        "prompt_tokens": prompt_tokens or None,
        "ts": _norm_ts(meta.get("timestamp"), run_dir.name) or mtime_ts,
        "live": True,
    }


# HAVING MAX(rep) = 0 keeps full runs (always rep 0) and drops repeat-harness
# archives (reps 0..N of one question).
_ARCHIVED_SQL = """
SELECT suite, variant, run_name, MAX(model), MAX(provider),
       COUNT(*), SUM(passed), SUM(prompt_tokens),
       MAX(run_timestamp), MAX(archived_at)
FROM questions
GROUP BY suite, variant, run_name
HAVING MAX(rep) = 0
"""


def _archived_rows() -> list[dict]:
    if not archive.default_db_path().exists():
        return []
    conn = archive.connect()
    try:
        rows = conn.execute(_ARCHIVED_SQL).fetchall()
    finally:
        conn.close()
    out = []
    for suite, variant, run, model, provider, total, passed, ptok, ts, archived in rows:
        out.append(
            {
                "suite": suite,
                "suite_label": suite,
                "variant": variant or "unknown",
                "run": run,
                "model": model,
                "provider": provider,
                "passed": passed or 0,
                "total": total,
                "prompt_tokens": ptok,
                "ts": _norm_ts(ts, run, archived),
                "live": False,
            }
        )
    return out


def summary_payload(suites: dict[str, Suite]) -> dict:
    """All summary rows, newest first; the page groups by suite and variant."""
    rows: list[dict] = []
    for suite in suites.values():
        if not suite.results_dir.is_dir():
            continue
        for child in suite.results_dir.iterdir():
            if child.is_dir():
                row = _live_row(suite, child)
                if row:
                    rows.append(row)
    seen = {(r["suite"], r["run"]) for r in rows}
    for row in _archived_rows():
        if (row["suite"], row["run"]) in seen:
            continue
        known = suites.get(row["suite"])
        if known is not None:
            row["suite_label"] = known.label
        rows.append(row)
    rows.sort(key=lambda r: r["ts"], reverse=True)
    return {"rows": rows}
