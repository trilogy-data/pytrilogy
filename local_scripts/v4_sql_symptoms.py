"""Mechanical symptom detector over rendered SQL (v3 and v4 side by side).

Counts the shape defects the v4 shape audit tracks, so "additional optimization
room" is a measured number per symptom rather than an impression:

- passthrough  : a CTE that is a bare projection of ONE source - no WHERE, no
                 GROUP BY, no join, no aggregate. Should have folded.
- dedup_group  : a CTE whose GROUP BY covers every projected column (a pure
                 dedup, no aggregate) - the sibling-bucket split.
- split_agg    : an aggregate CTE whose only FROM is another CTE that is itself
                 a plain scan - the aggregate never fused with its scan.
- repeat_scan  : the same physical table scanned in two or more CTEs.

Usage: python local_scripts/v4_sql_symptoms.py [sqldir]
"""

from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path

CTE_SPLIT = re.compile(r"^(\w+) as \(\s*$", re.MULTILINE)
TABLE_RE = re.compile(r'FROM\s+"[\w.]+"\."(\w+)"|JOIN\s+"[\w.]+"\."(\w+)"')
AGG_RE = re.compile(r"\b(sum|count|min|max|avg|array_agg)\s*\(", re.IGNORECASE)
WINDOW_RE = re.compile(r"\bover\s*\(", re.IGNORECASE)


CTE_HEAD = re.compile(r"\n(\w+) as \(\n")


def _close_paren(text: str, start: int) -> int:
    """Index of the paren closing the one just before `start`, skipping quoted
    literals. Depth-tracked: a `rfind(')')` closes the last CTE at whatever
    paren happens to come last in the file, so a trailing `LIMIT (100)` used to
    swallow the entire final SELECT into the last CTE's body."""
    depth = 1
    cursor = start
    while cursor < len(text) and depth:
        char = text[cursor]
        if char in "'\"":
            cursor = text.find(char, cursor + 1)
            if cursor < 0:
                return len(text)
        else:
            depth += (char == "(") - (char == ")")
        cursor += 1
    return cursor - 1


def split_ctes(sql: str) -> list[tuple[str, str]]:
    """(name, body) per CTE, plus ('__final__', body) for the trailing select."""
    text = "\n" + sql
    matches = list(CTE_HEAD.finditer(text))
    if not matches:
        return [("__final__", sql)]
    out = [
        (match.group(1), text[match.end() : _close_paren(text, match.end())])
        for match in matches
    ]
    out.append(("__final__", text[_close_paren(text, matches[-1].end()) + 1 :]))
    head = text[: matches[0].start()]
    if head.strip() and not head.strip().upper().startswith("WITH"):
        out.append(("__head__", head))
    return out


def classify(name: str, body: str) -> set[str]:
    found: set[str] = set()
    upper = body.upper()
    has_join = " JOIN " in upper
    has_where = "\nWHERE" in upper
    has_group = "\nGROUP BY" in upper
    has_agg = bool(AGG_RE.search(body))
    sources = [t or u for t, u in TABLE_RE.findall(body)]
    # A bare projection off another CTE (no physical table) that does nothing:
    # a rename-only layer the fold contract should have removed.
    has_window = bool(WINDOW_RE.search(body))
    if not (has_join or has_where or has_group or has_agg or has_window or sources):
        found.add("passthrough")
    if has_group and not has_agg:
        found.add("dedup_group")
    if has_agg and has_group and not has_join and not sources:
        found.add("split_agg")
    return found


def analyze(sql: str) -> Counter:
    stats: Counter = Counter()
    tables: Counter = Counter()
    for name, body in split_ctes(sql):
        if name in ("__head__", "__final__"):
            continue
        for symptom in classify(name, body):
            stats[symptom] += 1
        for table in {t or u for t, u in TABLE_RE.findall(body)}:
            tables[table] += 1
    stats["repeat_scan"] = sum(1 for count in tables.values() if count > 1)
    return stats


def main() -> None:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else "local_scripts/v4_size")
    for suite in ("tpcds", "tpch"):
        directory = root / f"{suite}_sql"
        if not directory.exists():
            continue
        totals: dict[str, Counter] = {"v3": Counter(), "v4": Counter()}
        worst: list[tuple[int, str, Counter]] = []
        for path in sorted(directory.glob("*_v4.sql")):
            label = path.name[:-7]
            v4 = analyze(path.read_text())
            v3_path = directory / f"{label}_v3.sql"
            v3 = analyze(v3_path.read_text()) if v3_path.exists() else Counter()
            totals["v4"] += v4
            totals["v3"] += v3
            excess = sum(v4.values()) - sum(v3.values())
            if excess > 0:
                worst.append((excess, label, v4 - v3))
        print(f"== {suite} ==")
        for planner in ("v3", "v4"):
            row = totals[planner]
            print(f"  {planner}: " + "  ".join(f"{k}={row[k]}" for k in sorted(row)))
        print("  queries with more symptoms under v4:")
        for excess, label, delta in sorted(worst, reverse=True)[:12]:
            detail = " ".join(f"{k}+{v}" for k, v in sorted(delta.items()) if v > 0)
            print(f"    {label:<12} +{excess}  {detail}")
        print()


if __name__ == "__main__":
    main()
