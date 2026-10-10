"""Diff two SQL captures: `corpus_sql.py` json, or `sqlcap.py` jsonl.

    python local_scripts/sql_ab/sqldiff.py <base> <new> [--show N] [--only substr]

A test's statements are compared as a set, since several tests emit them in
an order that varies between runs. CTE names are normalized by order of
appearance and temp paths, timestamps and ids are masked, so neither is a
diff. See README.md.
"""

from __future__ import annotations

import argparse
import difflib
import json
import re
from pathlib import Path

CTE_DEF = re.compile(r"^(\w+) as \($", re.MULTILINE)

NOISE = [
    (re.compile(r"pytest-\d+"), "pytest-N"),
    (re.compile(r"tmp[a-z0-9_]{8}"), "tmpX"),
    (re.compile(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?"), "TS"),
    (
        re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
        "UUID",
    ),
    (re.compile(r"[0-9a-f]{32}"), "HEX"),
    # the checkout's own directory, which differs between two worktrees
    (re.compile(r"coding_projects[\\/]+[\w.-]+"), "coding_projects/TREE"),
]


def load(path: str) -> dict[str, tuple[str, list[str]]]:
    """key -> (outcome, statements)"""
    p = Path(path)
    if p.suffix == ".jsonl":
        out: dict[str, tuple[str, list[str]]] = {}
        for line in p.read_text(encoding="utf-8").splitlines():
            r = json.loads(line)
            out[r["nodeid"]] = (r["outcome"], r["sql"])
        return out
    corpus = json.loads(p.read_text(encoding="utf-8"))
    return {k: ("compiled", [v]) for k, v in corpus.items()}


def normalize(sql: str) -> str:
    for pattern, repl in NOISE:
        sql = pattern.sub(repl, sql)
    names: list[str] = []
    for m in CTE_DEF.finditer(sql):
        if m.group(1) not in names:
            names.append(m.group(1))
    # two steps, so a new name never collides with an old one
    for i, name in enumerate(names):
        sql = re.sub(rf"\b{re.escape(name)}\b", f"@@{i}@@", sql)
    return re.sub(r"@@(\d+)@@", r"cte_\1", sql)


def stats(sql: str) -> tuple[int, int, int]:
    return (len(CTE_DEF.findall(sql)), len(re.findall(r"\bJOIN\b", sql)), len(sql))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("base")
    p.add_argument("new")
    p.add_argument("--show", type=int, default=0, help="diff lines per statement")
    p.add_argument("--only", default="", help="keys containing this")
    a = p.parse_args()
    base, new = load(a.base), load(a.new)
    moved = 0
    for key in sorted(set(base) | set(new)):
        if a.only and a.only not in key:
            continue
        if key not in base or key not in new:
            print(f"ONLY IN {'new' if key in new else 'base'}: {key}")
            continue
        if base[key][0] != new[key][0]:
            print(f"OUTCOME {key}: {base[key][0]} -> {new[key][0]}")
        b = [normalize(s) for s in base[key][1]]
        n = [normalize(s) for s in new[key][1]]
        gone = [s for s in dict.fromkeys(b) if s not in n]
        came = [s for s in dict.fromkeys(n) if s not in b]
        if not gone and not came:
            continue
        moved += 1
        print(f"MOVED {key}: {len(gone)} gone, {len(came)} new")
        for g, c in zip(gone, came):
            sb, sn = stats(g), stats(c)
            smaller = all(y <= x for x, y in zip(sb, sn))
            print(
                f"    ctes {sb[0]}->{sn[0]} joins {sb[1]}->{sn[1]}"
                f" len {sb[2]}->{sn[2]} {'smaller' if smaller else 'LARGER'}"
            )
            diff = difflib.unified_diff(
                g.splitlines(), c.splitlines(), "base", "new", lineterm="", n=1
            )
            print("\n".join(list(diff)[: a.show]))
    print(f"{len(base)} base keys, {len(new)} new keys, {moved} moved")


if __name__ == "__main__":
    main()
