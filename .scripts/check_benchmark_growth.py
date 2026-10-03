"""Compare every committed benchmark plan with its version on a base ref.

The per-run size check (`tests/modeling/_benchmark_artifacts.py`) compares a
query with its log on the same branch, so growth committed earlier on the
branch passes it. This compares the branch's logs with the base's: a query
that gained a CTE or outgrew its size budget fails unless
`tests/modeling/accepted_growth.toml` names it with a reason.

    python .scripts/check_benchmark_growth.py [base-ref]   # default origin/main
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests.modeling._benchmark_artifacts import cte_count, size_budget

ACCEPTED = ROOT / "tests" / "modeling" / "accepted_growth.toml"


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout


def _logs() -> list[str]:
    return [
        path
        for path in _git("ls-files", "tests/modeling").splitlines()
        if Path(path).name.startswith("zquery")
        and not Path(path).name.startswith("zquery_timing")
        and path.endswith(".log")
    ]


def _at(ref: str, path: str) -> dict | None:
    try:
        return tomllib.loads(_git("show", f"{ref}:{path}"))
    except (subprocess.CalledProcessError, tomllib.TOMLDecodeError):
        return None


def growth(base: dict, head: dict) -> str | None:
    base_sql, head_sql = base.get("generated_sql"), head.get("generated_sql")
    if isinstance(base_sql, str) and isinstance(head_sql, str):
        if cte_count(head_sql) > cte_count(base_sql):
            return f"CTEs {cte_count(base_sql)} -> {cte_count(head_sql)}"
    base_len, head_len = base.get("gen_length"), head.get("gen_length")
    if isinstance(base_len, int) and isinstance(head_len, int):
        if head_len > size_budget(base_len):
            return f"chars {base_len} -> {head_len} (budget {size_budget(base_len)})"
    return None


def main(base_ref: str) -> int:
    accepted = tomllib.loads(ACCEPTED.read_text()) if ACCEPTED.exists() else {}
    failures = []
    for path in _logs():
        base = _at(base_ref, path)
        if base is None:
            continue
        head = tomllib.loads((ROOT / path).read_text(encoding="utf-8"))
        grew = growth(base, head)
        if grew is None:
            continue
        key = path.removeprefix("tests/modeling/")
        if key in accepted:
            print(f"accepted {key}: {grew} ({accepted[key]['reason']})")
            continue
        failures.append(f"{key}: {grew}")
    for failure in failures:
        print(f"GREW {failure}")
    if failures:
        print(
            f"{len(failures)} benchmark plan(s) grew against {base_ref}. Shrink "
            f"them, or add each to {ACCEPTED.relative_to(ROOT)} with a reason."
        )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "origin/main"))
