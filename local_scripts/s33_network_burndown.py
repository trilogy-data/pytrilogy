"""Cutover burndown: generate the TPC-DS corpus with the network search ON and
report what breaks, grouped by failure signature.

    .venv/Scripts/python.exe local_scripts/s33_network_burndown.py [query ...]

Generation only — it isolates planner/render failures from row differences, which
the test battery covers. See docs/v4_network_discovery_design.md.
"""

from __future__ import annotations

import sys
import traceback
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from trilogy import Dialects, Environment

TPCDS_ROOT = (
    Path(__file__).resolve().parents[1] / "tests" / "modeling" / "tpc_ds_duckdb"
)


def generate(path: Path) -> tuple[bool, str]:
    env = Environment(working_path=path.parent)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    try:
        executor.generate_sql(path.read_text())
    except Exception as exc:
        frame = traceback.extract_tb(exc.__traceback__)[-1]
        site = f"{Path(frame.filename).name}:{frame.lineno}"
        return False, f"{type(exc).__name__} @ {site}: {str(exc)[:160]}"
    return True, ""


def main(argv: list[str]) -> int:
    queries = (
        [Path(a) if Path(a).exists() else TPCDS_ROOT / a for a in argv]
        if argv
        else sorted(TPCDS_ROOT.glob("query*.preql"))
    )
    stats: Counter[str] = Counter()
    by_signature: dict[str, list[str]] = defaultdict(list)
    for path in queries:
        ok, reason = generate(path)
        stats["pass" if ok else "fail"] += 1
        if ok:
            print(f"  ok   {path.name}")
        else:
            print(f"  FAIL {path.name}: {reason}")
            by_signature[reason.split(":")[0]].append(path.name)
    print("\n=== failures by signature ===")
    for signature, names in sorted(
        by_signature.items(), key=lambda kv: (-len(kv[1]), kv[0])
    ):
        print(f"{len(names):3}  {signature}")
        print(f"     {', '.join(names)}")
    print("\n=== summary ===")
    for key, value in sorted(stats.items()):
        print(f"{key:8} {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
