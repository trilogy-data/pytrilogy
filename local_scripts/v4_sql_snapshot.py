"""Golden-SQL snapshot harness for v4 shape work.

Renders real v4 SQL (engine fixture, DB imported -> inlining applies) for the
full TPC-DS corpus and compares against a checked-in golden directory, so a
refactor can prove "zero shape change" or produce a reviewable diff.

Usage:
  python local_scripts/v4_sql_snapshot.py snapshot   # (re)write goldens
  python local_scripts/v4_sql_snapshot.py check      # diff vs goldens; exit 1 on drift

check writes changed outputs to local_scripts/v4_sql_drift/ for review.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.setrecursionlimit(20000)

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig

WORKING = Path(__file__).parent.parent / "tests" / "modeling" / "tpc_ds_duckdb"
IMPORT_PATH = WORKING / "memory"
GOLDEN = Path(__file__).parent / "v4_sql_golden"
DRIFT = Path(__file__).parent / "v4_sql_drift"


def make_engine() -> Executor:
    env = Environment(working_path=WORKING)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, conf=DuckDBConfig()
    )
    engine.execute_raw_sql(f"IMPORT DATABASE '{IMPORT_PATH}';")
    return engine


def render(engine: Executor, fname: str) -> str:
    engine.environment = Environment(working_path=WORKING)
    text = (WORKING / fname).read_text()
    try:
        return "\n---\n".join(engine.generate_sql(text))
    except Exception as e:
        return f"ERR:{type(e).__name__}: {e}\n"


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "check"
    if mode not in ("snapshot", "check"):
        sys.exit(f"unknown mode {mode!r}; use snapshot|check")
    engine = make_engine()
    files = sorted(WORKING.glob("query*.preql"))
    GOLDEN.mkdir(exist_ok=True)
    changed: list[str] = []
    new: list[str] = []
    for f in files:
        label = f.stem.replace("query", "")
        sql = render(engine, f.name)
        golden_file = GOLDEN / f"q{label}.sql"
        if mode == "snapshot":
            golden_file.write_text(sql)
            print(f"q{label}: snapshotted ({len(sql)} chars)", flush=True)
            continue
        if not golden_file.exists():
            new.append(label)
            print(f"q{label}: NO GOLDEN", flush=True)
            continue
        old = golden_file.read_text()
        if old == sql:
            print(f"q{label}: ok", flush=True)
        else:
            changed.append(label)
            DRIFT.mkdir(exist_ok=True)
            (DRIFT / f"q{label}.sql").write_text(sql)
            delta = len(sql) - len(old)
            print(f"q{label}: CHANGED ({delta:+d} chars)", flush=True)
    if mode == "snapshot":
        print(f"\nwrote {len(files)} goldens to {GOLDEN}")
        return
    print(
        f"\n{len(files) - len(changed) - len(new)} identical, "
        f"{len(changed)} changed, {len(new)} missing golden"
    )
    if changed:
        print(f"changed: {', '.join(changed)}")
        print(f"drift SQL in {DRIFT} — diff vs {GOLDEN}")
        sys.exit(1)


if __name__ == "__main__":
    main()
