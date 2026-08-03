"""Where does TPC-DS generation time go NOW, and which of it is a good Rust
candidate?

A good candidate is (a) hot, (b) operating on PLAIN DATA — addresses, sets,
ints — rather than walking BuildConcept / StrategyNode object graphs, and (c)
reachable across a COARSE call boundary, so the FFI crossing is paid once per
request and not once per inner-loop step.

Usage: python local_scripts/v4_rust_candidates.py [N_QUERIES]
"""

from __future__ import annotations

import cProfile
import pstats
import sys
from collections import defaultdict
from pathlib import Path

sys.setrecursionlimit(20000)

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig

WORKING = Path(__file__).parent.parent / "tests" / "modeling" / "tpc_ds_duckdb"


def make_engine() -> Executor:
    env = Environment(working_path=WORKING)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, conf=DuckDBConfig()
    )
    engine.execute_raw_sql(f"IMPORT DATABASE '{WORKING / 'memory'}';")
    return engine


def main() -> None:
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 109
    engine = make_engine()
    files = sorted(WORKING.glob("query*.preql"))[:limit]
    profiler = cProfile.Profile()
    profiler.enable()
    for f in files:
        engine.environment = Environment(working_path=WORKING)
        engine.generate_sql(f.read_text())
    profiler.disable()
    stats = pstats.Stats(profiler)

    by_module: dict[str, list[float]] = defaultdict(lambda: [0.0, 0])
    for (path, _line, _name), (calls, _nc, tottime, _ct, _cs) in stats.stats.items():
        module = Path(path).name if path != "~" else "<builtin>"
        by_module[module][0] += tottime
        by_module[module][1] += calls
    total = sum(entry[0] for entry in by_module.values())
    print(f"\n== tottime by module over {len(files)} queries ({total:.1f}s) ==")
    for module, (tottime, calls) in sorted(by_module.items(), key=lambda kv: -kv[1][0])[
        :20
    ]:
        print(f"{tottime:8.2f}s  {tottime / total:5.1%}  {calls:>12,} calls  {module}")

    print("\n== hottest functions (tottime) ==")
    stats.sort_stats("tottime").print_stats(25)


if __name__ == "__main__":
    main()
