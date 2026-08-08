"""Call-count profile for `functional_dependency.py` over TPC-DS generation.

Reports CALL COUNTS, not seconds — this box swings 45-75s on identical runs.
`build_fd_closure` calls and the `_fd_*` control rows must be IDENTICAL across
a before/after pair: they prove the fixpoint did the same work and only the
per-iteration cost moved.

Usage: python local_scripts/v4_fd_closure_profile.py [N_QUERIES]
"""

from __future__ import annotations

import cProfile
import pstats
import sys
from pathlib import Path

sys.setrecursionlimit(20000)

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig

WORKING = Path(__file__).parent.parent / "tests" / "modeling" / "tpc_ds_duckdb"

# (file fragment, function name) rows to report, in display order.
WATCH = [
    ("functional_dependency.py", "build_fd_closure"),
    ("functional_dependency.py", "build_fd_determines"),
    ("functional_dependency.py", "minimize_build_grain"),
    ("functional_dependency.py", "_build_fd_concepts"),
    ("functional_dependency.py", "_fd_facts"),
    ("functional_dependency.py", "equivalents_for"),
    ("functional_dependency.py", "concept_attr_fd_closure"),
    ("build.py", "equivalent_addresses"),
    ("build.py", "output_concepts"),
    ("build.py", "grain"),
]


def main() -> None:
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 109
    env = Environment(working_path=WORKING)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, conf=DuckDBConfig()
    )
    engine.execute_raw_sql(f"IMPORT DATABASE '{WORKING / 'memory'}';")
    files = sorted(WORKING.glob("query*.preql"))[:limit]
    profiler = cProfile.Profile()
    profiler.enable()
    for f in files:
        engine.environment = Environment(working_path=WORKING)
        try:
            engine.generate_sql(f.read_text())
        except Exception as e:
            print(f"  !! {f.stem}: {type(e).__name__}: {e}")
    profiler.disable()
    stats = pstats.Stats(profiler)

    totals: dict[tuple[str, str], tuple[int, float, float]] = {}
    module_calls = 0
    module_tottime = 0.0
    for (path, _, name), (_, nc, tt, ct, _) in stats.stats.items():  # type: ignore[attr-defined]
        base = Path(path).name
        if base == "functional_dependency.py":
            module_calls += nc
            module_tottime += tt
        for fragment, watched in WATCH:
            if base == fragment and name == watched:
                prior = totals.get((fragment, watched), (0, 0.0, 0.0))
                totals[(fragment, watched)] = (
                    prior[0] + nc,
                    prior[1] + tt,
                    prior[2] + ct,
                )

    print(f"\nTPC-DS {len(files)} queries\n")
    print(f"{'function':<48} {'ncalls':>12} {'tottime':>9} {'cumtime':>9}")
    print("-" * 82)
    for key in WATCH:
        nc, tt, ct = totals.get(key, (0, 0.0, 0.0))
        label = f"{key[0].removesuffix('.py')}.{key[1]}"
        print(f"{label:<48} {nc:>12,} {tt:>9.2f} {ct:>9.2f}")
    print("-" * 82)
    print(
        f"{'functional_dependency.py TOTAL':<48} {module_calls:>12,} {module_tottime:>9.2f}"
    )


if __name__ == "__main__":
    main()
