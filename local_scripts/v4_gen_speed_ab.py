"""v3 vs v4 query GENERATION cost A/B — one process, legs interleaved per query.

Per query and leg: one unprofiled generation (wall seconds, sanity only — this
box is spiky) and one cProfile'd generation (total function calls, the real
metric per feedback_measure_call_counts_not_seconds). Fresh Environment per
generation; engine per suite with the DB imported so datasource inlining
applies, matching production shape.

Usage: python local_scripts/v4_gen_speed_ab.py [tpcds|tpch|both] [walls]

`walls` mode: no profiling; three interleaved reps per leg, keep the MIN wall
per leg (noise only adds time). Run it with NOTHING else on the box.
Writes local_scripts/v4_gen_ab_results[_walls].tsv and prints a summary.
"""

from __future__ import annotations

import cProfile
import pstats
import sys
import time
from pathlib import Path

sys.setrecursionlimit(20000)

from trilogy import Dialects, Executor
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig

ROOT = Path(__file__).parent.parent
SUITES = {
    "tpcds": ROOT / "tests" / "modeling" / "tpc_ds_duckdb",
    "tpch": ROOT / "tests" / "modeling" / "tpc_h",
}
OUT = Path(__file__).parent / "v4_gen_ab_results.tsv"


def make_engine(working: Path) -> Executor:
    env = Environment(working_path=working)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, conf=DuckDBConfig()
    )
    engine.execute_raw_sql(f"IMPORT DATABASE '{working / 'memory'}';")
    return engine


def gen(engine: Executor, working: Path, text: str, v4: bool) -> tuple[float, str]:
    engine.environment = Environment(working_path=working)
    CONFIG.use_v4_discovery = v4
    t0 = time.perf_counter()
    try:
        engine.generate_sql(text)
        err = ""
    except Exception as e:
        err = type(e).__name__
    finally:
        CONFIG.use_v4_discovery = True
    return time.perf_counter() - t0, err


def gen_calls(engine: Executor, working: Path, text: str, v4: bool) -> tuple[int, str]:
    engine.environment = Environment(working_path=working)
    CONFIG.use_v4_discovery = v4
    profiler = cProfile.Profile()
    profiler.enable()
    try:
        engine.generate_sql(text)
        err = ""
    except Exception as e:
        err = type(e).__name__
    finally:
        profiler.disable()
        CONFIG.use_v4_discovery = True
    return pstats.Stats(profiler).total_calls, err


def main() -> None:
    which = sys.argv[1] if len(sys.argv) > 1 else "both"
    walls_only = "walls" in sys.argv[2:]
    suites = ["tpcds", "tpch"] if which == "both" else [which]
    rows: list[tuple[str, float, float, int, int, str, str]] = []
    for suite in suites:
        working = SUITES[suite]
        engine = make_engine(working)
        for f in sorted(working.glob("query*.preql")):
            label = f"{suite}/{f.stem}"
            text = f.read_text()
            v4_wall, v4_err = gen(engine, working, text, v4=True)
            v3_wall, v3_err = gen(engine, working, text, v4=False)
            if walls_only:
                v4_calls = v3_calls = 0
                for _ in range(2):
                    w, e = gen(engine, working, text, v4=True)
                    v4_wall, v4_err = min(v4_wall, w), v4_err or e
                    w, e = gen(engine, working, text, v4=False)
                    v3_wall, v3_err = min(v3_wall, w), v3_err or e
            else:
                v4_calls, e = gen_calls(engine, working, text, v4=True)
                v4_err = v4_err or e
                v3_calls, e = gen_calls(engine, working, text, v4=False)
                v3_err = v3_err or e
            rows.append((label, v4_wall, v3_wall, v4_calls, v3_calls, v4_err, v3_err))
            print(
                f"{label}\twall v4={v4_wall:.2f}s v3={v3_wall:.2f}s\t"
                f"calls v4={v4_calls} v3={v3_calls}"
                + (f"\tERR v4={v4_err} v3={v3_err}" if v4_err or v3_err else ""),
                flush=True,
            )
    out = OUT.with_name("v4_gen_ab_results_walls.tsv") if walls_only else OUT
    with out.open("w", encoding="utf-8") as fh:
        fh.write("query\tv4_wall\tv3_wall\tv4_calls\tv3_calls\tv4_err\tv3_err\n")
        for r in rows:
            fh.write("\t".join(str(x) for x in r) + "\n")
    ok = [r for r in rows if not r[5] and not r[6]]
    for suite in suites:
        sub = [r for r in ok if r[0].startswith(suite)]
        if not sub:
            continue
        w4, w3 = sum(r[1] for r in sub), sum(r[2] for r in sub)
        c4, c3 = sum(r[3] for r in sub), sum(r[4] for r in sub)
        print(
            f"\n{suite}: {len(sub)} comparable queries\n"
            f"  wall  v4={w4:.1f}s v3={w3:.1f}s ratio={w4 / w3:.2f}x"
        )
        if c3:
            print(f"  calls v4={c4:,} v3={c3:,} ratio={c4 / c3:.2f}x")
        metric = (lambda r: r[3] - r[4]) if c3 else (lambda r: r[1] - r[2])
        unit = "call" if c3 else "wall"
        print(f"  worst v4 queries by {unit} delta (v4-v3):")
        for r in sorted(sub, key=metric, reverse=True)[:8]:
            if c3:
                print(f"    {r[0]}\tv4={r[3]:,}\tv3={r[4]:,}\tdelta={r[3] - r[4]:+,}")
            else:
                print(
                    f"    {r[0]}\tv4={r[1]:.2f}s\tv3={r[2]:.2f}s\tdelta={r[1] - r[2]:+.2f}s"
                )
    err_rows = [r for r in rows if r[5] or r[6]]
    if err_rows:
        print(f"\n{len(err_rows)} queries with generation errors (excluded):")
        for r in err_rows:
            print(f"  {r[0]}\tv4_err={r[5]}\tv3_err={r[6]}")
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
