"""Profile one query's GENERATION under v4 or v3 and print hot functions.

Usage: python local_scripts/v4_gen_profile_query.py <tpcds|tpch> <queryNN> [v3] [ncalls|tottime|cumtime]
"""

from __future__ import annotations

import cProfile
import pstats
import sys
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


def main() -> None:
    suite, query = sys.argv[1], sys.argv[2]
    CONFIG.use_v4_discovery = "v3" not in sys.argv[3:]
    sort = next(
        (a for a in sys.argv[3:] if a in ("ncalls", "tottime", "cumtime")), "ncalls"
    )
    working = SUITES[suite]
    env = Environment(working_path=working)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, conf=DuckDBConfig()
    )
    engine.execute_raw_sql(f"IMPORT DATABASE '{working / 'memory'}';")
    text = (working / f"{query}.preql").read_text()
    engine.environment = Environment(working_path=working)
    profiler = cProfile.Profile()
    profiler.enable()
    engine.generate_sql(text)
    profiler.disable()
    stats = pstats.Stats(profiler)
    print(f"{suite}/{query} v4={CONFIG.use_v4_discovery} total={stats.total_calls}")
    stats.sort_stats(sort).print_stats(25)


if __name__ == "__main__":
    main()
