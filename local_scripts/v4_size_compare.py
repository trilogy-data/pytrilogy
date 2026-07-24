"""Compare v3 vs v4 generated-SQL size for the _TPCDS_SIZE known-failing queries.

Generates SQL only (no execution) under both planners, reports stripped length
(same metric as the test ceilings) and a CTE count proxy.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.setrecursionlimit(20000)

from tests.modeling.tpc_ds_duckdb.query_size import query_size
from trilogy import Dialects
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment

WORKING = Path("tests/modeling/tpc_ds_duckdb")

# (label, preql_file, ceiling)
QUERIES = [
    ("02", "query02.preql", 7500),
    ("2.1", "query02-one.preql", 7500),
    ("2.2", "query02-two.preql", 7500),
    ("10", "query10.preql", 7000),
    ("12", "query12.preql", 3200),
    ("20", "query20.preql", 3200),
    ("23", "query23.preql", 8500),
    ("30.alt", "query30-alt.preql", 12000),
    ("47", "query47.preql", 6800),
    ("50", "query50.preql", 7000),
    ("57", "query57.preql", 6500),
    ("62", "query62.preql", 2500),
    ("69", "query69.preql", 5000),
    ("73", "query73.preql", 3000),
    ("76", "query76.preql", 10000),
    ("81", "query81.preql", 8000),
    ("94", "query94.preql", 5000),
    ("97.1", "query97-one.preql", 4250),
]


def cte_count(sql: str) -> int:
    # proxy: number of SELECTs (each CTE / subquery is one SELECT)
    return sql.lower().count("select")


def gen(label: str, preql_file: str, v4: bool) -> tuple[int, int, str]:
    CONFIG.use_v4_discovery = v4
    env = Environment(working_path=WORKING)
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    text = (WORKING / preql_file).read_text()
    sql = engine.generate_sql(text)[-1]
    return query_size(sql, "sql"), cte_count(sql), sql


def main() -> None:
    rows = []
    for label, pf, ceil in QUERIES:
        try:
            v3_len, v3_ctes, _ = gen(label, pf, v4=False)
        except Exception as e:
            v3_len, v3_ctes = -1, -1
            print(f"{label}: v3 ERROR {type(e).__name__}: {e}")
        try:
            v4_len, v4_ctes, v4_sql = gen(label, pf, v4=True)
        except Exception as e:
            v4_len, v4_ctes = -1, -1
            print(f"{label}: v4 ERROR {type(e).__name__}: {e}")
            continue
        rows.append((label, ceil, v3_len, v4_len, v3_ctes, v4_ctes))
        (WORKING / f"zsize_v4_{label}.sql").write_text(v4_sql)

    print(
        f"\n{'q':>7} {'ceil':>6} {'v3':>6} {'v4':>6} {'d':>6} {'over?':>5} "
        f"{'v3sel':>6} {'v4sel':>6}"
    )
    for label, ceil, v3_len, v4_len, v3c, v4c in rows:
        delta = v4_len - v3_len
        over = "OVER" if v4_len >= ceil else ""
        print(
            f"{label:>7} {ceil:>6} {v3_len:>6} {v4_len:>6} {delta:>+6} {over:>5} "
            f"{v3c:>6} {v4c:>6}"
        )


if __name__ == "__main__":
    main()
