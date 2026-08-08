"""Full-corpus v3 vs v4 generated-SQL size audit for TPC-DS.

Real rendered SQL (engine fixture, DB imported -> inlining applies), all
query*.preql files. Generation only, no execution. Writes a TSV report.

Usage: python local_scripts/v4_size_audit_full.py [out.tsv]
"""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path

sys.setrecursionlimit(20000)

from tests.modeling.tpc_ds_duckdb.query_size import query_size
from trilogy import Dialects, Executor
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig

WORKING = Path(__file__).parent.parent / "tests" / "modeling" / "tpc_ds_duckdb"
IMPORT_PATH = WORKING / "memory"

CTE_RE = re.compile(r"\b\w+\s+as\s*\(\s*\n?\s*SELECT", re.IGNORECASE)


def make_engine() -> Executor:
    env = Environment(working_path=WORKING)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, conf=DuckDBConfig()
    )
    engine.execute_raw_sql(f"IMPORT DATABASE '{IMPORT_PATH}';")
    return engine


def measure(engine: Executor, fname: str) -> tuple[int, int, int, float, str] | str:
    engine.environment = Environment(working_path=WORKING)
    text = (WORKING / fname).read_text()
    t0 = time.perf_counter()
    try:
        stmts = engine.generate_sql(text)
    except Exception as e:
        return f"ERR:{type(e).__name__}"
    dt = time.perf_counter() - t0
    sql = stmts[-1]
    return (
        query_size(sql, "sql"),
        len(CTE_RE.findall(sql)),
        sql.lower().count("select"),
        dt,
        sql,
    )


def main() -> None:
    out = (
        Path(sys.argv[1])
        if len(sys.argv) > 1
        else Path("local_scripts/v4_size_full.tsv")
    )
    sql_dir = Path("local_scripts/v4_size_sql")
    sql_dir.mkdir(exist_ok=True)
    engine = make_engine()
    files = sorted(WORKING.glob("query*.preql"))
    lines = [
        "query\tv3_len\tv4_len\tdelta\tratio\tv3_cte\tv4_cte\tv3_sel\tv4_sel\tv3_t\tv4_t"
    ]
    for f in files:
        label = f.stem.replace("query", "")
        CONFIG.use_v4_discovery = False
        r3 = measure(engine, f.name)
        CONFIG.use_v4_discovery = True
        r4 = measure(engine, f.name)
        if isinstance(r3, str) or isinstance(r4, str):
            v3s = r3 if isinstance(r3, str) else str(r3[0])
            v4s = r4 if isinstance(r4, str) else str(r4[0])
            row = f"{label}\t{v3s}\t{v4s}\t-\t-\t-\t-\t-\t-\t-\t-"
        else:
            v3_len, v3_cte, v3_sel, v3_t, v3_sql = r3
            v4_len, v4_cte, v4_sel, v4_t, v4_sql = r4
            ratio = v4_len / v3_len if v3_len else 0.0
            row = (
                f"{label}\t{v3_len}\t{v4_len}\t{v4_len - v3_len}\t{ratio:.2f}"
                f"\t{v3_cte}\t{v4_cte}\t{v3_sel}\t{v4_sel}"
                f"\t{v3_t:.2f}\t{v4_t:.2f}"
            )
            (sql_dir / f"q{label}_v3.sql").write_text(v3_sql)
            (sql_dir / f"q{label}_v4.sql").write_text(v4_sql)
        lines.append(row)
        print(row, flush=True)
    out.write_text("\n".join(lines) + "\n")
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
