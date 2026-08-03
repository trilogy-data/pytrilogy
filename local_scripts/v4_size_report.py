"""v3 vs v4 generated-SQL size report over the FULL TPC-DS and TPC-H corpora.

Real rendered SQL (DB imported, so inlining/optimization apply), generation only.
Writes a TSV per suite plus the raw SQL for both planners so any regression can be
diffed directly.

Usage: python local_scripts/v4_size_report.py [tpcds|tpch|both] [outdir]
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

ROOT = Path(__file__).parent.parent
CTE_RE = re.compile(r"\b\w+\s+as\s*\(\s*\n?\s*SELECT", re.IGNORECASE)

SUITES = {
    "tpcds": ROOT / "tests" / "modeling" / "tpc_ds_duckdb",
    "tpch": ROOT / "tests" / "modeling" / "tpc_h",
}

HEADER = (
    "query\tv3_len\tv4_len\tdelta\tratio\tv3_cte\tv4_cte\tv3_sel\tv4_sel"
    "\tref_len\tv4_vs_ref\tv3_t\tv4_t"
)


def make_engine(working: Path) -> Executor:
    env = Environment(working_path=working)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, conf=DuckDBConfig()
    )
    engine.execute_raw_sql(f"IMPORT DATABASE '{working / 'memory'}';")
    return engine


def measure(engine: Executor, working: Path, fname: str) -> tuple | str:
    engine.environment = Environment(working_path=working)
    text = (working / fname).read_text()
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


def reference_size(working: Path, label: str) -> int:
    ref = working / f"query{label}.sql"
    return query_size(ref.read_text(), "sql") if ref.exists() else 0


def run_suite(suite: str, outdir: Path) -> None:
    working = SUITES[suite]
    sql_dir = outdir / f"{suite}_sql"
    sql_dir.mkdir(parents=True, exist_ok=True)
    engine = make_engine(working)
    files = sorted(working.glob("query*.preql"))
    lines = [HEADER]
    for f in files:
        label = f.stem.replace("query", "")
        CONFIG.use_v4_discovery = False
        r3 = measure(engine, working, f.name)
        CONFIG.use_v4_discovery = True
        r4 = measure(engine, working, f.name)
        ref = reference_size(working, label)
        if isinstance(r3, str) or isinstance(r4, str):
            v3s = r3 if isinstance(r3, str) else str(r3[0])
            v4s = r4 if isinstance(r4, str) else str(r4[0])
            row = f"{label}\t{v3s}\t{v4s}\t-\t-\t-\t-\t-\t-\t{ref}\t-\t-\t-"
            if not isinstance(r4, str):
                (sql_dir / f"q{label}_v4.sql").write_text(r4[4])
        else:
            v3_len, v3_cte, v3_sel, v3_t, v3_sql = r3
            v4_len, v4_cte, v4_sel, v4_t, v4_sql = r4
            ratio = v4_len / v3_len if v3_len else 0.0
            vs_ref = f"{v4_len / ref:.2f}" if ref else "-"
            row = (
                f"{label}\t{v3_len}\t{v4_len}\t{v4_len - v3_len}\t{ratio:.2f}"
                f"\t{v3_cte}\t{v4_cte}\t{v3_sel}\t{v4_sel}"
                f"\t{ref}\t{vs_ref}\t{v3_t:.2f}\t{v4_t:.2f}"
            )
            (sql_dir / f"q{label}_v3.sql").write_text(v3_sql)
            (sql_dir / f"q{label}_v4.sql").write_text(v4_sql)
        lines.append(row)
        print(f"[{suite}] {row}", flush=True)
    out = outdir / f"v4_size_{suite}.tsv"
    out.write_text("\n".join(lines) + "\n")
    print(f"wrote {out}", flush=True)


def main() -> None:
    which = sys.argv[1] if len(sys.argv) > 1 else "both"
    outdir = (
        Path(sys.argv[2]) if len(sys.argv) > 2 else ROOT / "local_scripts" / "v4_size"
    )
    outdir.mkdir(parents=True, exist_ok=True)
    for suite in ["tpcds", "tpch"] if which == "both" else [which]:
        run_suite(suite, outdir)


if __name__ == "__main__":
    main()
