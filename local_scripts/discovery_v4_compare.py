"""Run TPC-DS queries through the v4 planner and compare against the
v3-logged reference SQL.

For each `queryNN.preql`:
  1. Generate v4 SQL via the new planner (catch failures).
  2. Read the v3 reference SQL from `zqueryNN.log` (TOML).
  3. Execute both against the duckdb engine from the test suite's `memory/`
     dataset (sf=1).
  4. Compare result rows (multiset-equal — count by tuple).
  5. Write a per-query markdown report to `local_scripts/v4_compare/`.

A summary index goes to `local_scripts/v4_compare/SUMMARY.md`.

    python local_scripts/discovery_v4_compare.py                # all queries
    python local_scripts/discovery_v4_compare.py --queries 1,2,3
    python local_scripts/discovery_v4_compare.py --query 2
"""

from __future__ import annotations

import argparse
import sys
import traceback
import tomllib
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import duckdb

# Make `discovery_v4` importable.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from discovery_v4 import (  # noqa: E402
    TPCDS_DIR,
    compile_sql,
    run_tpcds_query,
)

OUT_DIR = Path(__file__).parent / "v4_compare"
DATASET_DIR = TPCDS_DIR / "memory"


@dataclass
class QueryResult:
    query_id: str
    v4_sql: Optional[str] = None
    v4_gen_error: Optional[str] = None
    v4_exec_error: Optional[str] = None
    v4_rows: Optional[list[tuple]] = None
    ref_sql: Optional[str] = None
    ref_exec_error: Optional[str] = None
    ref_rows: Optional[list[tuple]] = None
    diff: list[str] = field(default_factory=list)

    @property
    def status(self) -> str:
        if self.v4_gen_error:
            return "gen_fail"
        if self.v4_exec_error:
            return "exec_fail"
        if self.ref_exec_error:
            return "ref_fail"
        if self.v4_rows is None or self.ref_rows is None:
            return "no_data"
        if Counter(self.v4_rows) == Counter(self.ref_rows):
            return "match"
        return "mismatch"


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Replicate the tpc_ds_duckdb engine setup, but just the raw duckdb conn
    (no trilogy executor — we only need to execute SQL)."""
    if not (DATASET_DIR / "call_center.parquet").exists():
        raise FileNotFoundError(
            f"TPC-DS sf=1 dataset missing at {DATASET_DIR}. Run a test from "
            f"tests/modeling/tpc_ds_duckdb/ once to generate it."
        )
    con = duckdb.connect(":memory:")
    con.execute(f"IMPORT DATABASE '{DATASET_DIR}';")
    con.execute("SET enable_progress_bar=false;")
    return con


def load_reference_sql(query_id: str) -> Optional[str]:
    log_path = TPCDS_DIR / f"zquery{query_id}.log"
    if not log_path.exists():
        return None
    data = tomllib.loads(log_path.read_text())
    sql = data.get("generated_sql")
    if not isinstance(sql, str):
        return None
    return sql.strip()


def execute(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple]:
    return list(con.execute(sql).fetchall())


def generate_v4_sql(query_id: str) -> tuple[Optional[str], Optional[str]]:
    """Run the v4 planner and return (sql, error). Either may be None."""
    try:
        info, build_env, _ = run_tpcds_query(query_id)
    except Exception:
        return None, traceback.format_exc()
    if info.strategy_node is None:
        return None, "v4 produced no strategy node"
    try:
        sql = compile_sql(info, build_env)
    except Exception:
        return None, traceback.format_exc()
    return (sql.strip() if sql else None), None


def diff_summary(v4_rows: list[tuple], ref_rows: list[tuple]) -> list[str]:
    """Multiset diff: rows only in v4 / only in ref / count mismatch."""
    v4_counter = Counter(v4_rows)
    ref_counter = Counter(ref_rows)
    lines: list[str] = []
    lines.append(f"v4 rows: {len(v4_rows)} ({len(v4_counter)} distinct)")
    lines.append(f"ref rows: {len(ref_rows)} ({len(ref_counter)} distinct)")
    only_v4 = list((v4_counter - ref_counter).items())
    only_ref = list((ref_counter - v4_counter).items())
    if only_v4:
        lines.append(f"only in v4 (showing up to 5 of {len(only_v4)}):")
        for row, count in only_v4[:5]:
            lines.append(f"  {count}x  {row}")
    if only_ref:
        lines.append(f"only in ref (showing up to 5 of {len(only_ref)}):")
        for row, count in only_ref[:5]:
            lines.append(f"  {count}x  {row}")
    return lines


def run_one(
    con: duckdb.DuckDBPyConnection,
    query_id: str,
) -> QueryResult:
    result = QueryResult(query_id=query_id)

    v4_sql, v4_err = generate_v4_sql(query_id)
    result.v4_sql = v4_sql
    result.v4_gen_error = v4_err

    ref_sql = load_reference_sql(query_id)
    result.ref_sql = ref_sql

    if v4_sql:
        try:
            result.v4_rows = execute(con, v4_sql)
        except Exception:
            result.v4_exec_error = traceback.format_exc()

    if ref_sql:
        try:
            result.ref_rows = execute(con, ref_sql)
        except Exception:
            result.ref_exec_error = traceback.format_exc()

    if result.v4_rows is not None and result.ref_rows is not None:
        result.diff = diff_summary(result.v4_rows, result.ref_rows)
    return result


def write_query_report(result: QueryResult, preql_text: str) -> Path:
    path = OUT_DIR / f"query{result.query_id}.md"
    lines: list[str] = []
    lines.append(f"# Query {result.query_id}")
    lines.append("")
    lines.append(f"**Status:** `{result.status}`")
    lines.append("")
    lines.append("| Stage | Result |")
    lines.append("| --- | --- |")
    lines.append(f"| v4 SQL generation | {'OK' if result.v4_sql else 'FAILED'} |")
    if result.v4_rows is not None:
        lines.append(f"| v4 execution | OK ({len(result.v4_rows)} rows) |")
    elif result.v4_exec_error:
        lines.append("| v4 execution | FAILED |")
    elif result.v4_sql:
        lines.append("| v4 execution | not attempted |")
    if result.ref_rows is not None:
        lines.append(f"| reference execution | OK ({len(result.ref_rows)} rows) |")
    elif result.ref_exec_error:
        lines.append("| reference execution | FAILED |")
    elif result.ref_sql:
        lines.append("| reference execution | not attempted |")
    else:
        lines.append("| reference execution | no log |")
    if result.v4_rows is not None and result.ref_rows is not None:
        identical = Counter(result.v4_rows) == Counter(result.ref_rows)
        lines.append(f"| results identical | {'YES' if identical else 'NO'} |")
    lines.append("")

    lines.append("## Preql")
    lines.append("")
    lines.append("```")
    lines.append(preql_text.rstrip())
    lines.append("```")
    lines.append("")

    lines.append("## v4 generated SQL")
    lines.append("")
    if result.v4_sql:
        lines.append("```sql")
        lines.append(result.v4_sql)
        lines.append("```")
    else:
        lines.append("_v4 did not produce SQL._")
    lines.append("")

    lines.append("## Reference SQL (zquery log)")
    lines.append("")
    if result.ref_sql:
        lines.append("```sql")
        lines.append(result.ref_sql)
        lines.append("```")
    else:
        lines.append("_no reference log found._")
    lines.append("")

    if result.diff:
        lines.append("## Result comparison")
        lines.append("")
        for line in result.diff:
            lines.append(line)
        lines.append("")

    for label, err in (
        ("v4 generation error", result.v4_gen_error),
        ("v4 execution error", result.v4_exec_error),
        ("reference execution error", result.ref_exec_error),
    ):
        if err:
            lines.append(f"## {label}")
            lines.append("")
            lines.append("```")
            lines.append(err.rstrip())
            lines.append("```")
            lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def write_summary(results: list[QueryResult]) -> Path:
    path = OUT_DIR / "SUMMARY.md"
    lines: list[str] = []
    lines.append("# v4 vs reference — TPC-DS comparison")
    lines.append("")
    counts = Counter(r.status for r in results)
    lines.append(f"**Total:** {len(results)} queries")
    for status in ("match", "mismatch", "exec_fail", "gen_fail", "ref_fail", "no_data"):
        if counts[status]:
            lines.append(f"- {status}: {counts[status]}")
    lines.append("")
    lines.append("| Query | Status | v4 rows | ref rows |")
    lines.append("| --- | --- | --- | --- |")
    for r in sorted(results, key=lambda x: x.query_id):
        v4r = len(r.v4_rows) if r.v4_rows is not None else "-"
        rfr = len(r.ref_rows) if r.ref_rows is not None else "-"
        lines.append(f"| [{r.query_id}](query{r.query_id}.md) | {r.status} | {v4r} | {rfr} |")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def discover_query_ids() -> list[str]:
    ids: list[str] = []
    for path in sorted(TPCDS_DIR.glob("query*.preql")):
        stem = path.stem  # queryNN
        suffix = stem[len("query"):]
        if suffix.isdigit():
            ids.append(suffix.zfill(2))
    return ids


def parse_query_arg(arg: str) -> list[str]:
    out: list[str] = []
    for piece in arg.split(","):
        piece = piece.strip()
        if not piece:
            continue
        if piece.isdigit():
            out.append(f"{int(piece):02d}")
        else:
            out.append(piece)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query", help="Single query id (e.g. '02' or '2').")
    parser.add_argument(
        "--queries",
        help="Comma-separated query ids; overrides --query.",
    )
    args = parser.parse_args()

    if args.queries:
        ids = parse_query_arg(args.queries)
    elif args.query:
        ids = parse_query_arg(args.query)
    else:
        ids = discover_query_ids()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = get_duckdb_connection()
    results: list[QueryResult] = []
    for qid in ids:
        preql_path = TPCDS_DIR / f"query{qid}.preql"
        if not preql_path.exists():
            print(f"[skip] {qid}: no preql file")
            continue
        preql_text = preql_path.read_text()
        print(f"[run] {qid} ...", end=" ", flush=True)
        result = run_one(con, qid)
        print(result.status)
        write_query_report(result, preql_text)
        results.append(result)

    if results:
        summary_path = write_summary(results)
        print(f"\nWrote {len(results)} per-query reports + {summary_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
