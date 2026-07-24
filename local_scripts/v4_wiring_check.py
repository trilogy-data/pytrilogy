"""Prove the CONFIG.use_v4_discovery wiring mirrors the validated harness.

For each query id, generate v4 SQL two ways and diff the text:
  * production path  — Executor.generate_sql with CONFIG.use_v4_discovery=True
  * harness path     — discovery_v4.run_tpcds_query + compile_sql

The harness path is what discovery_v4_compare.py already validates against the
v3 reference logs, so identical text means the production entrypoint inherits
that validation.

    python local_scripts/v4_wiring_check.py            # full test set
    python local_scripts/v4_wiring_check.py 01 77 59
"""

from __future__ import annotations

import sys
import traceback
from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "local_scripts"))

from discovery_v4 import (
    TPCDS_DIR,
    compile_sql,
    run_tpcds_query,
)


def production_sql(query_id: str) -> str:
    preql = (TPCDS_DIR / f"query{query_id}.preql").read_text()
    env = Environment(working_path=TPCDS_DIR)
    eng = Dialects.DUCK_DB.default_executor(environment=env)
    CONFIG.use_v4_discovery = True
    try:
        statements = eng.generate_sql(preql)
    finally:
        CONFIG.use_v4_discovery = False
    return statements[-1].strip()


def harness_sql(query_id: str) -> str:
    info, build_env, _, build_stmt = run_tpcds_query(query_id)
    sql = compile_sql(info, build_env, build_stmt)
    return (sql or "").strip()


def run_one(qid: str) -> str:
    try:
        prod = production_sql(qid)
        harn = harness_sql(qid)
    except Exception:
        return "error\n" + traceback.format_exc()
    if prod == harn:
        return f"identical ({prod.count(chr(10)) + 1} lines)"
    return f"DIFFERS prod={prod.count(chr(10)) + 1}L harness={harn.count(chr(10)) + 1}L"


def main(argv: list[str]) -> int:
    if argv:
        ids = [
            a if a.lstrip("-").isdigit() and int(a) < 0 else a.zfill(2) for a in argv
        ]
    else:
        ids = [
            line.split("#", 1)[0].strip()
            for line in (REPO_ROOT / "local_scripts" / "v4_compare" / "test_set.txt")
            .read_text()
            .splitlines()
        ]
        ids = [i for i in ids if i]
    bad: list[str] = []
    for qid in ids:
        if not (TPCDS_DIR / f"query{qid}.preql").exists():
            print(f"[skip] {qid}")
            continue
        status = run_one(qid)
        first = status.splitlines()[0]
        print(f"{qid}: {first}")
        if not first.startswith("identical"):
            bad.append(f"{qid}: {status}")
    print(f"\n{len(ids) - len(bad)}/{len(ids)} identical")
    if bad:
        print("\n=== differences ===")
        for b in bad:
            print(b)
            print("-" * 60)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
