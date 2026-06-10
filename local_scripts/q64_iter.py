"""Fast q64 refactor harness. Caps DuckDB at 3GB. Runs a candidate preql at sf=1
and compares to a saved gold (the known-good 2-row baseline)."""

import json
import sys
from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig

working_path = Path("tests/modeling/tpc_ds_duckdb")
GOLD = Path("local_scripts/q64_gold.json")


def make_engine(subdir="memory"):
    env = Environment(working_path=working_path)
    eng = Dialects.DUCK_DB.default_executor(environment=env, conf=DuckDBConfig())
    eng.execute_raw_sql(f"IMPORT DATABASE '{working_path / subdir}';")
    eng.execute_raw_sql("SET memory_limit='3GB';")
    eng.execute_raw_sql("SET threads=4;")
    return eng


def run(eng, filename):
    eng.environment = Environment(working_path=working_path)
    sql = eng.generate_sql((working_path / filename).read_text())[-1]
    rows = [[str(c) for c in r] for r in eng.execute_raw_sql(sql).fetchall()]
    return sql, rows


if __name__ == "__main__":
    cmd = sys.argv[1]
    eng = make_engine()
    if cmd == "gold":
        _, rows = run(eng, "query64.preql")
        GOLD.write_text(json.dumps(rows))
        print(f"gold saved: {len(rows)} rows")
    else:
        sql, rows = run(eng, cmd)
        gold = json.loads(GOLD.read_text())
        print(f"sql_len={len(sql)} cte_count={sql.count(' as (')} rows={len(rows)}")
        print("MATCH" if rows == gold else "MISMATCH")
        if rows != gold:
            print("gold:", gold)
            print("got :", rows)
