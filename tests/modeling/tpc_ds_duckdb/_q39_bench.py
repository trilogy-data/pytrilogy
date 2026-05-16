"""Ad-hoc benchmark for query 39: prints generated SQL and times execution."""

import sys
import time
from pathlib import Path

local = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(local))

from trilogy import Dialects  # noqa: E402
from trilogy.core.models.environment import Environment  # noqa: E402
from trilogy.dialect.config import DuckDBConfig  # noqa: E402

working_path = Path(__file__).parent


def main(reps: int = 25):
    env = Environment(working_path=working_path)
    engine = Dialects.DUCK_DB.default_executor(environment=env, conf=DuckDBConfig())
    engine.execute_raw_sql(f"IMPORT DATABASE '{working_path / 'memory'}';")
    engine.execute_raw_sql("SET enable_progress_bar=false;")

    text = (working_path / "query39.preql").read_text()
    engine.environment = Environment(working_path=working_path)
    sql = engine.generate_sql(text)[-1]
    print("=" * 70)
    print(f"GENERATED SQL (len={len(sql)})")
    print("=" * 70)
    print(sql)
    print("=" * 70)

    # correctness check vs reference
    rows = list(engine.execute_raw_sql(sql).fetchall())
    ref = list(engine.execute_raw_sql("PRAGMA tpcds(39);").fetchall())
    ok = len(rows) == len(ref) and all(a == b for a, b in zip(rows, ref))
    print(f"rows={len(rows)} ref={len(ref)} correct={ok}")

    # A/B: deduped GROUP BY (as generated) vs the old redundant 1,2,3,4 form,
    # interleaved so machine noise hits both equally.
    sql_dedup = sql
    sql_redundant = sql.replace(
        "GROUP BY\n    2,\n    4)", "GROUP BY\n    1,\n    2,\n    3,\n    4)"
    )
    assert sql_redundant != sql_dedup, "could not build redundant variant"

    def timed(q: str) -> float:
        t = time.perf_counter()
        engine.execute_raw_sql(q).fetchall()
        return time.perf_counter() - t

    for _ in range(5):  # warmup
        engine.execute_raw_sql(sql_dedup).fetchall()
    a, b = [], []
    for _ in range(reps):
        a.append(timed(sql_dedup))
        b.append(timed(sql_redundant))
    for label, ts in (("dedup  (GROUP BY 2,4)", a), ("redund (GROUP BY 1,2,3,4)", b)):
        ts.sort()
        print(
            f"{label}: min={ts[0]:.4f} median={ts[len(ts)//2]:.4f} "
            f"mean={sum(ts)/len(ts):.4f} max={ts[-1]:.4f} (reps={reps})"
        )


if __name__ == "__main__":
    main()
