"""SQL generation must be deterministic across interpreter hash seeds.

The q04 agent candidate (_q04_agent_rowset_union_join.preql) chains two
union joins over six per-customer rowsets. Which member of an authored join-key
equivalence group each side exposed — and therefore which key downstream joins
referenced — used to follow set iteration order: some hash seeds generated SQL
whose joins referenced a group member the source CTE never projected
(DuckDB: Values list "..." does not have a column named "w01_cid"), so a query
validated in one process failed when regenerated in another.

Generation is in-process deterministic, so each seed needs its own
interpreter: the test re-runs this module as a subprocess per seed. Seeds 0/1
diverged at plan time (exposure order) and seed 2 at render time (pseudonym
walk in CTE.get_alias); together they pin all three ordering fixes.
"""

import subprocess
import sys
from pathlib import Path

working_path = Path(__file__).parent

SEEDS = ("0", "1", "2")


def _generate() -> str:
    from trilogy import Dialects
    from trilogy.core.models.environment import Environment

    text = (working_path / "_q04_agent_rowset_union_join.preql").read_text()
    env = Environment(working_path=working_path)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    return executor.generate_sql(text)[-1]


def test_q04_generation_deterministic_and_binds():
    results: dict[str, str] = {}
    for seed in SEEDS:
        proc = subprocess.run(
            [sys.executable, __file__],
            capture_output=True,
            text=True,
            timeout=300,
            env={
                **__import__("os").environ,
                "PYTHONHASHSEED": seed,
                "PYTHONIOENCODING": "utf-8",
            },
        )
        assert proc.returncode == 0, f"seed {seed} generation failed: {proc.stderr}"
        results[seed] = proc.stdout

    distinct = set(results.values())
    assert len(distinct) == 1, (
        "SQL generation varied with hash seed; got "
        f"{len(distinct)} variants across seeds {SEEDS}"
    )

    import duckdb

    con = duckdb.connect(":memory:")
    for statement in (working_path / "memory" / "schema.sql").read_text().split(";;"):
        if statement.strip():
            con.execute(statement)
    # binds every column reference against its CTE projection (no data needed)
    con.execute(f"EXPLAIN {results[SEEDS[0]]}")


if __name__ == "__main__":
    print(_generate())
