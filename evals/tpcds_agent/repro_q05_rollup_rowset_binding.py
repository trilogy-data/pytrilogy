"""Generation-only inspector for the q05 rollup binder failure.

This deliberately does not execute the generated SQL.
"""

from __future__ import annotations

from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = REPO_ROOT / "tests" / "modeling" / "tpc_ds_duckdb"
QUERY_PATH = Path(__file__).with_name("repro_q05_rollup_rowset_binding.preql")


def rollup_cte(sql: str) -> str:
    marker = "GROUP BY\n    ROLLUP"
    marker_at = sql.index(marker)
    cte_start = sql.rfind(",\n", 0, sql.rfind(" as (\nSELECT", 0, marker_at)) + 2
    next_cte = sql.find("),\n", marker_at)
    final_select = sql.find(")\nSELECT", marker_at)
    ends = [position for position in (next_cte, final_select) if position >= 0]
    cte_end = min(ends) + 1
    return sql[cte_start:cte_end]


def main() -> None:
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=MODEL_DIR)
    )
    query = QUERY_PATH.read_text(encoding="utf-8")
    sql = executor.generate_sql(query)[0]
    snippet = rollup_cte(sql)
    print(snippet)

    leaked = any(
        projection in snippet
        for projection in (
            '"r_a_channel" as "r_a_channel"',
            '"r_channel" as "r_channel"',
        )
    )
    if not leaked:
        raise AssertionError("The known ungrouped backing key was not generated")
    print("\nREPRODUCED: rollup CTE projects a backing channel outside its GROUP BY")


if __name__ == "__main__":
    main()
