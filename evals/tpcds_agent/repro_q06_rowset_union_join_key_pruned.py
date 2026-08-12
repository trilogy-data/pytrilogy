"""Generation-only reproduction for the q06 rowset join-key pruning bug."""

from __future__ import annotations

from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = REPO_ROOT / "tests" / "modeling" / "tpc_ds_duckdb"
QUERY_PATH = Path(__file__).with_name("repro_q06_rowset_union_join_key_pruned.preql")


def main() -> None:
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=MODEL_DIR)
    )
    sql = executor.generate_sql(QUERY_PATH.read_text(encoding="utf-8"))[-1]
    print(sql)

    if "FULL JOIN" not in sql:
        raise AssertionError("The category join disappeared entirely")
    if "cat_avg_category" not in sql:
        raise AssertionError("The category-average key was not projected")
    if "on 1=1" in sql:
        raise AssertionError(
            "REPRODUCED: the category key is grouped, then pruned before its join"
        )
    print("\nFIXED: the union join renders the category key")


if __name__ == "__main__":
    main()
