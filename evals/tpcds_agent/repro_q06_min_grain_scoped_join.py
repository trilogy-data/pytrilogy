"""Minimal A/B: a `grain(...)` wrapper drops the scoped-join key.

Leg A uses `count(ss.item.sk ? cond)` and renders the `union join` predicate.
Leg B is identical except the count key is wrapped in `grain(...)`, which
degrades the same join to `on 1=1`.
"""

from __future__ import annotations

from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = REPO_ROOT / "tests" / "modeling" / "tpc_ds_duckdb"
QUERY_PATH = Path(__file__).with_name("repro_q06_min_grain_scoped_join.preql")

GRAIN_KEY = "grain(ss.item.sk)"
BARE_KEY = "ss.item.sk"


def generate(query: str) -> str:
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=MODEL_DIR)
    )
    return executor.generate_sql(query)[-1]


def main() -> None:
    grain_query = QUERY_PATH.read_text(encoding="utf-8")
    bare_query = grain_query.replace(GRAIN_KEY, BARE_KEY)

    bare_sql = generate(bare_query)
    grain_sql = generate(grain_query)

    print("=== leg A: count(ss.item.sk ? ...) ===")
    print(bare_sql)
    print("\n=== leg B: count(grain(ss.item.sk) ? ...) ===")
    print(grain_sql)

    if "is not distinct from" not in bare_sql:
        raise AssertionError("control leg lost the join key; repro is invalid")
    if "on 1=1" not in grain_sql:
        print("\nFIXED: the grain() leg now renders the scoped join key")
        return
    raise AssertionError(
        "REPRODUCED: wrapping the count key in grain() degrades the "
        "union join to on 1=1"
    )


if __name__ == "__main__":
    main()
