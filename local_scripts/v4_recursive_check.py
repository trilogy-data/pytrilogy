"""Confirm v4 recursive parity against v3 on the tests/engine recursive model.

The TPC-DS compare harness has no recursive query, so this plans a recursive
query through BOTH v4 (search_concepts + compile_sql) and v3
(executor.generate_sql), executes both on the same duckdb, and compares rows."""

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from discovery_v4 import _materialize_for_query, compile_sql  # noqa: E402

from trilogy import Dialects, Environment  # noqa: E402
from trilogy.core.env_processor import generate_graph  # noqa: E402
from trilogy.core.processing.concept_strategies_v4 import (  # noqa: E402
    V4History,
    search_concepts,
)
from trilogy.core.statements.author import SelectStatement  # noqa: E402

ENGINE_DIR = Path(__file__).resolve().parent.parent / "tests" / "engine"
QUERY = """import recursive;
auto first_parent <- recurse_edge(id, parent);
where first_parent = 1
select id, label
order by id asc;
"""


def main() -> None:
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=ENGINE_DIR)
    )
    _, statements = executor.environment.parse(QUERY)
    select = [q for q in statements if isinstance(q, SelectStatement)][-1]

    v3_sql = executor.generate_sql(QUERY)[-1]

    history = V4History(base_environment=executor.environment)
    build_stmt, build_env, conditions = _materialize_for_query(
        executor.environment, select, history
    )
    info = search_concepts(
        mandatory_list=list(build_stmt.output_components),
        history=history,
        environment=build_env,
        depth=0,
        g=generate_graph(build_env),
        conditions=[conditions] if conditions else [],
    )
    assert info.strategy_node is not None, "v4 produced no strategy node"
    v4_sql = compile_sql(info, build_env, build_stmt)
    assert v4_sql is not None, "v4 produced no SQL"

    from sqlalchemy import text

    con = executor.connection
    v3_rows = Counter(tuple(r) for r in con.execute(text(v3_sql)).fetchall())
    v4_rows = Counter(tuple(r) for r in con.execute(text(v4_sql)).fetchall())
    print(f"v3 rows: {sum(v3_rows.values())}, v4 rows: {sum(v4_rows.values())}")
    if v3_rows == v4_rows:
        print("MATCH: v4 recursive == v3 recursive")
    else:
        print("MISMATCH")
        print("  only v3:", list((v3_rows - v4_rows).items())[:5])
        print("  only v4:", list((v4_rows - v3_rows).items())[:5])
        print("--- v4 SQL ---")
        print(v4_sql)


if __name__ == "__main__":
    main()
