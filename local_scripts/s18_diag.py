"""Group/strategy diagnostics for the exp_rows1 shape (arbitrary inline query)."""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from discovery_v4 import (  # noqa: E402
    OUT_DIR,
    _find_select,
    _materialize_for_query,
    write_diagnostics,
)

from trilogy import Environment  # noqa: E402
from trilogy.core.env_processor import generate_graph  # noqa: E402
from trilogy.core.processing.concept_strategies_v4 import (  # noqa: E402
    V4History,
    search_concepts,
)

SALES = """key sid int;
property sid.period int;
property sid.region int;
property sid.amt float;
datasource sales (sid: sid, p: period, r: region, a: amt) grain (sid)
query '''
select 1 sid, 1 p, 10 r, 100.0 a union all
select 2 sid, 1 p, 10 r, 50.0 a union all
select 3 sid, 54 p, 10 r, 30.0 a union all
select 4 sid, 54 p, 10 r, 7.0 a union all
select 5 sid, 2 p, 10 r, 20.0 a''';
"""
BASE1 = """import sales as s;
rowset agg <- select s.period, sum(s.amt) as tot;
rowset fut <- select s.period, sum(s.amt) as tot;
"""

KEY = sys.argv[1] if len(sys.argv) > 1 else "fut.period + 53 = agg.period"
STEM = sys.argv[2] if len(sys.argv) > 2 else "s18_case1"
TAIL = f"select agg.period, sum(agg.tot) / sum(fut.tot) as r subset join {KEY};"

with tempfile.TemporaryDirectory() as tmp:
    d = Path(tmp)
    (d / "sales.preql").write_text(SALES)
    env = Environment(working_path=d)
    _, queries = env.parse(BASE1 + TAIL)
    select = _find_select(queries)
    history = V4History(base_environment=env)
    history.build_caches.scoped_joins = [
        (j.source_address, j.target_address, j.join_type) for j in select.join_clauses
    ]
    build_stmt, build_env, conditions = _materialize_for_query(env, select, history)
    from trilogy.core.query_processor import _authored_reference_addresses

    lineage = select.as_lineage(env)
    build_env.statement_authored_addresses = _authored_reference_addresses(lineage, env)
    build_env.statement_output_addresses = _authored_reference_addresses(
        lineage, env, include_where=False
    )
    info = search_concepts(
        mandatory_list=list(build_stmt.output_components),
        history=history,
        environment=build_env,
        depth=0,
        g=generate_graph(build_env),
        conditions=[conditions] if conditions else [],
    )
    print("SCOPED", build_env.scoped_join_key_groups)
    print("CANONICAL", build_env.domain_graph.canonical_map())
    print("SUBSET_SRC", build_env.domain_graph.subset_sources())
    for a in ("agg.s.period", "fut.s.period"):
        c = build_env.concepts.get(a)
        print(
            "PSEUDO", a, c.address if c else None, sorted(c.pseudonyms) if c else None
        )
    for a, c in build_env.concepts.items():
        if "_virt_func_add" in a:
            print(
                "VIRT",
                a,
                c.address,
                sorted(c.pseudonyms),
                [x.address for x in c.concept_arguments],
            )
    write_diagnostics(info, STEM, OUT_DIR / "v4_diagnostics")
    print("ok", STEM)
