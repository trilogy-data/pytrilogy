"""Group diagnostics for the ROWSET BODY plan of the s20 yoy shape.

usage: python local_scripts/s20_diag.py [nested|plain]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, "tests/discovery")

from discovery_v4 import _find_select  # noqa: E402
from test_filter_node_retains_row_grain_keys import MODEL  # noqa: E402

from trilogy import Environment  # noqa: E402
from trilogy.constants import CONFIG  # noqa: E402
from trilogy.core.env_processor import generate_graph  # noqa: E402
from trilogy.core.models.build import BuildRowsetItem  # noqa: E402
from trilogy.core.processing.concept_strategies_v4 import (  # noqa: E402
    V4History,
    _build_nested_select,
    search_concepts,
)
from trilogy.core.processing.v4_helper.source_policy import (  # noqa: E402
    ROWSET_SOURCE_POLICY,
)

CONFIG.use_v4_discovery = True
for f in CONFIG.optimizations.__dataclass_fields__:
    if isinstance(getattr(CONFIG.optimizations, f), bool):
        setattr(CONFIG.optimizations, f, False)

MODE = sys.argv[1] if len(sys.argv) > 1 else "nested"

ROWSET_Q = """
with st as
select csk as s_csk, cid as s_cid, ss_year as s_yr, sum(ss_net) as st_tot;

select st.s_csk, st.s_cid, st.s_yr, st.st_tot;
"""
PLAIN_Q = "select csk, cid, ss_year, sum(ss_net) as st_tot;"

env = Environment()
env.parse(MODEL)

if MODE == "plain":
    _, queries = env.parse(PLAIN_Q)
    select = _find_select(queries)
    from discovery_v4 import _materialize_for_query

    history = V4History(base_environment=env)
    build_stmt, build_env, conditions = _materialize_for_query(env, select, history)
    info = search_concepts(
        mandatory_list=list(build_stmt.output_components),
        history=history,
        environment=build_env,
        depth=0,
        g=generate_graph(build_env),
        conditions=[conditions] if conditions else [],
    )
else:
    _, queries = env.parse(ROWSET_Q)
    select = _find_select(queries)
    history = V4History(base_environment=env)
    lineage = select.as_lineage(env)
    from trilogy.core.models.build import Factory

    factory = Factory(environment=env)
    built_outer = factory.build(lineage)
    item = next(
        c.lineage
        for c in built_outer.output_components
        if isinstance(c.lineage, BuildRowsetItem)
    )
    inner_select = item.rowset.select
    built, inner_env, inner_where = _build_nested_select(
        inner_select, history, exclude_derived=item.rowset.derived_concepts
    )
    info = search_concepts(
        mandatory_list=list(built.output_components),
        history=history,
        environment=inner_env,
        depth=0,
        g=generate_graph(inner_env),
        source_policy=ROWSET_SOURCE_POLICY,
        conditions=[inner_where] if inner_where else [],
    )
    print("MANDATORY", [c.address for c in built.output_components])
    print("BUILT GRAIN", built.grain)

for gid in sorted(info.group_graph.nodes):
    a = info.group_attrs.get(gid)
    if a is None:
        print(gid, "<no attrs>")
        continue
    print(
        gid,
        "\n   members:",
        sorted(a.members),
        "\n   grain_components:",
        sorted(a.grain_components),
        "\n   outputs:",
        sorted(a.output_concepts),
        "\n   inputs:",
        sorted(a.input_concepts),
        "\n   preds:",
        sorted(info.group_graph.predecessors(gid)),
    )
