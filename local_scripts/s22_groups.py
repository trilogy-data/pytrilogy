"""Group diagnostics for the composite union-join keys-3 shape.

usage: python local_scripts/s22_groups.py [keys]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, "tests/engine")

from discovery_v4 import _find_select, _materialize_for_query  # noqa: E402
from test_duckdb_rowset import (  # noqa: E402
    _COMPOSITE_UNION_JOIN_STDDEV_FIXTURE,
    _composite_union_join_query,
)

from trilogy import Environment  # noqa: E402
from trilogy.constants import CONFIG  # noqa: E402
from trilogy.core.env_processor import generate_graph  # noqa: E402
from trilogy.core.processing.concept_strategies_v4 import (  # noqa: E402
    V4History,
    search_concepts,
)

CONFIG.use_v4_discovery = True
for f in CONFIG.optimizations.__dataclass_fields__:
    if isinstance(getattr(CONFIG.optimizations, f), bool):
        setattr(CONFIG.optimizations, f, False)

keys = int(sys.argv[1]) if len(sys.argv) > 1 else 3

env = Environment()
env.parse(_COMPOSITE_UNION_JOIN_STDDEV_FIXTURE)
_, queries = env.parse(_composite_union_join_query("stddev", keys))
select = _find_select(queries)
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
print("MANDATORY", [c.address for c in build_stmt.output_components])
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
