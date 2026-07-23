"""Group diagnostics for the dim_bridge all_subset_unaffected shape."""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, "tests")

from discovery_v4 import _find_select, _materialize_for_query  # noqa: E402
from test_scoped_join_dim_bridge_outer_key import _QUERY, CUSTOMER, SALES  # noqa: E402

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

tmp = Path(tempfile.mkdtemp())
(tmp / "sales.preql").write_text(SALES)
(tmp / "customer.preql").write_text(CUSTOMER)
env = Environment(working_path=tmp)
_, queries = env.parse(_QUERY)
select = _find_select(queries)
history = V4History(base_environment=env)
history.build_caches.scoped_joins = [
    (j.source_address, j.target_address, j.join_type) for j in select.join_clauses
]
build_stmt, build_env, conditions = _materialize_for_query(env, select, history)
print("scoped_join_key_groups:")
for canonical, members in build_env.scoped_join_key_groups.items():
    print("  ", canonical, "->", sorted(members))
info = search_concepts(
    mandatory_list=list(build_stmt.output_components),
    history=history,
    environment=build_env,
    depth=0,
    g=generate_graph(build_env),
    conditions=[conditions] if conditions else [],
)
print("MANDATORY", [c.address for c in build_stmt.output_components])


def _dump(node, indent=0):
    pad = "  " * indent
    print(
        f"{pad}{type(node).__name__}",
        "out:",
        sorted(o.address for o in node.output_concepts),
        "| partial:",
        sorted(p.address for p in node.partial_concepts),
    )
    for p in node.parents:
        _dump(p, indent + 1)


if "tree" in sys.argv:
    info2 = search_concepts(
        mandatory_list=list(build_stmt.output_components),
        history=history,
        environment=build_env,
        depth=0,
        g=generate_graph(build_env),
        conditions=[conditions] if conditions else [],
    )
    _dump(info2.strategy_node)
    sys.exit(0)
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
        "\n   atoms:",
        [str(x) for x in a.condition_atoms],
        "\n   preds:",
        sorted(info.group_graph.predecessors(gid)),
    )
