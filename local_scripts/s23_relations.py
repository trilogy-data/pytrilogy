import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, "tests")

from discovery_v4 import _find_select, _materialize_for_query  # noqa: E402
from test_scoped_left_join_multi_partial_anchor import PRESERVING_QUERY  # noqa: E402

from trilogy import Environment  # noqa: E402
from trilogy.constants import CONFIG  # noqa: E402
from trilogy.core.processing.concept_strategies_v4 import V4History  # noqa: E402

CONFIG.use_v4_discovery = True

env = Environment()
_, queries = env.parse(PRESERVING_QUERY)
select = _find_select(queries)
history = V4History(base_environment=env)
history.build_caches.scoped_joins = [
    (j.source_address, j.target_address, j.join_type) for j in select.join_clauses
]
build_stmt, build_env, conditions = _materialize_for_query(env, select, history)
print("scoped_join_key_groups:")
for canonical, members in build_env.scoped_join_key_groups.items():
    print("  ", canonical, "->", sorted(members))
print("domain_graph edges:")
for e in build_env.domain_graph.edges:
    print("  ", e.source, "->", e.target, e.scope, getattr(e, "kind", None))
print("other_qty concept:")
c = build_env.concepts["local.other_qty"]
print("   grain:", sorted(c.grain.components) if c.grain else None)
print("   derivation:", c.derivation)
print("   lineage:", type(c.lineage).__name__)
