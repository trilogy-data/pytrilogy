"""Group diagnostics for the islanded bare shared-key rowset read (s25).

usage: python local_scripts/s25_groups.py
"""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from discovery_v4 import _find_select, _materialize_for_query  # noqa: E402

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

A = """key aid int;
property aid.av float;
property aid.aw float;
datasource a (i: aid, v: av, w: aw) grain (aid) address a_tbl;
"""

QUERY = """
import a as a;
with rs as select a.aid as k, a.av as sv;
select a.aid, a.aw, rs.sv
order by a.aid;
"""

if "kp" in sys.argv:
    QUERY = """
import a as a;
with rs as select a.aid as k, a.av as sv;
select rs.k, rs.sv, a.aw
order by rs.k;
"""

tmp = Path(tempfile.mkdtemp())
(tmp / "a.preql").write_text(A)
env = Environment(working_path=tmp)
_, queries = env.parse(QUERY)
select = _find_select(queries)
history = V4History(base_environment=env)
if select.join_clauses:
    history.build_caches.scoped_joins = [
        (j.source_address, j.target_address, j.join_type) for j in select.join_clauses
    ]
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
