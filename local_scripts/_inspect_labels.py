import sys

sys.path.insert(0, "local_scripts")
from collections import Counter
from pathlib import Path

from discovery_v4 import _find_select, _materialize_for_query

from trilogy import Environment
from trilogy.core.processing.concept_strategies_v4 import History
from trilogy.core.processing.v4_helper.concept_graph import build_concept_graph

TPCDS = Path(__file__).resolve().parent.parent / "tests" / "modeling" / "tpc_ds_duckdb"
qname = sys.argv[1] if len(sys.argv) > 1 else "5"
text = (TPCDS / f"query{int(qname):02d}.preql").read_text()
env = Environment(working_path=TPCDS)
_, queries = env.parse(text)
select = _find_select(queries)
history = History(base_environment=env)
build_stmt, build_env, conditions = _materialize_for_query(env, select, history)
mandatory = list(build_stmt.output_components)
cg = build_concept_graph(mandatory, build_env, [conditions] if conditions else [])
print(f"Total nodes: {len(cg.nodes)}")
labels = Counter(d.get("label", "") for _, d in cg.nodes(data=True))
print(f"By label: {dict(labels)}")
print()
for label in sorted(labels.keys()):
    print(f"=== label='{label}' ===")
    for nid, data in sorted(cg.nodes(data=True)):
        if data.get("label", "") != label:
            continue
        print(
            f"  {nid}  addr={data.get('address')}  deriv={data.get('derivation')}  depth={data.get('depth_label')}  grain={sorted(data.get('grain_components', []))}"
        )
