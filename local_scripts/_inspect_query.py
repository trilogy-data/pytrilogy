import sys
sys.path.insert(0, "local_scripts")
from pathlib import Path
from trilogy import Environment
from discovery_v4 import _materialize_for_query, _find_select
from trilogy.core.processing.concept_strategies_v4 import History
from trilogy.core.processing.v4_helper.concept_graph import build_concept_graph
from trilogy.core.processing.v4_helper.group_graph import build_group_graph

TPCDS = Path(__file__).resolve().parent.parent / "tests" / "modeling" / "tpc_ds_duckdb"
qname = sys.argv[1] if len(sys.argv) > 1 else "47"
text = (TPCDS / f"query{int(qname):02d}.preql").read_text()
env = Environment(working_path=TPCDS)
_, queries = env.parse(text)
select = _find_select(queries)
history = History(base_environment=env)
build_stmt, build_env, conditions = _materialize_for_query(env, select, history)

mandatory = list(build_stmt.output_components)
cg = build_concept_graph(mandatory, build_env, [conditions] if conditions else [])
gg, attrs = build_group_graph(cg, [conditions] if conditions else [], mandatory)
print("=== GROUPS ===")
for n in gg.nodes:
    a = attrs[n]
    print(f"  {n}")
    print(f"    derivation={a.derivation} depth={a.depth_label}")
    print(f"    grain      ={sorted(a.grain_components)}")
    print(f"    primary    ={a.primary_members}")
    print(f"    secondary  ={a.secondary_members}")
    print(f"    output     ={a.output_concepts}")
    print(f"    input      ={a.input_concepts}")
    if a.conditions:
        print(f"    conditions ={a.conditions}")
print()
print("=== EDGES ===")
for u, v, ed in gg.edges(data=True):
    print(f"  {u} -> {v}  kind={ed.get('kind')}")
