import sys
sys.path.insert(0, "local_scripts")
from pathlib import Path
from trilogy import Environment
from discovery_v4 import _materialize_for_query, _find_select
from trilogy.core.processing.concept_strategies_v4 import History
from trilogy.core.processing.v4_helper.concept_graph import build_concept_graph
from trilogy.core.processing.v4_helper.group_graph import build_group_graph

TPCDS = Path(__file__).resolve().parent.parent / "tests" / "modeling" / "tpc_ds_duckdb"
text = (TPCDS / "query02.preql").read_text()
env = Environment(working_path=TPCDS)
_, queries = env.parse(text)
select = _find_select(queries)
history = History(base_environment=env)
build_stmt, build_env, conditions = _materialize_for_query(env, select, history)

mandatory = list(build_stmt.output_components)
print("MANDATORY:", [c.address for c in mandatory])

for c in mandatory:
    grain = list(c.grain.components) if c.grain else []
    print(f"  {c.address}  derivation={c.derivation}  grain={grain}")
si = build_env.concepts["local.sunday_increase"]
print(f"\nsunday_increase lineage type: {type(si.lineage).__name__}")
print(f"sunday_increase grain: {list(si.grain.components) if si.grain else None}")
print(f"sunday_increase lineage concept_arguments: {[c.address for c in si.lineage.concept_arguments] if si.lineage else None}")
for a in si.lineage.concept_arguments:
    ac = build_env.concepts.get(a.address, a)
    print(f"  arg {ac.address} derivation={ac.derivation} grain={list(ac.grain.components) if ac.grain else None}")
    if ac.lineage:
        print(f"    -> lineage args: {[x.address for x in ac.lineage.concept_arguments]}")
print()
cg = build_concept_graph(mandatory, build_env, [conditions] if conditions else [])
gg = build_group_graph(cg, [conditions] if conditions else [], mandatory)
print("=== GROUPS ===")
for n in gg.nodes:
    d = gg.nodes[n]
    print(f"  {n}")
    print(f"    derivation={d.get('derivation')} depth={d.get('depth_label')} grain={sorted(d.get('grain_components', []))}")
    print(f"    primary  ={d.get('primary_members', ())}")
    print(f"    secondary={d.get('secondary_members', ())}")
    print(f"    output   ={d.get('output_concepts', ())}")
    print(f"    hidden   ={d.get('hidden_concepts', ())}")
    print(f"    input    ={d.get('input_concepts', ())}")
    if d.get('conditions'):
        print(f"    conditions={d.get('conditions')}")
print()
print("=== EDGES ===")
for u, v, ed in gg.edges(data=True):
    print(f"  {u} -> {v}  kind={ed.get('kind')}")
