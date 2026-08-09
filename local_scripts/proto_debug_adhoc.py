import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "local_scripts"))

from proto_ambiguity import build_key_graph, connector_sets_from

from trilogy import Environment

root = REPO / "tests/modeling/tpc_ds_duckdb"
env = Environment(working_path=root)
env.parse((root / "adhoc01_imports.preql").read_text(encoding="utf-8"))
benv = env.materialize_for_select()
kg = build_key_graph(benv)

name_cls = kg.rep.get("store_sales.store.name")
date_cls = kg.rep.get("store_sales.sale_date.sk")
print("store.name class:", name_cls)
print("sale_date.sk class:", date_cls)
print("closure(name):", sorted(kg.key_closure.get(name_cls, ())))
print("closure(date):", sorted(kg.key_closure.get(date_cls, ())))
concept = benv.concepts.get("store_sales.store.name")
print("concept.keys:", concept.keys if concept else None)
print("concept.grain:", concept.grain.components if concept else None)
if date_cls and name_cls:
    print("alternatives:", connector_sets_from(kg, date_cls).get(name_cls))
