import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "local_scripts"))

from proto_ambiguity import (
    JOIN_RESOLUTION_MODEL,
    build_key_graph,
    connector_sets_from,
    load_build_env,
)

benv = load_build_env(None, JOIN_RESOLUTION_MODEL)
kg = build_key_graph(benv)
print("closures:")
for cls, closure in sorted(kg.key_closure.items()):
    print(f"  {cls}: {sorted(closure)}")
print(
    "store<->product alternatives:",
    connector_sets_from(kg, "local.product_id").get("local.store_id"),
)
