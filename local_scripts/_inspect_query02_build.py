import sys

sys.path.insert(0, "local_scripts")
from pathlib import Path

from discovery_v4 import (
    run_tpcds_query,
)

TPCDS = Path(__file__).resolve().parent.parent / "tests" / "modeling" / "tpc_ds_duckdb"

info, build_env, _, build_stmt = run_tpcds_query("02")


def walk(node, depth=0):
    indent = "  " * depth
    name = type(node).__name__
    outs = sorted(o.address for o in node.output_concepts)
    conds = getattr(node, "conditions", None)
    ex = getattr(node, "existence_concepts", None)
    print(f"{indent}{name}")
    print(f"{indent}  outputs: {outs}")
    if conds:
        print(f"{indent}  conditions: {conds}")
    if ex:
        print(f"{indent}  existence: {[c.address for c in ex]}")
    for p in node.parents:
        walk(p, depth + 1)


print("=== STRATEGY NODE TREE ===")
walk(info.strategy_node)
