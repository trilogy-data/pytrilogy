"""Dump the v4 strategy-node tree for a tpc_ds query to find materialization
explosions. Captures the REAL production strategy node via generate_sql.
Usage: python -m local_scripts.v4_dump_tree query02-one.preql"""

import sys
from collections import Counter
from pathlib import Path

from trilogy import Dialects
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig

CONFIG.use_v4_discovery = True

import trilogy.core.query_processor as qp  # noqa: E402

TPCDS = Path(__file__).parent.parent / "tests" / "modeling" / "tpc_ds_duckdb"

_captured = {}
_orig = qp.search_concepts_v4


def _spy(*a, **k):
    info = _orig(*a, **k)
    if info is not None and info.strategy_node is not None:
        _captured.setdefault("node", info.strategy_node)
    return info


qp.search_concepts_v4 = _spy


def walk(node, depth=0, counter=None):
    pad = "  " * depth
    cond = " COND" if getattr(node, "conditions", None) is not None else ""
    outs = sorted(c.address for c in node.output_concepts)
    ds = getattr(node, "datasource", None)
    dsn = f" ds={ds.name}" if ds is not None else ""
    print(f"{pad}{type(node).__name__}{cond}{dsn} [{len(outs)}c]")
    if depth > 50:
        print(f"{pad}  ...truncated...")
        return
    for p in node.parents:
        walk(p, depth + 1, counter)


def main():
    name = sys.argv[1] if len(sys.argv) > 1 else "query02-one.preql"
    env = Environment(working_path=TPCDS)
    eng = Dialects.DUCK_DB.default_executor(environment=env, conf=DuckDBConfig())
    eng.environment = Environment(working_path=TPCDS)
    text = (TPCDS / name).read_text()
    eng.generate_sql(text)
    node = _captured.get("node")
    if node is None:
        print("no strategy node captured")
        return
    counts: Counter = Counter()
    stack = [node]
    visited = set()
    while stack:
        n = stack.pop()
        if id(n) in visited:
            continue
        visited.add(id(n))
        counts[type(n).__name__] += 1
        stack.extend(n.parents)
    print("node type counts (unique):", dict(counts))
    print("total unique nodes:", len(visited))
    print("--- tree ---")
    walk(node)


if __name__ == "__main__":
    main()
