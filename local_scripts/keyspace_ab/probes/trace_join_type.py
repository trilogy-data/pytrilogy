"""Trace every plan-time get_join_type call for argv queries on GUEST_ALLDESC
(or a model file given with --model <path>)."""

import importlib.util
import inspect
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

sys.path.insert(0, str(REPO))
from trilogy import Dialects, Environment
from trilogy.core.processing import join_resolution as jr

spec = importlib.util.spec_from_file_location(
    "t",
    str(REPO / "tests" / "engine" / "test_duckdb_rowset_null_group_rejoin.py"),
)
t = importlib.util.module_from_spec(spec)
spec.loader.exec_module(t)

model = (
    t.UNSOLD_MODEL.replace("i_sk: ~item_sk", "i_sk: ~?item_sk")
    .replace(
        "union all select 4, 30, 3'''",
        "union all select 4, 30, 3 union all select 5, null, 7'''",
    )
    .replace("cast(null as varchar)", "'delta'")
)

_orig = jr.get_join_type
_sig = inspect.signature(_orig)


def _short(s: str) -> str:
    s = s.replace("local.", "")
    return s if len(s) < 70 else s[:34] + "…" + s[-34:]


def _side(bound, name, side):
    d = bound.arguments.get(name) or {}
    return sorted(_short(k) for k in d.get(side, []) or [])


def _traced(*args, **kwargs):
    bound = _sig.bind(*args, **kwargs)
    bound.apply_defaults()
    a = bound.arguments
    result = _orig(*args, **kwargs)
    left, right = a["left"], a["right"]
    print(
        f"  {result.name}: keys={sorted(_short(k) for k in a['all_connecting_keys'])}"
    )
    for side, label in ((left, "L"), (right, "R")):
        holders = (a.get("region_holders") or {}).get(side, set())
        print(
            f"    {label} {_short(side)}\n"
            f"       partial={_side(bound, 'partials', side)} nullable={_side(bound, 'nullables', side)}"
            f" value={_side(bound, 'value_nullables', side)} extent={_side(bound, 'extent_nullables', side)}"
            f" holds={sorted(_short(h) for h in holders)} host={side in (a.get('host_nodes') or set())}"
            f" grain={sorted(_short(g) for g in (a.get('node_grains') or {}).get(side, set()))}"
        )
    return result


jr.get_join_type = _traced

env = Environment()
env.parse(model)
ex = Dialects.DUCK_DB.default_executor(environment=env)
for q in [x for x in sys.argv[1:] if not x.startswith("--")]:
    print(f"\n=== {q}")
    try:
        print("  rows:", ex.execute_query(q).fetchall())
    except Exception as e:
        print("  ERROR", type(e).__name__, str(e)[:200])
