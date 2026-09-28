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
if "--two" in sys.argv:
    model = t.TWO_PROP_GUEST_ALLDESC_MODEL

_orig = jr.get_join_type
_sig = inspect.signature(_orig)


def _short(s: str) -> str:
    s = s.replace("local.", "")
    return s if len(s) < 70 else s[:34] + "…" + s[-34:]


def _keys(values) -> list[str]:
    return sorted(_short(k) for k in values)


def _traced(*args, **kwargs):
    bound = _sig.bind(*args, **kwargs)
    bound.apply_defaults()
    a = bound.arguments
    result = _orig(*args, **kwargs)
    facts = a["facts"]
    print(
        f"  {result.name}: keys={sorted(_short(k) for k in a['all_connecting_keys'])}"
    )
    for side, label in ((a["left"], "L"), (a["right"], "R")):
        f = facts.side(side)
        print(
            f"    {label} {_short(side)}\n"
            f"       partial={_keys(f.partials)} nullable={_keys(f.nullables)}"
            f" value={_keys(f.value_nullables)} extent={_keys(f.extent_nullables)}"
            f" holds={_keys(f.held_spans)} host={f.hosts}"
            f" grain={_keys(f.grain)}"
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
