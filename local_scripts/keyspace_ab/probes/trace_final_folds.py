"""Wrap every FINAL-assembly pass in strategy_builder that takes and returns
the `parents` list, printing each contributor's outputs before and after, for
argv queries on UNSOLD_MODEL (--guest / --two as in trace_buckets.py)."""

import importlib.util
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

sys.path.insert(0, str(REPO))
from trilogy import Dialects, Environment
from trilogy.core.processing.v4_helper import strategy_builder as sb

spec = importlib.util.spec_from_file_location(
    "t",
    str(REPO / "tests" / "engine" / "test_duckdb_rowset_null_group_rejoin.py"),
)
t = importlib.util.module_from_spec(spec)
spec.loader.exec_module(t)

model = t.UNSOLD_MODEL
if "--guest" in sys.argv:
    model = t.GUEST_ALLDESC_MODEL
if "--two" in sys.argv:
    model = t.TWO_PROP_GUEST_ALLDESC_MODEL

PASSES = [
    "_fold_constant_parents",
    "_satisfy_parent_projection_contract",
    "_fold_passthrough_parents",
    "_fold_covered_contributors",
    "_bridge_unpaired_parents",
]


def _desc(parents) -> list[str]:
    return [
        f"{type(p).__name__}[{','.join(sorted(c.address.replace('local.', '') for c in p.output_concepts))}]"
        for p in parents
    ]


def _wrap(name: str):
    orig = getattr(sb, name)

    def traced(parents, *args, **kwargs):
        before = _desc(parents)
        out = orig(parents, *args, **kwargs)
        after = _desc(out)
        flag = "" if before == after else "  <-- CHANGED"
        print(f"  {name}{flag}\n     in : {before}\n     out: {after}")
        return out

    setattr(sb, name, traced)


for name in PASSES:
    _wrap(name)

_orig_widen = sb._widen_merge_join_keys
_orig_carry = sb._carry_join_keys


def _traced_widen(parents, environment, join_key_addresses):
    print(
        f"  _widen_merge_join_keys grain={sorted(a.replace('local.', '') for a in join_key_addresses)}"
        f"\n     in : {_desc(parents)}"
    )
    _orig_widen(parents, environment, join_key_addresses)
    print(f"     out: {_desc(parents)}")


def _traced_carry(parents, join_key_concepts, environment):
    print(
        f"     _carry_join_keys keys={sorted(c.address.replace('local.', '') for c in join_key_concepts)}"
    )
    return _orig_carry(parents, join_key_concepts, environment)


sb._widen_merge_join_keys = _traced_widen
sb._carry_join_keys = _traced_carry

env = Environment()
env.parse(model)
ex = Dialects.DUCK_DB.default_executor(environment=env)
for q in [x for x in sys.argv[1:] if not x.startswith("--")]:
    print(f"\n=== {q}")
    try:
        print("  rows:", ex.execute_query(q).fetchall())
    except Exception as e:
        print("  ERROR", type(e).__name__, str(e)[:300])
