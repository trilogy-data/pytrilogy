"""Trace the FINAL dedup decision (_group_to_grain_if_required) for argv
queries on the GUEST model: the node, its resolved grain, its outputs (hidden
marked), its parents' grains, and check_if_group_required's verdict."""

import importlib.util
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

sys.path.insert(0, str(REPO))
from trilogy import Dialects, Environment
from trilogy.core.processing.discovery_utility import check_if_group_required
from trilogy.core.processing.v4_helper import strategy_builder as sb

spec = importlib.util.spec_from_file_location(
    "t",
    str(REPO / "tests" / "engine" / "test_duckdb_rowset_null_group_rejoin.py"),
)
t = importlib.util.module_from_spec(spec)
spec.loader.exec_module(t)

model = t.UNSOLD_MODEL.replace("i_sk: ~item_sk", "i_sk: ~?item_sk").replace(
    "union all select 4, 30, 3'''",
    "union all select 4, 30, 3 union all select 5, null, 7'''",
)

_orig = sb._group_to_grain_if_required


def _desc(node):
    qds = node.resolve()
    outs = [
        (
            o.address.replace("local.", "")
            + ("(h)" if o.address in node.hidden_concepts else "")
        )
        for o in node.output_concepts
    ]
    return (
        f"{type(node).__name__} grain={sorted(str(c).replace('local.', '') for c in qds.grain.components)}"
        f" outs={outs} force_group={node.force_group}"
    )


def _traced(node, mandatory_list, final_contract, environment):
    print("  _group_to_grain_if_required on:", _desc(node))
    for p in node.parents:
        print("     parent:", _desc(p))
    contract_outputs = [
        c for c in mandatory_list if c.address in final_contract.output_addresses
    ]
    verdict = check_if_group_required(
        downstream_concepts=contract_outputs,
        parents=[node.resolve()],
        environment=environment,
    )
    print(
        f"  deduplicate_to_grain={final_contract.deduplicate_to_grain} verdict={verdict}"
    )
    out = _orig(node, mandatory_list, final_contract, environment)
    print("  ->", _desc(out))
    return out


sb._group_to_grain_if_required = _traced

from trilogy.core.processing import grain_utility as gu

_orig_omit = gu._join_right_grain_can_be_omitted


def _traced_omit(join, grain, environment):
    result = _orig_omit(join, grain, environment)
    pc = gu._join_right_preserves_cardinality(join, environment)
    lc = gu._join_left_keys_covered_by_grain(join, grain, environment)
    right = join.right_datasource
    keys = (
        [(p.left.address, p.right.address) for p in join.concept_pairs]
        if getattr(join, "concept_pairs", None)
        else [c.address for c in (getattr(join, "concepts", None) or [])]
    )
    print(
        f"  omit_right? {result} (cardinality={pc}, left_keys_covered={lc}) join={join.join_type.name}"
        f" keys={keys} right_grain={sorted(str(c) for c in right.effective_grain.components)}"
        f" target_grain={sorted(str(c) for c in grain.components)} right={right.identifier[:60]}"
    )
    return result


gu._join_right_grain_can_be_omitted = _traced_omit

env = Environment()
env.parse(model)
ex = Dialects.DUCK_DB.default_executor(environment=env)
for q in [a for a in sys.argv[1:] if not a.startswith("--")]:
    print(f"\n=== {q}")
    print("  rows:", ex.execute_query(q).fetchall())
