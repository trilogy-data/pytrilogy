import sys
from pathlib import Path

from trilogy import Dialects, Environment, Executor
from trilogy.constants import CONFIG

sys.path.insert(0, "tests")
from test_union_arm_subset_join_full_grain import DATES, RETURNS, SALES, SITE  # noqa

CONFIG.use_v4_discovery = True

import trilogy.core.processing.concept_strategies_v4 as cs  # noqa: E402

_orig = cs._resolve_union_select


def patched(union_concept, mandatory_list, environment, depth, g, history, conditions):
    lineage = union_concept.lineage
    from trilogy.core.env_processor import generate_graph
    from trilogy.core.processing.concept_strategies_v4 import (
        _build_nested_select,
        search_concepts,
    )

    print(f"=== union {union_concept.address} ===")
    for item in lineage.align.items:
        print("  align", item.aligned_concept, "<-", [c.address for c in item.concepts])
    for i, arm in enumerate(lineage.selects):
        built_arm, arm_env, arm_where = _build_nested_select(arm, history)
        print(
            f"  arm{i} output_components:",
            sorted(c.address for c in built_arm.output_components),
        )
        arm_info = search_concepts(
            mandatory_list=list(built_arm.output_components),
            history=history,
            environment=arm_env,
            depth=depth + 1,
            g=generate_graph(arm_env),
            conditions=[arm_where] if arm_where else [],
        )
        n = arm_info.strategy_node
        if n is None:
            print(f"  arm{i} DID NOT RESOLVE")
            continue
        print(f"  arm{i} node outputs:", sorted(c.address for c in n.output_concepts))
        for out in list(n.output_concepts):
            mn = lineage.get_merge_concept(out)
            if mn:
                print(f"     merge {out.address} -> {mn}")
    return _orig(
        union_concept, mandatory_list, environment, depth, g, history, conditions
    )


cs._resolve_union_select = patched

QUERY = """
import sales as ws;
import returns as wr;

with combined as union(
    (where wr.date_dim.date between '2001-01-01'::date and '2001-12-31'::date
     select wr.order_number, 0.0),
    (where wr.date_dim.date between '2000-01-01'::date and '2000-12-31'::date
     select wr.order_number, coalesce(sum(wr.return_amt),0.0)
     subset join wr.item = ws.item
     subset join wr.order_number = ws.order_number)
) -> (eid, ret);
select combined.eid, sum(combined.ret) as total;
"""

tmp = Path("local_scripts/_s21_ua_model")
tmp.mkdir(exist_ok=True)
(tmp / "site.preql").write_text(SITE)
(tmp / "dates.preql").write_text(DATES)
(tmp / "sales.preql").write_text(SALES)
(tmp / "returns.preql").write_text(RETURNS)

engine: Executor = Dialects.DUCK_DB.default_executor(
    environment=Environment(working_path=tmp)
)
try:
    engine.generate_sql(QUERY)
except Exception as e:
    print("ERR", type(e).__name__, str(e)[:200])
