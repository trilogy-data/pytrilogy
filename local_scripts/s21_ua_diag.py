import sys
from pathlib import Path

from trilogy import Dialects, Environment, Executor
from trilogy.constants import CONFIG

sys.path.insert(0, "tests")
from test_union_arm_subset_join_full_grain import (  # noqa: E402
    DATES,
    RETURNS,
    SALES,
    SITE,
)

CONFIG.use_v4_discovery = sys.argv[1] == "v4"

import trilogy.core.processing.node_generators.union_select_node as usn  # noqa: E402

_orig = usn.gen_union_select_node


def patched(
    concept,
    local_optional,
    environment,
    g,
    depth,
    source_concepts,
    history,
    conditions=None,
):
    from trilogy.core.query_processor import get_query_node

    lineage = concept.lineage
    print(f"=== union {concept.address} ===")
    for item in lineage.align.items:
        print("  align", item.aligned_concept, "<-", [c.address for c in item.concepts])
    caches = history.build_caches
    prior = list(caches.scoped_joins)
    for i, select in enumerate(lineage.selects):
        node = get_query_node(
            history.base_environment,
            select,
            history,
            scoped_joins=(prior + list(select.scoped_joins)) or None,
        )
        print(f"  arm{i} outputs:", sorted(c.address for c in node.output_concepts))
        print(f"  arm{i} hidden :", sorted(node.hidden_concepts))
    caches.scoped_joins = prior
    return _orig(
        concept,
        local_optional,
        environment,
        g,
        depth,
        source_concepts,
        history,
        conditions,
    )


usn.gen_union_select_node = patched
import trilogy.core.processing.discovery_node_factory as dnf  # noqa: E402

dnf.gen_union_select_node = patched

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
engine.generate_sql(QUERY)
