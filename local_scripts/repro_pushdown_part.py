"""Repro for test_pushdown_partitioned_aggregate_boundary v4 failures."""

import sys

from trilogy import Dialects
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment

BASE = """
key entity int;
property entity.segment string;
property entity.part string;
property entity.amount float;

datasource rows (e: entity, s: segment, p: part, a: amount) grain (entity)
query '''
select 1 e, 'keep' s, 'A' p, 100.0 a union all
select 2 e, 'keep' s, 'A' p, 100.0 a union all
select 3 e, 'other' s, 'A' p, 0.0 a union all
select 4 e, 'other' s, 'A' p, 0.0 a''';
"""

WHERE_AND_AGG_REF = BASE + """
auto part_avg <- avg(amount) by part;

where segment = 'keep' and part_avg > 60
select
    entity,
    rank(entity) over (order by part_avg asc, entity asc) as rnk
order by entity asc;
"""

WHERE_AND_BOUND_AGG = BASE + """
auto part_avg <- avg(amount ? segment = 'keep') by part;

where segment = 'keep' and part_avg > 60
select
    entity,
    rank(entity) over (order by part_avg asc, entity asc) as rnk
order by entity asc;
"""


def run(name: str, query: str, use_v4: bool) -> None:
    CONFIG.use_v4_discovery = use_v4
    label = "v4" if use_v4 else "v3"
    exec_ = Dialects.DUCK_DB.default_executor(environment=Environment())
    try:
        sql = exec_.generate_sql(query)[-1]
        print(f"=== {name} [{label}] SQL ===")
        print(sql)
        rows = [tuple(r) for r in exec_.execute_text(query)[0].fetchall()]
        print(f"=== {name} [{label}] rows: {rows}")
    except Exception as e:
        print(f"=== {name} [{label}] EXCEPTION: {type(e).__name__}: {e}")


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "both"
    if which in ("both", "agg_ref"):
        run("agg_ref", WHERE_AND_AGG_REF, use_v4=False)
        run("agg_ref", WHERE_AND_AGG_REF, use_v4=True)
    if which in ("both", "bound_agg"):
        run("bound_agg", WHERE_AND_BOUND_AGG, use_v4=False)
        run("bound_agg", WHERE_AND_BOUND_AGG, use_v4=True)
