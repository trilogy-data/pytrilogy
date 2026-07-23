"""Dump the final CTE set (names, outputs, source_map keys) for the exp_rows1 shape."""

import sys
import tempfile
from pathlib import Path

from trilogy import Dialects
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment

SALES = """key sid int;
property sid.period int;
property sid.region int;
property sid.amt float;
datasource sales (sid: sid, p: period, r: region, a: amt) grain (sid)
query '''
select 1 sid, 1 p, 10 r, 100.0 a union all
select 2 sid, 1 p, 10 r, 50.0 a union all
select 3 sid, 54 p, 10 r, 30.0 a union all
select 4 sid, 54 p, 10 r, 7.0 a union all
select 5 sid, 2 p, 10 r, 20.0 a''';
"""
BASE1 = """import sales as s;
rowset agg <- select s.period, sum(s.amt) as tot;
rowset fut <- select s.period, sum(s.amt) as tot;
"""
KEY = sys.argv[2] if len(sys.argv) > 2 else "fut.period + 53 = agg.period"
TAIL = f"select agg.period, sum(agg.tot) / sum(fut.tot) as r subset join {KEY};"

CONFIG.use_v4_discovery = sys.argv[1] == "v4"
for _f in CONFIG.optimizations.__dataclass_fields__:
    if isinstance(getattr(CONFIG.optimizations, _f), bool):
        setattr(CONFIG.optimizations, _f, False)

with tempfile.TemporaryDirectory() as tmp:
    d = Path(tmp)
    (d / "sales.preql").write_text(SALES)
    eng = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=d))
    _, statements = eng.environment.parse(BASE1 + TAIL)
    processed = eng.generator.generate_queries(eng.environment, [statements[-1]])[0]
    for cte in processed.ctes:
        print(f"--- {cte.name} group_to_grain={cte.group_to_grain} grain={cte.grain}")
        print("    outputs:", sorted(c.address for c in cte.output_columns))
        print("    hidden:", sorted(cte.hidden_concepts))
        print(
            "    source_map:", {k: sorted(v) for k, v in sorted(cte.source_map.items())}
        )
