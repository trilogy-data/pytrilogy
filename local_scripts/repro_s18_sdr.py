"""exp_rows1: `subset join fut.period + 53 = agg.period` renders FULL under v4."""

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
if len(sys.argv) > 3 and sys.argv[3] == "noopt":
    for _f in CONFIG.optimizations.__dataclass_fields__:
        if isinstance(getattr(CONFIG.optimizations, _f), bool):
            setattr(CONFIG.optimizations, _f, False)

with tempfile.TemporaryDirectory() as tmp:
    d = Path(tmp)
    (d / "sales.preql").write_text(SALES)
    eng = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=d))
    sql = eng.generate_sql(BASE1 + TAIL)[-1]
    print(sql)
    rows = sorted(
        (tuple(r) for r in eng.execute_text(BASE1 + TAIL)[0].fetchall()),
        key=lambda x: (x[0] is None, x[0]),
    )
    print("ROWS", rows)
