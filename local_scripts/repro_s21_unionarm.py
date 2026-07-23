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
if "noopt" in sys.argv:
    for f in CONFIG.optimizations.__dataclass_fields__:
        if isinstance(getattr(CONFIG.optimizations, f), bool):
            setattr(CONFIG.optimizations, f, False)

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
sql = engine.generate_sql(QUERY)[-1]
print(sql)
print("---- rows ----")
for r in engine.execute_raw_sql(sql).fetchall():
    print(tuple(r))
