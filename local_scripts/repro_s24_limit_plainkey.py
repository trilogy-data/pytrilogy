import sys
import tempfile
from pathlib import Path

from trilogy import Dialects
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment

CONFIG.use_v4_discovery = sys.argv[1] == "v4"
if "noopt" in sys.argv:
    for f in CONFIG.optimizations.__dataclass_fields__:
        if isinstance(getattr(CONFIG.optimizations, f), bool):
            setattr(CONFIG.optimizations, f, False)

A = """key aid int;
property aid.av float;
property aid.aw float;
datasource a (i: aid, v: av, w: aw) grain (aid) address a_tbl;
"""

QUERY = """
import a as a;
with rs as select a.aid as k, a.av as sv order by k desc limit 2;
select a.aid, a.aw, rs.sv
subset join rs.k = a.aid
order by a.aid;
"""

tmp = Path(tempfile.mkdtemp())
(tmp / "a.preql").write_text(A)
eng = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=tmp))
eng.execute_raw_sql("create table a_tbl (i int, v double, w double)")
eng.execute_raw_sql(
    "insert into a_tbl values (1,10,1000),(2,20,2000),(3,30,3000),(4,40,4000)"
)
print(eng.generate_sql(QUERY)[-1])
print("ROWS:")
for r in eng.execute_text(QUERY)[0].fetchall():
    print("  ", tuple(r))
print("EXPECTED: (1,1000,None) (2,2000,None) (3,3000,30) (4,4000,40)")
