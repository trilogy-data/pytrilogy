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

B = """key bid int;
property bid.bv float;
datasource b (i: bid, v: bv) grain (bid) address b_tbl;
"""

QUERY = """
import a as a;
import b as b;
with rs as
where a.aid in b.bid
select a.aid as k, sum(a.av) as sa;


where a.aw is not null and rs.sa is not null
select rs.k, a.aw
subset join a.aid = rs.k
order by rs.k;
"""

tmp = Path(tempfile.mkdtemp())
(tmp / "a.preql").write_text(A)
(tmp / "b.preql").write_text(B)
eng = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=tmp))
eng.execute_raw_sql("create table a_tbl (i int, v double, w double)")
eng.execute_raw_sql("insert into a_tbl values (1,10,NULL),(2,20,2000),(3,30,3000)")
eng.execute_raw_sql("create table b_tbl (i int, v double)")
eng.execute_raw_sql("insert into b_tbl values (1,100),(2,200),(4,400)")
print(eng.generate_sql(QUERY)[-1])
print("ROWS:")
for r in eng.execute_text(QUERY)[0].fetchall():
    print("  ", tuple(r))
print("EXPECTED: (2, 2000.0)")
