import tempfile
from pathlib import Path

from trilogy import Dialects
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment
from trilogy.parsing.parse_engine_v2 import clear_parse_cache

A = """key aid int;
property aid.av float;
property aid.aw float;
datasource a (i: aid, v: av, w: aw) grain (aid) address a_tbl;
"""

QUERY = """
import a as a;
with rs as select a.aid as k, a.av as sv order by k desc limit 2;
select a.aid, a.aw, rs.sv
order by a.aid;
"""

QUERY_NOLIMIT = """
import a as a;
with rs as select a.aid as k, a.av as sv;
select a.aid, a.aw, rs.sv
order by a.aid;
"""

QUERY_BAREKEY_JOIN = """
import a as a;
with rs as select a.aid as k, a.av as sv order by k desc limit 2;
select a.aid, rs.sv
subset join rs.k = a.aid
order by a.aid;
"""

for mode in ("v3", "v4"):
    CONFIG.use_v4_discovery = mode == "v4"
    clear_parse_cache()
    tmp = Path(tempfile.mkdtemp())
    (tmp / "a.preql").write_text(A)
    eng = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=tmp))
    eng.execute_raw_sql("create table a_tbl (i int, v double, w double)")
    eng.execute_raw_sql(
        "insert into a_tbl values (1,10,1000),(2,20,2000),(3,30,3000),(4,40,4000)"
    )
    for label, q in (
        ("limit", QUERY),
        ("nolimit", QUERY_NOLIMIT),
        ("barekey_join", QUERY_BAREKEY_JOIN),
    ):
        try:
            rows = [tuple(r) for r in eng.execute_text(q)[0].fetchall()]
            print(mode, label, rows)
        except Exception as e:  # noqa: BLE001
            print(mode, label, "ERROR:", type(e).__name__, str(e)[:200])
