import tempfile
from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment

SALES = """key sid int;
property sid.period int;
property sid.region int;
property sid.amt float;
datasource sales (sid: sid, p: period, r: region, a: amt) grain (sid)
query '''
select 1 sid, 1 p, 10 r, 100.0 a union all
select 2 sid, 2 p, 20 r, 50.0 a''';
"""
BODY = """import sales as s;
rowset wh <- where s.region > 0 select s.region as reg, 1 as jk;
rowset mon <- select s.period as period, 1 as jk;
select {proj} full join wh.jk = mon.jk order by mon.period;
"""
with tempfile.TemporaryDirectory() as tmp:
    d = Path(tmp); (d/"sales.preql").write_text(SALES)
    eng = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=d))
    # control raw dim
    print("raw:", eng.generate_sql(BODY.format(proj="wh.reg, mon.period"))[-1][:0] == "")
    body = BODY.format(proj="wh.reg * 2 as r, mon.period")
    rows = sorted(tuple(r) for r in eng.execute_text(body)[0].fetchall())
    print("derived-over-raw-dim rows:", rows)
