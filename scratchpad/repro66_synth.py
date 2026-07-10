import tempfile, sys
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
select 2 sid, 1 p, 10 r, 50.0 a union all
select 3 sid, 54 p, 10 r, 30.0 a''';
"""
# q66 shape: constant-key cross join; one rowset projects a raw measure output,
# other projects a raw dim. Select a NON-agg derived expr over the raw output.
BODY = """import sales as s;
rowset wh <- where s.region = 10 select s.region as reg, sum(s.amt) as tot, 1 as jk;
rowset mon <- select s.period as period, 1 as jk;
select {proj} {jt} join wh.jk = mon.jk;
"""
cases = {
  "raw col (control)": "wh.tot, mon.period",
  "derived *2 over raw output": "wh.tot * 2 as r, mon.period",
  "CASE over raw output": "case when wh.tot > 0 then wh.tot else 0 end as r, mon.period",
}
with tempfile.TemporaryDirectory() as tmp:
    d = Path(tmp); (d/"sales.preql").write_text(SALES)
    eng = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=d))
    for jt in ["full", "left", "subset"]:
        print(f"\n## {jt} join")
        for name, proj in cases.items():
            body = BODY.format(proj=proj, jt=jt)
            try:
                eng.generate_sql(body); print(f"  OK    {name}")
            except Exception as e:
                print(f"  FAIL  {name}: {type(e).__name__}: {str(e)[:120]}")
