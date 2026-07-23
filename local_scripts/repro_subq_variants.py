"""Variant probes around the scalar-global-agg-in-WHERE crash."""

from trilogy import Dialects
from trilogy.constants import CONFIG

CONFIG.use_v4_discovery = True

MODEL = """
key id int;
property id.val int;
property id.cat int;
datasource t (id: id, val: val, cat: cat) grain (id)
  query '''
    select 1 as id, 10 as val, 1 as cat
    union all select 2, 20, 1
    union all select 3, 30, 2
  ''';
"""

VARIANTS = {
    "plain_auto": "auto half <- max(val)/2; select id where val > half order by id asc;",
    "rowset_output": "with rs as select max(val)/2 -> half; select id, rs.half order by id asc;",
    "rowset_where_bare_agg": "with rs as select max(val) -> mx; select id where val = rs.mx;",
}

for name, query in VARIANTS.items():
    engine = Dialects.DUCK_DB.default_executor()
    engine.execute_text(MODEL)
    try:
        rows = engine.execute_text(query)[-1].fetchall()
        print(f"{name}: OK {rows}")
    except Exception as e:  # noqa: BLE001
        msg = str(e).replace("\n", " ")[:160]
        print(f"{name}: FAIL {type(e).__name__}: {msg}")
