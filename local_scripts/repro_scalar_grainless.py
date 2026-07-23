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

for q in [
    "with rs as select id where val >= 20; select 2 in (rs.id) as present;",
    "with rs as select id where val >= 20; select 2 in (select rs.id) as present;",
]:
    engine = Dialects.DUCK_DB.default_executor()
    engine.execute_text(MODEL)
    try:
        print(f"OK {engine.execute_text(q)[-1].fetchall()}")
    except Exception as e:
        print(f"FAIL {type(e).__name__}: {str(e)[:200]}")
