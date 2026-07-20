"""Repro: v4 scalar-subquery-in-WHERE crash (audit s11 NEXT)."""

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

QUERY = "select id where val > (select max(val)/2 -> half) order by id asc;"

engine = Dialects.DUCK_DB.default_executor()
engine.execute_text(MODEL)
print(engine.generate_sql(QUERY)[-1])
print(engine.execute_text(QUERY)[-1].fetchall())
