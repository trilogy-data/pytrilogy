"""Repro: grainless constant-LHS tuple membership (duckdb_subquery ×4)."""

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

QUERY = (
    "with pairs as select val, cat where val = 20;"
    "select (20, 1) in (pairs.val, pairs.cat) as present,"
    " (20, 2) in (pairs.val, pairs.cat) as cross_pair_absent;"
)

engine = Dialects.DUCK_DB.default_executor()
engine.execute_text(MODEL)
try:
    print(engine.generate_sql(QUERY)[-1])
    print(engine.execute_text(QUERY)[-1].fetchall())
except Exception as e:  # noqa: BLE001
    print(f"FAIL {type(e).__name__}: {str(e)[:400]}")
