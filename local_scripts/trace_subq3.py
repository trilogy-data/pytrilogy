"""Watch the v4 build order for the crash query via logger output."""

import logging

from trilogy import Dialects
from trilogy.constants import CONFIG

logging.basicConfig(level=logging.INFO, format="%(message)s")
for name in list(logging.root.manager.loggerDict):
    if "trilogy" not in name:
        logging.getLogger(name).setLevel(logging.WARNING)

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
try:
    print(engine.execute_text(QUERY)[-1].fetchall())
except Exception as e:  # noqa: BLE001
    print(f"FAIL {type(e).__name__}")
