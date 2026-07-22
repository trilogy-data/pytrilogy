import sys

from trilogy import Dialects
from trilogy.constants import CONFIG

sys.path.insert(0, "tests")
from test_scoped_left_join_multi_partial_anchor import MODEL, ROWSETS  # noqa: E402

CONFIG.use_v4_discovery = sys.argv[1] == "v4"

QUERY = MODEL + ROWSETS + """
select
    store_nr.cust_id,
    store_nr.store_qty,
    coalesce(web_nr.web_qty, 0) as other_qty
subset join web_nr.cust_id = store_nr.cust_id
order by store_nr.cust_id;
"""

executor = Dialects.DUCK_DB.default_executor()
if "sql" in sys.argv:
    print(executor.generate_sql(QUERY)[-1])
print("ROWS:")
for r in executor.execute_text(QUERY)[0].fetchall():
    print("  ", tuple(r))
