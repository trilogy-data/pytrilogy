import sys

from trilogy import Dialects
from trilogy.constants import CONFIG

sys.path.insert(0, "tests/engine")
from test_duckdb_rowset import (  # noqa: E402
    _COMPOSITE_UNION_JOIN_STDDEV_FIXTURE,
    _composite_union_join_query,
)

CONFIG.use_v4_discovery = sys.argv[1] == "v4"
if "noopt" in sys.argv:
    for f in CONFIG.optimizations.__dataclass_fields__:
        if isinstance(getattr(CONFIG.optimizations, f), bool):
            setattr(CONFIG.optimizations, f, False)

keys = 3
for a in sys.argv[2:]:
    if a.isdigit():
        keys = int(a)

agg = "stddev"
query = _composite_union_join_query(agg, keys)
executor = Dialects.DUCK_DB.default_executor()
executor.execute_text(_COMPOSITE_UNION_JOIN_STDDEV_FIXTURE)
print(query)
print(executor.generate_sql(query)[-1])
print("ROWS:")
for r in executor.execute_text(query)[0].fetchall():
    print("  ", tuple(r))
