import sys

from trilogy import Dialects
from trilogy.constants import CONFIG

sys.path.insert(0, "tests")
from test_scoped_left_join_multi_partial_anchor import (  # noqa: E402
    PRESERVING_QUERY,
    RESTRICTED_QUERY,
)

CONFIG.use_v4_discovery = sys.argv[1] == "v4"
if "noopt" in sys.argv:
    for f in CONFIG.optimizations.__dataclass_fields__:
        if isinstance(getattr(CONFIG.optimizations, f), bool):
            setattr(CONFIG.optimizations, f, False)

query = RESTRICTED_QUERY if "restricted" in sys.argv else PRESERVING_QUERY
executor = Dialects.DUCK_DB.default_executor()
print(executor.generate_sql(query)[-1])
print("ROWS:")
for r in executor.execute_text(query)[0].fetchall():
    print("  ", tuple(r))
