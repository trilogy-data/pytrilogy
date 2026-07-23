"""three_key subset join under `by rollup`: v4 emits ungrouped all_ch_* columns."""

import sys

from trilogy import Dialects
from trilogy.constants import CONFIG

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))
from tests.engine.test_duckdb_rollup_scoped_join import (  # noqa: E402
    CANONICAL_TUPLE_KEY,
    FIXTURE,
    SUBSET_JOIN_ROLLUP,
)

CONFIG.use_v4_discovery = sys.argv[1] == "v4"
QUERY = (
    CANONICAL_TUPLE_KEY
    if len(sys.argv) > 2 and sys.argv[2] == "canon"
    else SUBSET_JOIN_ROLLUP
)
if len(sys.argv) > 3 and sys.argv[3] == "noopt":
    for _f in CONFIG.optimizations.__dataclass_fields__:
        if isinstance(getattr(CONFIG.optimizations, _f), bool):
            setattr(CONFIG.optimizations, _f, False)

eng = Dialects.DUCK_DB.default_executor()
eng.execute_text(FIXTURE)
print(eng.generate_sql(QUERY)[-1])
try:
    rows = sorted(
        (tuple(r) for r in eng.execute_text(QUERY)[0].fetchall()),
        key=lambda t: tuple((x is None, str(x)) for x in t),
    )
    print("NROWS", len(rows))
    for r in rows:
        print(" ", r)
except Exception as e:  # noqa: BLE001
    print("EXEC FAILED:", str(e)[:300])
