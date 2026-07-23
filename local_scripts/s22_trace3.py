import sys
import traceback

from trilogy import Dialects
from trilogy.constants import CONFIG

sys.path.insert(0, "tests/engine")
from test_duckdb_rowset import (  # noqa: E402
    _COMPOSITE_UNION_JOIN_STDDEV_FIXTURE,
    _composite_union_join_query,
)

CONFIG.use_v4_discovery = True
keys = int(sys.argv[1]) if len(sys.argv) > 1 else 3

import trilogy.core.processing.v4_node_generators.dispatch as dispatch  # noqa: E402
from trilogy.core.enums import Derivation  # noqa: E402

_orig = dispatch._GENERATORS[Derivation.ROOT]


def gen(outputs, *a, **kw):
    addrs = sorted(c.address for c in outputs)
    if any("r_filtered" in x for x in addrs):
        print("GEN_ROOT", addrs)
        print("".join(traceback.format_stack()[-7:-1]))
        print("=" * 50)
    return _orig(outputs, *a, **kw)


dispatch._GENERATORS[Derivation.ROOT] = gen

query = _composite_union_join_query("stddev", keys)
executor = Dialects.DUCK_DB.default_executor()
executor.execute_text(_COMPOSITE_UNION_JOIN_STDDEV_FIXTURE)
executor.generate_sql(query)
