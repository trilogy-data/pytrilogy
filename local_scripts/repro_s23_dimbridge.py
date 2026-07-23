import sys
import tempfile
from pathlib import Path

from trilogy import Dialects
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment

sys.path.insert(0, "tests")
from test_scoped_join_dim_bridge_outer_key import _QUERY, CUSTOMER, SALES  # noqa: E402

CONFIG.use_v4_discovery = sys.argv[1] == "v4"
if "noopt" in sys.argv:
    for f in CONFIG.optimizations.__dataclass_fields__:
        if isinstance(getattr(CONFIG.optimizations, f), bool):
            setattr(CONFIG.optimizations, f, False)

tmp = Path(tempfile.mkdtemp())
(tmp / "sales.preql").write_text(SALES)
(tmp / "customer.preql").write_text(CUSTOMER)
executor = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=tmp))
print(executor.generate_sql(_QUERY)[-1])
print("ROWS:")
for r in executor.execute_text(_QUERY)[0].fetchall():
    print("  ", tuple(r))
