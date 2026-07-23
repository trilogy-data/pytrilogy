import sys

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG

sys.path.insert(0, "tests/discovery")
from test_filter_node_retains_row_grain_keys import MODEL, QUERY  # noqa: E402

CONFIG.use_v4_discovery = sys.argv[1] == "v4"
if len(sys.argv) > 2 and sys.argv[2] == "noopt":
    for f in CONFIG.optimizations.__dataclass_fields__:
        if isinstance(getattr(CONFIG.optimizations, f), bool):
            setattr(CONFIG.optimizations, f, False)

env = Environment()
env.parse(MODEL)
exe = Dialects.DUCK_DB.default_executor(environment=env)
sql = exe.generate_sql(QUERY)[-1]
print(sql)
print("---- ROWS ----")
for r in exe.execute_query(QUERY).fetchall():
    print(tuple(r))
