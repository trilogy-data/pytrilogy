import sys

from trilogy import Dialects
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment

sys.path.insert(0, "tests")
from test_scoped_join_cross_rowset_membership_existence import _query  # noqa: E402

CONFIG.use_v4_discovery = sys.argv[1] == "v4"
clause = (
    "subset join ftr_sales.ws - 53 = cur_sales.ws"
    if len(sys.argv) < 3 or sys.argv[2] == "expr"
    else "subset join ftr_sales.ws = cur_sales.ws"
)
q = _query(clause)
exe = Dialects.DUCK_DB.default_executor(environment=Environment())
print(exe.generate_sql(q)[-1])
print("ROWS", exe.execute_text(q)[0].fetchall())
