import sys

from trilogy import Dialects
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment

sys.path.insert(0, "tests")
from test_membership_having_aggregate_dimension_key_groupby import QUERY  # noqa: E402

CONFIG.use_v4_discovery = sys.argv[1] == "v4"
exe = Dialects.DUCK_DB.default_executor(environment=Environment())
print(exe.generate_sql(QUERY)[-1])
print("ROWS", exe.execute_text(QUERY)[0].fetchall())
