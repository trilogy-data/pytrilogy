import sys
from pathlib import Path

from trilogy import Dialects
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig

wp = Path("tests/modeling/geography")

QUERY = """
import tree_enrichment;

SELECT native_status, count(tree_id) as tree_count
WHERE city = 'USSFO' and native_status IS NOT NULL
ORDER BY tree_count DESC;"""


def gen(v4: bool):
    CONFIG.use_v4_discovery = v4
    base = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=wp),
        conf=DuckDBConfig(enable_python_datasources=True),
    )
    return base.generate_sql(QUERY)[-1]


mode = sys.argv[1] if len(sys.argv) > 1 else "both"
if mode in ("v3", "both"):
    sql = gen(False)
    print("===== V3 =====")
    print(sql)
    print("full_tree_info present:", "full_tree_info" in sql)
if mode in ("v4", "both"):
    sql = gen(True)
    print("\n===== V4 =====")
    print(sql)
    print("full_tree_info present:", "full_tree_info" in sql)
