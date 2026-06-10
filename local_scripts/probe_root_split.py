from __future__ import annotations

import traceback

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG

# (a) region as an ENUM with closed domain {east, west}: coverage IS provable.
ENUM_PARTIAL = """
key customer_id int;
property customer_id.region enum<string>['east','west'];
property customer_id.revenue float;
auto total_revenue <- sum(revenue);

datasource east_summary (customer_id: ~customer_id, region: region, total_revenue: total_revenue)
grain (customer_id) complete where region = 'east'
query '''select 101 as customer_id, 'east' as region, 10.0 as total_revenue''';

datasource west_summary (customer_id: ~customer_id, region: region, total_revenue: total_revenue)
grain (customer_id) complete where region = 'west'
query '''select 202 as customer_id, 'west' as region, 20.0 as total_revenue''';

SELECT customer_id, total_revenue ORDER BY customer_id;
"""

# (b) date-partitioned (provably exhaustive: <= X OR > X), but NO ~ partial marker
# on the key — only `complete where`. Does v3 still silently drop a partition?
DATE_NO_TILDE = """
key customer_id int;
property customer_id.signup date;
property customer_id.revenue float;

datasource early (customer_id: customer_id, signup: signup, revenue: revenue)
grain (customer_id) complete where signup <= '2024-01-01'::date
query '''select 101 as customer_id, date '2023-06-01' as signup, 10.0 as revenue''';

datasource late (customer_id: customer_id, signup: signup, revenue: revenue)
grain (customer_id) complete where signup > '2024-01-01'::date
query '''select 202 as customer_id, date '2024-06-01' as signup, 20.0 as revenue''';

SELECT customer_id, revenue ORDER BY customer_id;
"""

# (c) date-partitioned WITH ~ partial marker (mirrors test_dataset_merge).
DATE_TILDE = DATE_NO_TILDE.replace(
    "(customer_id: customer_id,", "(customer_id: ~customer_id,"
)


def run(model: str, v4: bool):
    CONFIG.use_v4_discovery = v4
    try:
        env = Environment()
        ex = Dialects.DUCK_DB.default_executor(environment=env)
        sql = ex.generate_sql(model)[-1]
        cur = ex.execute_raw_sql(sql)
        rows = [tuple(r) for r in cur.fetchall()]
        print(f"  v4={v4} rows={rows}")
    except Exception:
        print(f"  v4={v4} ERROR: {traceback.format_exc().splitlines()[-1][:130]}")
    finally:
        CONFIG.use_v4_discovery = False


if __name__ == "__main__":
    for label, model in [
        ("ENUM_PARTIAL", ENUM_PARTIAL),
        ("DATE_NO_TILDE", DATE_NO_TILDE),
        ("DATE_TILDE", DATE_TILDE),
    ]:
        print(f"===== {label} =====")
        run(model, False)
        run(model, True)
