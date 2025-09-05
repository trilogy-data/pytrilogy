from pathlib import Path

from trilogy import Dialects, Environment

base = Path(__file__).parent


def test_studio_join_issue():
    env = Environment(working_path=base)
    env, queries = env.parse(
        """import orders as orders;

auto cancelled_orders <- filter orders.id where orders.status = 'Cancelled';
auto orders.id.cancelled_count <- count(cancelled_orders);

WHERE
    orders.created_at.year = 2020
SELECT
    orders.users.city,
    orders.id.cancelled_count / orders.id.count -> cancellation_rate,
    orders.id.cancelled_count,
    orders.id.count,
    orders.created_at.year,
HAVING
    orders.id.count>10
ORDER BY

    cancellation_rate desc;"""
    )

    sql = Dialects.DUCK_DB.default_executor(environment=env).generate_sql(queries[-1])
