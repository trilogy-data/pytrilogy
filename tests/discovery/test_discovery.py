from decimal import Decimal
from pathlib import Path

from trilogy import Dialects, Environment

working_dir = Path(__file__).parent


def test_history_e2e():
    env = Environment(working_path=working_dir).from_file(working_dir / "inputs.preql")
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    query = """where customer_id = 2
        select local.customer_id, local.total_customer_revenue;
        
        """
    cmd = exec.generate_sql(query)[-1]
    assert cmd.strip() == """SELECT
    "customer_revenue_for_two"."customer_id" as "customer_id",
    "customer_revenue_for_two"."total_customer_revenue" as "total_customer_revenue"
FROM
    (
select
    2 as customer_id,
    11.03 as total_customer_revenue
) as "customer_revenue_for_two" """.strip()

    results = exec.execute_text(query)[-1].fetchall()
    assert results == [(2, Decimal("11.03"))], "Results should match expected output"


def test_history_e2e_non_materialized_field():
    env = Environment(working_path=working_dir).from_file(working_dir / "inputs.preql")
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    query2 = """
    where name = 'Sarah'
        select 
            local.customer_id, 
            local.total_customer_revenue, 
;
        """

    cmd = exec.generate_sql(query2)[-1]
    assert cmd.strip() == """SELECT
    "customer_revenue_for_sarah"."customer_id" as "customer_id",
    "customer_revenue_for_sarah"."total_customer_revenue" as "total_customer_revenue"
FROM
    (
select
    2 as customer_id,
    11.03 as total_customer_revenue
) as "customer_revenue_for_sarah" """.strip()

    results = exec.execute_text(query2)[-1].fetchall()
    assert results == [(2, Decimal("11.03"))], "Results should match expected output"
