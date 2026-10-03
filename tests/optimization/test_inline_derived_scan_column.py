"""A scan that computes a row-level scalar off its own raw columns still folds
into its consumer: the consumer renders the scalar from lineage post-fold, as
it does a rename."""

from trilogy import Dialects, Environment

MODEL = """
key order_id int;
property order_id.customer_id int;
datasource orders (o_id: order_id, o_customer: customer_id)
grain (order_id)
address orders;

key customer_id int;
property customer_id.name string;
datasource customers (c_id: customer_id, c_name: name)
grain (customer_id)
address customers;

property order_id.return_ticket int;
datasource returns (r_order: order_id, r_ticket: return_ticket)
grain (order_id)
address returns;

auto is_returned <- return_ticket is not null;
"""

QUERY = "where is_returned select order_id, name, return_ticket order by order_id asc;"


def test_scan_with_derived_column_inlines():
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.execute_raw_sql(
        "create table orders as select * from (values (1,10),(2,10),(3,11)) t(o_id,o_customer)"
    )
    executor.execute_raw_sql(
        "create table customers as select * from (values (10,'a'),(11,'b')) t(c_id,c_name)"
    )
    executor.execute_raw_sql(
        "create table returns as select * from (values (1,100),(3,null)) t(r_order,r_ticket)"
    )
    sql = executor.generate_sql(MODEL + QUERY)[-1]
    assert "WITH" not in sql, sql
    assert executor.execute_text(MODEL + QUERY)[-1].fetchall() == [(1, "a", 100)]
