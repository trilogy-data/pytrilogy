def test_demo(engine):
    query = """
import store_sales as store_sales;
with ranked_states as
select 
    store_sales.customer.first_name,
    store_sales.customer.state,
    rank store_sales.customer.first_name 
        over store_sales.customer.state 
        order by  sum(store_sales.sales_price) by store_sales.customer.first_name, store_sales.customer.state desc 
    -> sales_rank;

select 
    ranked_states.store_sales.customer.first_name,
    avg(cast(ranked_states.sales_rank as int))-> avg_sales_rank
order by 
    avg_sales_rank desc
limit 10
;"""

    results = engine.execute_text(query)[0].fetchall()

    assert results[0].avg_sales_rank != 1.0
