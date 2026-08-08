
WITH 
wakeful as (
SELECT
    coalesce("orders_customer_customers"."c_custkey","orders_orders"."o_custkey") as "orders_customer_id",
    coalesce(count(CASE WHEN "orders_orders"."o_comment" not like '%special%requests%' THEN "orders_orders"."o_orderkey" ELSE NULL END),0) as "c_count"
FROM
    "memory"."customer" as "orders_customer_customers"
    LEFT OUTER JOIN "memory"."orders" as "orders_orders" on "orders_customer_customers"."c_custkey" = "orders_orders"."o_custkey"
GROUP BY
    1)
SELECT
    "wakeful"."c_count" as "c_count",
    count("wakeful"."orders_customer_id") as "custdist"
FROM
    "wakeful"
GROUP BY
    1
ORDER BY 
    "custdist" desc,
    "wakeful"."c_count" desc