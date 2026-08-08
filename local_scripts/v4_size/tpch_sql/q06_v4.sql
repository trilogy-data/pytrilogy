SELECT
    sum("lineitem"."l_extendedprice" * "lineitem"."l_discount") as "q_revenue"
FROM
    "memory"."lineitem" as "lineitem"
WHERE
    "lineitem"."l_shipdate" >= date '1994-01-01' and "lineitem"."l_shipdate" < date '1995-01-01' and "lineitem"."l_discount" BETWEEN 0.05 AND 0.07 and "lineitem"."l_quantity" < 24
