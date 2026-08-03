SELECT
    ( 100.0 * sum(CASE WHEN "part_part"."p_type" like 'PROMO%' THEN "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") ELSE NULL END) ) / sum("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) as "promo_revenue"
FROM
    "memory"."part" as "part_part"
    INNER JOIN "memory"."lineitem" as "lineitem" on "part_part"."p_partkey" = "lineitem"."l_partkey"
WHERE
    "lineitem"."l_shipdate" >= date '1995-09-01' and "lineitem"."l_shipdate" < date '1995-10-01'
