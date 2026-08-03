
WITH 
thoughtful as (
SELECT
    avg("lineitem"."l_quantity") as "avg_qty_per_part",
    coalesce("lineitem"."l_partkey","part_partsupp"."ps_partkey") as "part_id"
FROM
    "memory"."partsupp" as "part_partsupp"
    FULL JOIN "memory"."lineitem" as "lineitem" on "part_partsupp"."ps_partkey" = "lineitem"."l_partkey" AND "part_partsupp"."ps_suppkey" = "lineitem"."l_suppkey"
GROUP BY
    2),
cheerful as (
SELECT
    "lineitem"."l_extendedprice" as "extended_price",
    "lineitem"."l_quantity" as "quantity",
    coalesce("lineitem"."l_partkey","part_part"."p_partkey","part_partsupp"."ps_partkey") as "part_id"
FROM
    "memory"."partsupp" as "part_partsupp"
    INNER JOIN "memory"."lineitem" as "lineitem" on "part_partsupp"."ps_partkey" = "lineitem"."l_partkey" AND "part_partsupp"."ps_suppkey" = "lineitem"."l_suppkey"
    INNER JOIN "memory"."part" as "part_part" on "part_partsupp"."ps_partkey" = "part_part"."p_partkey"
WHERE
    "part_part"."p_brand" = 'Brand#23' and "part_part"."p_container" = 'MED BOX'
)
SELECT
    sum("cheerful"."extended_price") / 7.0 as "avg_yearly"
FROM
    "cheerful"
    INNER JOIN "thoughtful" on "cheerful"."part_id" is not distinct from "thoughtful"."part_id"
WHERE
    "cheerful"."quantity" < 0.2 * "thoughtful"."avg_qty_per_part"
