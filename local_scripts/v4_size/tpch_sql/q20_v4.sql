
WITH 
abundant as (
SELECT
    "part_supplier_supplier"."s_suppkey" as "part_supplier_id",
    UPPER("part_supplier_nation_nation"."n_name")  as "part_supplier_nation_name"
FROM
    "memory"."supplier" as "part_supplier_supplier"
    INNER JOIN "memory"."nation" as "part_supplier_nation_nation" on "part_supplier_supplier"."s_nationkey" = "part_supplier_nation_nation"."n_nationkey"),
wakeful as (
SELECT
    "lineitem"."l_suppkey" as "part_supplier_id",
    coalesce("lineitem"."l_partkey","part_part"."p_partkey") as "part_id",
    sum(CASE WHEN "lineitem"."l_shipdate" >= date '1994-01-01' and "lineitem"."l_shipdate" < date '1995-01-01' THEN "lineitem"."l_quantity" ELSE NULL END) as "qty_per_partsupp_1994"
FROM
    "memory"."part" as "part_part"
    INNER JOIN "memory"."lineitem" as "lineitem" on "part_part"."p_partkey" = "lineitem"."l_partkey"
WHERE
    "part_part"."p_name" like 'forest%'

GROUP BY
    1,
    2),
uneven as (
SELECT
    "abundant"."part_supplier_nation_name" as "part_supplier_nation_name",
    coalesce("abundant"."part_supplier_id","part_partsupp"."ps_suppkey","wakeful"."part_supplier_id") as "part_supplier_id"
FROM
    "memory"."partsupp" as "part_partsupp"
    INNER JOIN "wakeful" on "part_partsupp"."ps_partkey" = "wakeful"."part_id" AND "part_partsupp"."ps_suppkey" = "wakeful"."part_supplier_id"
    INNER JOIN "memory"."part" as "part_part" on "part_partsupp"."ps_partkey" = "part_part"."p_partkey"
    INNER JOIN "abundant" on "wakeful"."part_supplier_id" = "abundant"."part_supplier_id"
WHERE
    "part_part"."p_name" like 'forest%' and "part_partsupp"."ps_availqty" > 0.5 * "wakeful"."qty_per_partsupp_1994"
)
SELECT
    "part_supplier_supplier"."s_name" as "part_supplier_name",
    "part_supplier_supplier"."s_address" as "part_supplier_address"
FROM
    "uneven"
    LEFT OUTER JOIN "memory"."supplier" as "part_supplier_supplier" on "uneven"."part_supplier_id" = "part_supplier_supplier"."s_suppkey"
WHERE
    "uneven"."part_supplier_nation_name" = 'CANADA'

GROUP BY
    1,
    2
ORDER BY 
    "part_supplier_supplier"."s_name" asc