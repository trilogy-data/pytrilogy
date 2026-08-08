
WITH 
yummy as (
SELECT
    "partsupp"."ps_partkey" as "id",
    "partsupp"."ps_suppkey" as "supplier_id",
    "partsupp"."ps_supplycost" as "supply_cost",
    "supplier_supplier"."s_name" as "supplier_name",
    UPPER("supplier_nation_nation"."n_name")  as "supplier_nation_name"
FROM
    "memory"."partsupp" as "partsupp"
    INNER JOIN "memory"."supplier" as "supplier_supplier" on "partsupp"."ps_suppkey" = "supplier_supplier"."s_suppkey"
    INNER JOIN "memory"."nation" as "supplier_nation_nation" on "supplier_supplier"."s_nationkey" = "supplier_nation_nation"."n_nationkey"),
cooperative as (
SELECT
    "partsupp"."ps_partkey" as "id",
    "partsupp"."ps_suppkey" as "supplier_id",
    "partsupp"."ps_supplycost" as "supply_cost",
    "supplier_nation_nation"."n_nationkey" as "supplier_nation_id",
    "supplier_nation_nation"."n_regionkey" as "supplier_nation_region_id",
    "supplier_nation_region_region"."r_name" as "supplier_nation_region_name"
FROM
    "memory"."partsupp" as "partsupp"
    INNER JOIN "memory"."supplier" as "supplier_supplier" on "partsupp"."ps_suppkey" = "supplier_supplier"."s_suppkey"
    INNER JOIN "memory"."nation" as "supplier_nation_nation" on "supplier_supplier"."s_nationkey" = "supplier_nation_nation"."n_nationkey"
    INNER JOIN "memory"."region" as "supplier_nation_region_region" on "supplier_nation_nation"."n_regionkey" = "supplier_nation_region_region"."r_regionkey"),
abundant as (
SELECT
    "cooperative"."id" as "id",
    min(CASE WHEN "cooperative"."supplier_nation_region_name" = 'EUROPE' THEN "cooperative"."supply_cost" ELSE NULL END) as "min_supply_cost_in_europe"
FROM
    "cooperative"
GROUP BY
    1),
questionable as (
SELECT
    "cooperative"."id" as "id",
    "cooperative"."supplier_id" as "supplier_id",
    "cooperative"."supplier_nation_id" as "supplier_nation_id",
    "cooperative"."supplier_nation_region_id" as "supplier_nation_region_id",
    "cooperative"."supplier_nation_region_name" as "supplier_nation_region_name",
    "cooperative"."supply_cost" as "supply_cost"
FROM
    "cooperative"
WHERE
    "cooperative"."supplier_nation_region_name" = 'EUROPE'
),
uneven as (
SELECT
    "abundant"."min_supply_cost_in_europe" as "min_supply_cost_in_europe",
    "part"."p_partkey" as "id",
    "questionable"."supplier_id" as "supplier_id"
FROM
    "questionable"
    INNER JOIN "abundant" on "questionable"."id" = "abundant"."id"
    INNER JOIN "memory"."part" as "part" on "questionable"."id" = "part"."p_partkey"
WHERE
    "part"."p_size" = 15 and "part"."p_type" like '%BRASS' and "questionable"."supply_cost" <= "abundant"."min_supply_cost_in_europe" * 1.5

GROUP BY
    1,
    2,
    3,
    "part"."p_size",
    "part"."p_type",
    "questionable"."supplier_nation_id",
    "questionable"."supplier_nation_region_id",
    "questionable"."supplier_nation_region_name")
SELECT
    "uneven"."id" as "id",
    "yummy"."supplier_name" as "supplier_name",
    "yummy"."supplier_nation_name" as "supplier_nation_name",
    "yummy"."supply_cost" as "supply_cost"
FROM
    "yummy"
    INNER JOIN "uneven" on "yummy"."id" = "uneven"."id" AND "yummy"."supplier_id" = "uneven"."supplier_id"
WHERE
    "yummy"."supply_cost" <= "uneven"."min_supply_cost_in_europe" * 1.5

GROUP BY
    1,
    2,
    3,
    4
ORDER BY 
    "uneven"."id" asc,
    "yummy"."supplier_name" asc
LIMIT (100)