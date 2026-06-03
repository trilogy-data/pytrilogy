# Query 41

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | YES |

## Result comparison

v4 rows: 100 (100 distinct)
ref rows: 100 (100 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2184 | 33 | 11.08 ms |
| reference | 2112 | 42 | 9.68 ms |
| v4 / ref | 1.03x | 0.79x | 1.15x |

## Preql

```
import item as item;

auto manufact_matches <- count(
    item.id
        ? (item.category = 'Books'       and item.color = 'tan'     and item.units = 'Oz'     and item.size = 'N/A')
       or (item.category = 'Electronics' and item.color = 'purple'  and item.units = 'Ton'    and item.size = 'N/A')
       or (item.category = 'Men'         and item.color = 'misty'   and item.units = 'Box'    and item.size = 'medium')
       or (item.category = 'Books'       and item.color = 'medium'  and item.units = 'Tsp'    and item.size = 'N/A')
       or (item.category = 'Books'       and item.color = 'midnight' and item.units = 'Gram'  and item.size = 'N/A')
       or (item.category = 'Books'       and item.color = 'pale'    and item.units = 'Pound'  and item.size = 'N/A')
       or (item.category = 'Electronics' and item.color = 'khaki'   and item.units = 'Pallet' and item.size = 'N/A')
       or (item.category = 'Electronics' and item.color = 'mint'    and item.units = 'Gross'  and item.size = 'N/A')
)
    by item.manufact;

select
    item.product_name ? item.manufacturer_id between 1 and 500 as filtered_product_name,
    --manufact_matches,
having
    filtered_product_name is not null and manufact_matches > 0

order by
    filtered_product_name asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
highfalutin as (
SELECT
    "item_items"."I_MANUFACT" as "item_manufact",
    CASE WHEN "item_items"."I_MANUFACT_ID" BETWEEN 1 AND 500 THEN "item_items"."I_PRODUCT_NAME" ELSE NULL END as "filtered_product_name",
    CASE WHEN ( "item_items"."I_CATEGORY" = 'Books' and "item_items"."I_COLOR" = 'tan' and "item_items"."I_UNITS" = 'Oz' and "item_items"."I_SIZE" = 'N/A' ) or ( "item_items"."I_CATEGORY" = 'Electronics' and "item_items"."I_COLOR" = 'purple' and "item_items"."I_UNITS" = 'Ton' and "item_items"."I_SIZE" = 'N/A' ) or ( "item_items"."I_CATEGORY" = 'Men' and "item_items"."I_COLOR" = 'misty' and "item_items"."I_UNITS" = 'Box' and "item_items"."I_SIZE" = 'medium' ) or ( "item_items"."I_CATEGORY" = 'Books' and "item_items"."I_COLOR" = 'medium' and "item_items"."I_UNITS" = 'Tsp' and "item_items"."I_SIZE" = 'N/A' ) or ( "item_items"."I_CATEGORY" = 'Books' and "item_items"."I_COLOR" = 'midnight' and "item_items"."I_UNITS" = 'Gram' and "item_items"."I_SIZE" = 'N/A' ) or ( "item_items"."I_CATEGORY" = 'Books' and "item_items"."I_COLOR" = 'pale' and "item_items"."I_UNITS" = 'Pound' and "item_items"."I_SIZE" = 'N/A' ) or ( "item_items"."I_CATEGORY" = 'Electronics' and "item_items"."I_COLOR" = 'khaki' and "item_items"."I_UNITS" = 'Pallet' and "item_items"."I_SIZE" = 'N/A' ) or ( "item_items"."I_CATEGORY" = 'Electronics' and "item_items"."I_COLOR" = 'mint' and "item_items"."I_UNITS" = 'Gross' and "item_items"."I_SIZE" = 'N/A' ) THEN "item_items"."I_ITEM_SK" ELSE NULL END as "_virt_filter_id_7263619893092856"
FROM
    "memory"."item" as "item_items"),
wakeful as (
SELECT
    "highfalutin"."item_manufact" as "item_manufact",
    count("highfalutin"."_virt_filter_id_7263619893092856") as "manufact_matches"
FROM
    "highfalutin"
GROUP BY
    1
HAVING
    "manufact_matches" > 0
)
SELECT
    "highfalutin"."filtered_product_name" as "filtered_product_name"
FROM
    "wakeful"
    INNER JOIN "highfalutin" on "wakeful"."item_manufact" is not distinct from "highfalutin"."item_manufact"
WHERE
    "highfalutin"."filtered_product_name" is not null

GROUP BY
    1,
    "wakeful"."manufact_matches"
ORDER BY 
    "highfalutin"."filtered_product_name" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
highfalutin as (
SELECT
    "item_items"."I_MANUFACT" as "item_manufact"
FROM
    "memory"."item" as "item_items"
WHERE
    ( "item_items"."I_CATEGORY" = 'Books' and "item_items"."I_COLOR" = 'tan' and "item_items"."I_UNITS" = 'Oz' and "item_items"."I_SIZE" = 'N/A' ) or ( "item_items"."I_CATEGORY" = 'Electronics' and "item_items"."I_COLOR" = 'purple' and "item_items"."I_UNITS" = 'Ton' and "item_items"."I_SIZE" = 'N/A' ) or ( "item_items"."I_CATEGORY" = 'Men' and "item_items"."I_COLOR" = 'misty' and "item_items"."I_UNITS" = 'Box' and "item_items"."I_SIZE" = 'medium' ) or ( "item_items"."I_CATEGORY" = 'Books' and "item_items"."I_COLOR" = 'medium' and "item_items"."I_UNITS" = 'Tsp' and "item_items"."I_SIZE" = 'N/A' ) or ( "item_items"."I_CATEGORY" = 'Books' and "item_items"."I_COLOR" = 'midnight' and "item_items"."I_UNITS" = 'Gram' and "item_items"."I_SIZE" = 'N/A' ) or ( "item_items"."I_CATEGORY" = 'Books' and "item_items"."I_COLOR" = 'pale' and "item_items"."I_UNITS" = 'Pound' and "item_items"."I_SIZE" = 'N/A' ) or ( "item_items"."I_CATEGORY" = 'Electronics' and "item_items"."I_COLOR" = 'khaki' and "item_items"."I_UNITS" = 'Pallet' and "item_items"."I_SIZE" = 'N/A' ) or ( "item_items"."I_CATEGORY" = 'Electronics' and "item_items"."I_COLOR" = 'mint' and "item_items"."I_UNITS" = 'Gross' and "item_items"."I_SIZE" = 'N/A' )

GROUP BY
    1
HAVING
    count("item_items"."I_ITEM_SK") > 0
),
cheerful as (
SELECT
    "item_items"."I_MANUFACT" as "item_manufact",
    "item_items"."I_PRODUCT_NAME" as "filtered_product_name"
FROM
    "memory"."item" as "item_items"
WHERE
    "item_items"."I_MANUFACT_ID" BETWEEN 1 AND 500

GROUP BY
    1,
    2),
cooperative as (
SELECT
    "cheerful"."filtered_product_name" as "filtered_product_name"
FROM
    "cheerful"
    INNER JOIN "highfalutin" on "cheerful"."item_manufact" is not distinct from "highfalutin"."item_manufact"
WHERE
    "cheerful"."filtered_product_name" is not null
)
SELECT
    "cooperative"."filtered_product_name" as "filtered_product_name"
FROM
    "cooperative"
ORDER BY 
    "cooperative"."filtered_product_name" asc
LIMIT (100)
```
