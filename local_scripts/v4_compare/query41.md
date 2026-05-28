# Query 41

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (4 rows) |
| reference execution | OK (4 rows) |
| results identical | YES |

## Result comparison

v4 rows: 4 (4 distinct)
ref rows: 4 (4 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2531 | 32 | 10.70 ms |
| reference | 2464 | 40 | 12.99 ms |
| v4 / ref | 1.03x | 0.80x | 0.82x |

## Preql

```
import item as item;

auto manufact_matches <- count(
    item.id
        ? (
    item.category = 'Women'
    and item.color in ('powder', 'khaki')
    and item.units in ('Ounce', 'Oz')
    and item.size in ('medium', 'extra large')
)
or (
    item.category = 'Women'
    and item.color in ('brown', 'honeydew')
    and item.units in ('Bunch', 'Ton')
    and item.size in ('N/A', 'small')
)
or (
    item.category = 'Men'
    and item.color in ('floral', 'deep')
    and item.units in ('N/A', 'Dozen')
    and item.size in ('petite', 'large')
)
or (
    item.category = 'Men'
    and item.color in ('light', 'cornflower')
    and item.units in ('Box', 'Pound')
    and item.size in ('medium', 'extra large')
)
or (
    item.category = 'Women'
    and item.color in ('midnight', 'snow')
    and item.units in ('Pallet', 'Gross')
    and item.size in ('medium', 'extra large')
)
or (
    item.category = 'Women'
    and item.color in ('cyan', 'papaya')
    and item.units in ('Cup', 'Dram')
    and item.size in ('N/A', 'small')
)
or (
    item.category = 'Men'
    and item.color in ('orange', 'frosted')
    and item.units in ('Each', 'Tbl')
    and item.size in ('petite', 'large')
)
or (
    item.category = 'Men'
    and item.color in ('forest', 'ghost')
    and item.units in ('Lb', 'Bundle')
    and item.size in ('medium', 'extra large')
)
)
    by item.manufact;

select
    item.product_name ? item.manufacturer_id between 738 and 778 as filtered_product_name,
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
    CASE WHEN "item_items"."I_MANUFACT_ID" BETWEEN 738 AND 778 THEN "item_items"."I_PRODUCT_NAME" ELSE NULL END as "filtered_product_name",
    CASE WHEN ( "item_items"."I_CATEGORY" = 'Women' and "item_items"."I_COLOR" in ('powder','khaki') and "item_items"."I_UNITS" in ('Ounce','Oz') and "item_items"."I_SIZE" in ('medium','extra large') ) or ( "item_items"."I_CATEGORY" = 'Women' and "item_items"."I_COLOR" in ('brown','honeydew') and "item_items"."I_UNITS" in ('Bunch','Ton') and "item_items"."I_SIZE" in ('N/A','small') ) or ( "item_items"."I_CATEGORY" = 'Men' and "item_items"."I_COLOR" in ('floral','deep') and "item_items"."I_UNITS" in ('N/A','Dozen') and "item_items"."I_SIZE" in ('petite','large') ) or ( "item_items"."I_CATEGORY" = 'Men' and "item_items"."I_COLOR" in ('light','cornflower') and "item_items"."I_UNITS" in ('Box','Pound') and "item_items"."I_SIZE" in ('medium','extra large') ) or ( "item_items"."I_CATEGORY" = 'Women' and "item_items"."I_COLOR" in ('midnight','snow') and "item_items"."I_UNITS" in ('Pallet','Gross') and "item_items"."I_SIZE" in ('medium','extra large') ) or ( "item_items"."I_CATEGORY" = 'Women' and "item_items"."I_COLOR" in ('cyan','papaya') and "item_items"."I_UNITS" in ('Cup','Dram') and "item_items"."I_SIZE" in ('N/A','small') ) or ( "item_items"."I_CATEGORY" = 'Men' and "item_items"."I_COLOR" in ('orange','frosted') and "item_items"."I_UNITS" in ('Each','Tbl') and "item_items"."I_SIZE" in ('petite','large') ) or ( "item_items"."I_CATEGORY" = 'Men' and "item_items"."I_COLOR" in ('forest','ghost') and "item_items"."I_UNITS" in ('Lb','Bundle') and "item_items"."I_SIZE" in ('medium','extra large') ) THEN "item_items"."I_ITEM_SK" ELSE NULL END as "_virt_filter_id_7632345629166937"
FROM
    "memory"."item" as "item_items"),
wakeful as (
SELECT
    "highfalutin"."item_manufact" as "item_manufact",
    count("highfalutin"."_virt_filter_id_7632345629166937") as "manufact_matches"
FROM
    "highfalutin"
GROUP BY
    1),
cheerful as (
SELECT
    "highfalutin"."filtered_product_name" as "filtered_product_name"
FROM
    "wakeful"
    INNER JOIN "highfalutin" on "wakeful"."item_manufact" is not distinct from "highfalutin"."item_manufact"
WHERE
    "highfalutin"."filtered_product_name" is not null and "wakeful"."manufact_matches" > 0
)
SELECT
    "cheerful"."filtered_product_name" as "filtered_product_name"
FROM
    "cheerful"
ORDER BY 
    "cheerful"."filtered_product_name" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
highfalutin as (
SELECT
    "item_items"."I_MANUFACT" as "item_manufact",
    count("item_items"."I_ITEM_SK") as "manufact_matches"
FROM
    "memory"."item" as "item_items"
WHERE
    ( "item_items"."I_CATEGORY" = 'Women' and "item_items"."I_COLOR" in ('powder','khaki') and "item_items"."I_UNITS" in ('Ounce','Oz') and "item_items"."I_SIZE" in ('medium','extra large') ) or ( "item_items"."I_CATEGORY" = 'Women' and "item_items"."I_COLOR" in ('brown','honeydew') and "item_items"."I_UNITS" in ('Bunch','Ton') and "item_items"."I_SIZE" in ('N/A','small') ) or ( "item_items"."I_CATEGORY" = 'Men' and "item_items"."I_COLOR" in ('floral','deep') and "item_items"."I_UNITS" in ('N/A','Dozen') and "item_items"."I_SIZE" in ('petite','large') ) or ( "item_items"."I_CATEGORY" = 'Men' and "item_items"."I_COLOR" in ('light','cornflower') and "item_items"."I_UNITS" in ('Box','Pound') and "item_items"."I_SIZE" in ('medium','extra large') ) or ( "item_items"."I_CATEGORY" = 'Women' and "item_items"."I_COLOR" in ('midnight','snow') and "item_items"."I_UNITS" in ('Pallet','Gross') and "item_items"."I_SIZE" in ('medium','extra large') ) or ( "item_items"."I_CATEGORY" = 'Women' and "item_items"."I_COLOR" in ('cyan','papaya') and "item_items"."I_UNITS" in ('Cup','Dram') and "item_items"."I_SIZE" in ('N/A','small') ) or ( "item_items"."I_CATEGORY" = 'Men' and "item_items"."I_COLOR" in ('orange','frosted') and "item_items"."I_UNITS" in ('Each','Tbl') and "item_items"."I_SIZE" in ('petite','large') ) or ( "item_items"."I_CATEGORY" = 'Men' and "item_items"."I_COLOR" in ('forest','ghost') and "item_items"."I_UNITS" in ('Lb','Bundle') and "item_items"."I_SIZE" in ('medium','extra large') )

GROUP BY
    1),
cheerful as (
SELECT
    "item_items"."I_MANUFACT" as "item_manufact",
    "item_items"."I_PRODUCT_NAME" as "filtered_product_name"
FROM
    "memory"."item" as "item_items"
WHERE
    "item_items"."I_MANUFACT_ID" BETWEEN 738 AND 778

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
    "cheerful"."filtered_product_name" is not null and coalesce("highfalutin"."manufact_matches",0) > 0
)
SELECT
    "cooperative"."filtered_product_name" as "filtered_product_name"
FROM
    "cooperative"
ORDER BY 
    "cooperative"."filtered_product_name" asc
LIMIT (100)
```
