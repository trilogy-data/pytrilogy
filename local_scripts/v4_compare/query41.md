# Query 41

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (4 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 3026 | 48 |
| reference | 2464 | 40 |
| v4 / ref | 1.23x | 1.20x |

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
quizzical as (
SELECT
    "item_items"."I_CATEGORY" as "item_category",
    "item_items"."I_COLOR" as "item_color",
    "item_items"."I_ITEM_SK" as "item_id",
    "item_items"."I_MANUFACT" as "item_manufact",
    "item_items"."I_MANUFACT_ID" as "item_manufacturer_id",
    "item_items"."I_PRODUCT_NAME" as "item_product_name",
    "item_items"."I_SIZE" as "item_size",
    "item_items"."I_UNITS" as "item_units"
FROM
    "memory"."item" as "item_items"),
wakeful as (
SELECT
    CASE WHEN ( "quizzical"."item_category" = 'Women' and "quizzical"."item_color" in ('powder','khaki') and "quizzical"."item_units" in ('Ounce','Oz') and "quizzical"."item_size" in ('medium','extra large') ) or ( "quizzical"."item_category" = 'Women' and "quizzical"."item_color" in ('brown','honeydew') and "quizzical"."item_units" in ('Bunch','Ton') and "quizzical"."item_size" in ('N/A','small') ) or ( "quizzical"."item_category" = 'Men' and "quizzical"."item_color" in ('floral','deep') and "quizzical"."item_units" in ('N/A','Dozen') and "quizzical"."item_size" in ('petite','large') ) or ( "quizzical"."item_category" = 'Men' and "quizzical"."item_color" in ('light','cornflower') and "quizzical"."item_units" in ('Box','Pound') and "quizzical"."item_size" in ('medium','extra large') ) or ( "quizzical"."item_category" = 'Women' and "quizzical"."item_color" in ('midnight','snow') and "quizzical"."item_units" in ('Pallet','Gross') and "quizzical"."item_size" in ('medium','extra large') ) or ( "quizzical"."item_category" = 'Women' and "quizzical"."item_color" in ('cyan','papaya') and "quizzical"."item_units" in ('Cup','Dram') and "quizzical"."item_size" in ('N/A','small') ) or ( "quizzical"."item_category" = 'Men' and "quizzical"."item_color" in ('orange','frosted') and "quizzical"."item_units" in ('Each','Tbl') and "quizzical"."item_size" in ('petite','large') ) or ( "quizzical"."item_category" = 'Men' and "quizzical"."item_color" in ('forest','ghost') and "quizzical"."item_units" in ('Lb','Bundle') and "quizzical"."item_size" in ('medium','extra large') ) THEN "quizzical"."item_id" ELSE NULL END as "_virt_filter_id_7632345629166937"
FROM
    "quizzical"),
cheerful as (
SELECT
    "quizzical"."item_manufact" as "item_manufact",
    count("wakeful"."_virt_filter_id_7632345629166937") as "manufact_matches"
FROM
    "quizzical"
GROUP BY
    1),
highfalutin as (
SELECT
    CASE WHEN "quizzical"."item_manufacturer_id" BETWEEN 738 AND 778 THEN "quizzical"."item_product_name" ELSE NULL END as "filtered_product_name"
FROM
    "quizzical"),
thoughtful as (
SELECT
    "highfalutin"."filtered_product_name" as "filtered_product_name",
    coalesce("cheerful"."manufact_matches",0) as "manufact_matches"
FROM
    "cheerful"
    FULL JOIN "highfalutin" on 1=1)
SELECT
    "thoughtful"."filtered_product_name" as "filtered_product_name"
FROM
    "thoughtful"
WHERE
    "thoughtful"."filtered_product_name" is not null and "thoughtful"."manufact_matches" > 0

ORDER BY 
    "thoughtful"."filtered_product_name" asc
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

## v4 execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 161, in run_one
    result.v4_rows = execute(con, v4_sql)
                     ~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 102, in execute
    cursor = con.execute(sql)
_duckdb.BinderException: Binder Error: Referenced table "wakeful" not found!
Candidate tables: "quizzical"

LINE 22:     count("wakeful"."_virt_filter_id_7632345629166937") as "manufact_...
                   ^
```
