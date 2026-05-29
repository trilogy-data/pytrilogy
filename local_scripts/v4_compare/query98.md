# Query 98

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (2522 rows) |
| reference execution | OK (2521 rows) |
| results identical | NO |

## Result comparison

v4 rows: 2522 (2522 distinct)
ref rows: 2521 (2521 distinct)
only in v4 (showing up to 5 of 8):
  1x  (Decimal('1554.04'), 0.7870806219255772, 'Books', 'business', Decimal('2.57'), 'National, famous weeks become just sufficient women. Humans allow there things.', 'AAAAAAAABIPBAAAA')
  1x  (Decimal('2145.25'), 0.6311303527701673, 'Books', 'home repair', Decimal('4.31'), 'Complex, a', 'AAAAAAAAINLAAAAA')
  1x  (Decimal('876.12'), 0.20933770607621258, 'Books', 'mystery', Decimal('1.83'), 'Interesting, payable rules hear computers; similar laws would get once then effective blues. Cultural increases ought to go major marks.', 'AAAAAAAAJFEEAAAA')
  1x  (Decimal('2145.25'), 0.7351823865756055, 'Home', 'bedding', Decimal('3.51'), 'As prime legs proceed probably orange, historic experiments. Here different skills may not appease usually continental terms. Cheerful daughters take on a shops. Far', 'AAAAAAAAAOGAAAAA')
  1x  (Decimal('71.76'), 0.021511576126742992, 'Home', 'furniture', Decimal('0.85'), 'Most increased shares may not examine sometimes evident, environmental roots. Minerals may live ge', 'AAAAAAAAAFDAAAAA')
only in ref (showing up to 5 of 7):
  1x  (None, None, 'Books', 'parenting', Decimal('1.50'), 'Direct', 'AAAAAAAACLKCAAAA')
  1x  (None, None, 'Books', 'parenting', Decimal('4.51'), 'Ancient forests read books. Patients give; especially personal fields provide parties. Social, obvious members used to support as quite financial aspects. Briskly p', 'AAAAAAAALODDAAAA')
  1x  (None, None, 'Books', 'sports', Decimal('3.78'), 'Modern, new communications come here to a databases. Expectation', 'AAAAAAAAKHFBAAAA')
  1x  (None, None, 'Home', 'paint', Decimal('5.47'), 'Then serious drugs cannot celebrate here. Possible, fatal problems could not save successful pr', 'AAAAAAAAEBKCAAAA')
  1x  (None, None, 'Sports', 'basketball', Decimal('1.19'), 'Primary, front circumstances may no', 'AAAAAAAACBFAAAAA')

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 3209 | 73 | 215.79 ms |
| reference | 2918 | 58 | 152.51 ms |
| v4 / ref | 1.10x | 1.26x | 1.41x |

## Preql

```
import store_sales as store_sales;

where
    store_sales.item.category in ('Sports', 'Books', 'Home')
    and store_sales.date.date between '1999-02-22'::date and '1999-03-24'::date
select
    store_sales.item.name,
    store_sales.item.desc,
    store_sales.item.category,
    store_sales.item.class,
    store_sales.item.current_price,
    sum(store_sales.ext_sales_price) as item_revenue,
    sum(store_sales.ext_sales_price) * 100.0 / (sum(store_sales.ext_sales_price) by store_sales.item.class) as revenueratio,
order by
    store_sales.item.category asc nulls first,
    store_sales.item.class asc nulls first,
    store_sales.item.name asc nulls first,
    store_sales.item.desc asc nulls first,
    revenueratio asc nulls first
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "store_sales_item_items"."I_CATEGORY" as "store_sales_item_category",
    "store_sales_item_items"."I_CLASS" as "store_sales_item_class",
    "store_sales_item_items"."I_CURRENT_PRICE" as "store_sales_item_current_price",
    "store_sales_item_items"."I_ITEM_DESC" as "store_sales_item_desc",
    "store_sales_item_items"."I_ITEM_ID" as "store_sales_item_name",
    "store_sales_store_sales"."SS_EXT_SALES_PRICE" as "store_sales_ext_sales_price"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
WHERE
    "store_sales_item_items"."I_CATEGORY" in ('Sports','Books','Home') and cast("store_sales_date_date"."D_DATE" as date) BETWEEN date '1999-02-22' AND date '1999-03-24'
),
cooperative as (
SELECT
    "cheerful"."store_sales_item_class" as "store_sales_item_class",
    sum("cheerful"."store_sales_ext_sales_price") as "_virt_agg_sum_7595906549305205"
FROM
    "cheerful"
GROUP BY
    1),
thoughtful as (
SELECT
    "cheerful"."store_sales_item_category" as "store_sales_item_category",
    "cheerful"."store_sales_item_class" as "store_sales_item_class",
    "cheerful"."store_sales_item_current_price" as "store_sales_item_current_price",
    "cheerful"."store_sales_item_desc" as "store_sales_item_desc",
    "cheerful"."store_sales_item_name" as "store_sales_item_name",
    sum("cheerful"."store_sales_ext_sales_price") as "_virt_agg_sum_9873055619986236",
    sum("cheerful"."store_sales_ext_sales_price") as "item_revenue"
FROM
    "cheerful"
GROUP BY
    1,
    2,
    3,
    4,
    5),
questionable as (
SELECT
    "thoughtful"."_virt_agg_sum_9873055619986236" as "_virt_agg_sum_9873055619986236",
    ( "thoughtful"."_virt_agg_sum_9873055619986236" * 100.0 ) / ("cooperative"."_virt_agg_sum_7595906549305205") as "revenueratio"
FROM
    "thoughtful"
    INNER JOIN "cooperative" on "thoughtful"."store_sales_item_class" is not distinct from "cooperative"."store_sales_item_class")
SELECT
    "thoughtful"."store_sales_item_name" as "store_sales_item_name",
    "thoughtful"."store_sales_item_desc" as "store_sales_item_desc",
    "thoughtful"."store_sales_item_category" as "store_sales_item_category",
    "thoughtful"."store_sales_item_class" as "store_sales_item_class",
    "thoughtful"."store_sales_item_current_price" as "store_sales_item_current_price",
    "thoughtful"."item_revenue" as "item_revenue",
    "questionable"."revenueratio" as "revenueratio"
FROM
    "questionable"
    INNER JOIN "thoughtful" on "questionable"."_virt_agg_sum_9873055619986236" = "thoughtful"."_virt_agg_sum_9873055619986236"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7
ORDER BY 
    "thoughtful"."store_sales_item_category" asc nulls first,
    "thoughtful"."store_sales_item_class" asc nulls first,
    "thoughtful"."store_sales_item_name" asc nulls first,
    "thoughtful"."store_sales_item_desc" asc nulls first,
    "questionable"."revenueratio" asc nulls first
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "store_sales_item_items"."I_CATEGORY" as "store_sales_item_category",
    "store_sales_item_items"."I_CLASS" as "store_sales_item_class",
    "store_sales_item_items"."I_CURRENT_PRICE" as "store_sales_item_current_price",
    "store_sales_item_items"."I_ITEM_DESC" as "store_sales_item_desc",
    "store_sales_item_items"."I_ITEM_ID" as "store_sales_item_name",
    "store_sales_store_sales"."SS_EXT_SALES_PRICE" as "store_sales_ext_sales_price"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
WHERE
    "store_sales_item_items"."I_CATEGORY" in ('Sports','Books','Home') and cast("store_sales_date_date"."D_DATE" as date) BETWEEN date '1999-02-22' AND date '1999-03-24'
),
cooperative as (
SELECT
    "cheerful"."store_sales_item_class" as "store_sales_item_class",
    sum("cheerful"."store_sales_ext_sales_price") as "_virt_agg_sum_7595906549305205"
FROM
    "cheerful"
GROUP BY
    1),
thoughtful as (
SELECT
    "cheerful"."store_sales_item_category" as "store_sales_item_category",
    "cheerful"."store_sales_item_class" as "store_sales_item_class",
    "cheerful"."store_sales_item_current_price" as "store_sales_item_current_price",
    "cheerful"."store_sales_item_desc" as "store_sales_item_desc",
    "cheerful"."store_sales_item_name" as "store_sales_item_name",
    sum("cheerful"."store_sales_ext_sales_price") as "_virt_agg_sum_9873055619986236",
    sum("cheerful"."store_sales_ext_sales_price") as "item_revenue"
FROM
    "cheerful"
GROUP BY
    1,
    2,
    3,
    4,
    5)
SELECT
    "thoughtful"."store_sales_item_name" as "store_sales_item_name",
    "thoughtful"."store_sales_item_desc" as "store_sales_item_desc",
    "thoughtful"."store_sales_item_category" as "store_sales_item_category",
    coalesce("cooperative"."store_sales_item_class","thoughtful"."store_sales_item_class") as "store_sales_item_class",
    "thoughtful"."store_sales_item_current_price" as "store_sales_item_current_price",
    "thoughtful"."item_revenue" as "item_revenue",
    ( "thoughtful"."_virt_agg_sum_9873055619986236" * 100.0 ) / ("cooperative"."_virt_agg_sum_7595906549305205") as "revenueratio"
FROM
    "thoughtful"
    INNER JOIN "cooperative" on "thoughtful"."store_sales_item_class" is not distinct from "cooperative"."store_sales_item_class"
ORDER BY 
    "thoughtful"."store_sales_item_category" asc nulls first,
    coalesce("cooperative"."store_sales_item_class","thoughtful"."store_sales_item_class") asc nulls first,
    "thoughtful"."store_sales_item_name" asc nulls first,
    "thoughtful"."store_sales_item_desc" asc nulls first,
    "revenueratio" asc nulls first
```
