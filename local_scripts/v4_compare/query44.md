# Query 44

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (10 rows) |
| results identical | NO |

## Result comparison

v4 rows: 100 (88 distinct)
ref rows: 10 (10 distinct)
only in v4 (showing up to 5 of 88):
  13x  (None, 'callyn stantiation', 1)
  1x  ('pripripripriought', 'callyn stantiation', 1)
  1x  ('pripripripri', 'callyn stantiation', 1)
  1x  ('pripripriought', 'callyn stantiation', 1)
  1x  ('pripripriese', 'callyn stantiation', 1)
only in ref (showing up to 5 of 10):
  1x  ('eingpricallyoughtought', 'callyn stantiation', 1)
  1x  ('ableableableable', 'callyableesepriought', 2)
  1x  ('eingableableation', 'eingesepriantiought', 3)
  1x  ('oughtableeseable', 'bareingpriought', 4)
  1x  ('eingpriationcallyought', 'baroughtoughtation', 5)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2924 | 105 | 106.44 ms |
| reference | 2861 | 101 | 110.91 ms |
| v4 / ref | 1.02x | 1.04x | 0.96x |

## Preql

```
import store_sales as ss;

# Threshold: avg net_profit for store 4 where address is null.
# Pre-compute in a filtered `with` block so trilogy emits
# `WHERE SS_ADDR_SK IS NULL` instead of `avg(CASE WHEN SS_ADDR_SK IS NULL THEN ...)`.
rowset addr_null_threshold <- where
    ss.store.id = 4 and ss.sale_address.id is null
select
    avg(ss.net_profit) as threshold,
;

# Per-item avg profit at store 4
auto item_avg_profit <- avg(ss.net_profit) by ss.item.id;

rowset ascending <- where
    ss.store.id = 4 and item_avg_profit > 0.9 * addr_null_threshold.threshold
select
    rank(ss.item.id) over (order by item_avg_profit asc) as rnk_a,
    ss.item.product_name as best_performing,
;

rowset descending <- where
    ss.store.id = 4 and item_avg_profit > 0.9 * addr_null_threshold.threshold
select
    rank(ss.item.id) over (order by item_avg_profit desc) as rnk_d,
    ss.item.product_name as worst_performing,
;

merge ascending.rnk_a into descending.rnk_d;

where
    ss.store.id = 4
select
    ascending.rnk_a as rnk,
    ascending.best_performing,
    descending.worst_performing,
having
    rnk < 11

order by
    rnk asc nulls first,
    ascending.best_performing desc nulls first,
    descending.worst_performing desc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
thoughtful as (
SELECT
    "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
    avg("ss_store_sales"."SS_NET_PROFIT") as "item_avg_profit"
FROM
    "memory"."store_sales" as "ss_store_sales"
WHERE
    "ss_store_sales"."SS_STORE_SK" = 4

GROUP BY
    1),
cooperative as (
SELECT
    "ss_store_sales"."SS_ITEM_SK" as "ss_item_id"
FROM
    "memory"."store_sales" as "ss_store_sales"
WHERE
    "ss_store_sales"."SS_STORE_SK" = 4

GROUP BY
    1,
    "ss_store_sales"."SS_STORE_SK"),
highfalutin as (
SELECT
    avg("ss_store_sales"."SS_NET_PROFIT") as "addr_null_threshold_threshold"
FROM
    "memory"."store_sales" as "ss_store_sales"
WHERE
    "ss_store_sales"."SS_STORE_SK" = 4 and "ss_store_sales"."SS_ADDR_SK" is null
),
questionable as (
SELECT
    "thoughtful"."item_avg_profit" as "item_avg_profit",
    "thoughtful"."ss_item_id" as "ss_item_id"
FROM
    "cooperative"
    INNER JOIN "thoughtful" on "cooperative"."ss_item_id" = "thoughtful"."ss_item_id"),
abundant as (
SELECT
    "questionable"."item_avg_profit" as "item_avg_profit",
    "questionable"."ss_item_id" as "ss_item_id"
FROM
    "highfalutin"
    INNER JOIN "questionable" on 1=1
WHERE
    "questionable"."item_avg_profit" > 0.9 * "highfalutin"."addr_null_threshold_threshold"

GROUP BY
    1,
    2),
uneven as (
SELECT
    "abundant"."ss_item_id" as "ss_item_id",
    rank() over (order by "abundant"."item_avg_profit" asc ) as "_ascending_rnk_a",
    rank() over (order by "abundant"."item_avg_profit" desc ) as "_descending_rnk_d"
FROM
    "abundant"),
juicy as (
SELECT
    "ss_item_items"."I_PRODUCT_NAME" as "descending_worst_performing",
    "uneven"."_descending_rnk_d" as "descending_rnk_d"
FROM
    "uneven"
    INNER JOIN "memory"."item" as "ss_item_items" on "uneven"."ss_item_id" = "ss_item_items"."I_ITEM_SK"
GROUP BY
    1,
    2),
yummy as (
SELECT
    "ss_item_items"."I_PRODUCT_NAME" as "ascending_best_performing"
FROM
    "uneven"
    INNER JOIN "memory"."item" as "ss_item_items" on "uneven"."ss_item_id" = "ss_item_items"."I_ITEM_SK"
GROUP BY
    1,
    "uneven"."_ascending_rnk_a"),
vacuous as (
SELECT
    "juicy"."descending_rnk_d" as "rnk",
    "juicy"."descending_worst_performing" as "descending_worst_performing"
FROM
    "juicy"),
concerned as (
SELECT
    "vacuous"."descending_worst_performing" as "descending_worst_performing",
    "vacuous"."rnk" as "rnk",
    "yummy"."ascending_best_performing" as "ascending_best_performing"
FROM
    "yummy"
    RIGHT OUTER JOIN "vacuous" on 1=1
WHERE
    "vacuous"."rnk" < 11
)
SELECT
    "concerned"."rnk" as "rnk",
    "concerned"."ascending_best_performing" as "ascending_best_performing",
    "concerned"."descending_worst_performing" as "descending_worst_performing"
FROM
    "concerned"
ORDER BY 
    "concerned"."rnk" asc nulls first,
    "concerned"."ascending_best_performing" desc nulls first,
    "concerned"."descending_worst_performing" desc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
thoughtful as (
SELECT
    "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
    avg("ss_store_sales"."SS_NET_PROFIT") as "item_avg_profit"
FROM
    "memory"."store_sales" as "ss_store_sales"
WHERE
    "ss_store_sales"."SS_STORE_SK" = 4

GROUP BY
    1),
cooperative as (
SELECT
    "ss_store_sales"."SS_ITEM_SK" as "ss_item_id"
FROM
    "memory"."store_sales" as "ss_store_sales"
WHERE
    "ss_store_sales"."SS_STORE_SK" = 4

GROUP BY
    1,
    "ss_store_sales"."SS_STORE_SK"),
highfalutin as (
SELECT
    avg("ss_store_sales"."SS_NET_PROFIT") as "addr_null_threshold_threshold"
FROM
    "memory"."store_sales" as "ss_store_sales"
WHERE
    "ss_store_sales"."SS_STORE_SK" = 4 and "ss_store_sales"."SS_ADDR_SK" is null
),
questionable as (
SELECT
    "thoughtful"."item_avg_profit" as "item_avg_profit",
    "thoughtful"."ss_item_id" as "ss_item_id"
FROM
    "cooperative"
    INNER JOIN "thoughtful" on "cooperative"."ss_item_id" = "thoughtful"."ss_item_id"),
abundant as (
SELECT
    "questionable"."item_avg_profit" as "item_avg_profit",
    "questionable"."ss_item_id" as "ss_item_id"
FROM
    "highfalutin"
    INNER JOIN "questionable" on 1=1
WHERE
    "questionable"."item_avg_profit" > 0.9 * "highfalutin"."addr_null_threshold_threshold"

GROUP BY
    1,
    2),
uneven as (
SELECT
    "abundant"."ss_item_id" as "ss_item_id",
    rank() over (order by "abundant"."item_avg_profit" asc ) as "_ascending_rnk_a",
    rank() over (order by "abundant"."item_avg_profit" desc ) as "_descending_rnk_d"
FROM
    "abundant"),
juicy as (
SELECT
    "ss_item_items"."I_PRODUCT_NAME" as "descending_worst_performing",
    "uneven"."_descending_rnk_d" as "descending_rnk_d",
    "uneven"."_descending_rnk_d" as "rnk"
FROM
    "uneven"
    INNER JOIN "memory"."item" as "ss_item_items" on "uneven"."ss_item_id" = "ss_item_items"."I_ITEM_SK"
GROUP BY
    1,
    2),
yummy as (
SELECT
    "ss_item_items"."I_PRODUCT_NAME" as "ascending_best_performing",
    "uneven"."_ascending_rnk_a" as "ascending_rnk_a"
FROM
    "uneven"
    INNER JOIN "memory"."item" as "ss_item_items" on "uneven"."ss_item_id" = "ss_item_items"."I_ITEM_SK"
GROUP BY
    1,
    2),
vacuous as (
SELECT
    "juicy"."descending_worst_performing" as "descending_worst_performing",
    "juicy"."rnk" as "rnk",
    "yummy"."ascending_best_performing" as "ascending_best_performing"
FROM
    "juicy"
    INNER JOIN "yummy" on "juicy"."descending_rnk_d" = "yummy"."ascending_rnk_a"
WHERE
    "juicy"."rnk" < 11
)
SELECT
    "vacuous"."rnk" as "rnk",
    "vacuous"."ascending_best_performing" as "ascending_best_performing",
    "vacuous"."descending_worst_performing" as "descending_worst_performing"
FROM
    "vacuous"
ORDER BY 
    "vacuous"."rnk" asc nulls first,
    "vacuous"."ascending_best_performing" desc nulls first,
    "vacuous"."descending_worst_performing" desc nulls first
LIMIT (100)
```
