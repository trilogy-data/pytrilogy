# Query 44

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (10 rows) |
| reference execution | OK (10 rows) |
| results identical | YES |

## Result comparison

v4 rows: 10 (10 distinct)
ref rows: 10 (10 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 3209 | 108 | 27.59 ms |
| reference | 2861 | 101 | 44.62 ms |
| v4 / ref | 1.12x | 1.07x | 0.62x |

## Preql

```
import physical_sales as ss;

# Threshold: avg net_profit for store 4 where address is null.
# Pre-compute in a filtered `with` block so trilogy emits
# `WHERE SS_ADDR_SK IS NULL` instead of `avg(CASE WHEN SS_ADDR_SK IS NULL THEN ...)`.
rowset addr_null_threshold <- where
    ss.store.id = 1 and ss.sale_address.id is null
select
    avg(ss.net_profit) as threshold,
;

# Per-item avg profit at store 4
auto item_avg_profit <- avg(ss.net_profit) by ss.item.id;

rowset ascending <- where
    ss.store.id = 1 and item_avg_profit > 0.9 * addr_null_threshold.threshold
select
    rank(ss.item.id) over (order by item_avg_profit asc) as rnk_a,
    ss.item.product_name as best_performing,
;

rowset descending <- where
    ss.store.id = 1 and item_avg_profit > 0.9 * addr_null_threshold.threshold
select
    rank(ss.item.id) over (order by item_avg_profit desc) as rnk_d,
    ss.item.product_name as worst_performing,
;

merge ascending.rnk_a into descending.rnk_d;

where
    ss.store.id = 1
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
abundant as (
SELECT
    "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
    "ss_store_sales"."SS_NET_PROFIT" as "ss_net_profit"
FROM
    "memory"."store_sales" as "ss_store_sales"
WHERE
    "ss_store_sales"."SS_STORE_SK" = 1
),
wakeful as (
SELECT
    avg("ss_store_sales"."SS_NET_PROFIT") as "_addr_null_threshold_threshold"
FROM
    "memory"."store_sales" as "ss_store_sales"
WHERE
    "ss_store_sales"."SS_STORE_SK" = 1 and "ss_store_sales"."SS_ADDR_SK" is null
),
quizzical as (
SELECT
    "ss_item_items"."I_ITEM_SK" as "ss_item_id",
    "ss_item_items"."I_PRODUCT_NAME" as "_ascending_best_performing",
    "ss_item_items"."I_PRODUCT_NAME" as "_descending_worst_performing"
FROM
    "memory"."item" as "ss_item_items"),
uneven as (
SELECT
    "abundant"."ss_item_id" as "ss_item_id",
    "abundant"."ss_net_profit" as "ss_net_profit"
FROM
    "abundant"),
questionable as (
SELECT
    "wakeful"."_addr_null_threshold_threshold" as "addr_null_threshold_threshold"
FROM
    "wakeful"),
yummy as (
SELECT
    "uneven"."ss_item_id" as "ss_item_id",
    avg("uneven"."ss_net_profit") as "item_avg_profit"
FROM
    "uneven"
GROUP BY
    1),
vacuous as (
SELECT
    "yummy"."item_avg_profit" as "item_avg_profit",
    "yummy"."ss_item_id" as "ss_item_id"
FROM
    "uneven"
    INNER JOIN "yummy" on "uneven"."ss_item_id" = "yummy"."ss_item_id"
    INNER JOIN "questionable" on 1=1
WHERE
    "yummy"."item_avg_profit" > 0.9 * "questionable"."addr_null_threshold_threshold"

GROUP BY
    1,
    2),
abhorrent as (
SELECT
    "vacuous"."ss_item_id" as "ss_item_id",
    rank() over (order by "vacuous"."item_avg_profit" asc ) as "_ascending_rnk_a",
    rank() over (order by "vacuous"."item_avg_profit" desc ) as "_descending_rnk_d"
FROM
    "vacuous"),
sweltering as (
SELECT
    "abhorrent"."_ascending_rnk_a" as "_ascending_rnk_a",
    "abhorrent"."_descending_rnk_d" as "_descending_rnk_d",
    "quizzical"."_ascending_best_performing" as "_ascending_best_performing",
    "quizzical"."_descending_worst_performing" as "_descending_worst_performing"
FROM
    "abhorrent"
    INNER JOIN "quizzical" on "abhorrent"."ss_item_id" = "quizzical"."ss_item_id"),
macho as (
SELECT
    "sweltering"."_descending_rnk_d" as "rnk",
    "sweltering"."_descending_worst_performing" as "descending_worst_performing"
FROM
    "sweltering"),
late as (
SELECT
    "sweltering"."_ascending_best_performing" as "ascending_best_performing",
    "sweltering"."_ascending_rnk_a" as "ascending_rnk_a"
FROM
    "sweltering"),
friendly as (
SELECT
    "late"."ascending_best_performing" as "ascending_best_performing",
    "macho"."descending_worst_performing" as "descending_worst_performing",
    "macho"."rnk" as "rnk"
FROM
    "macho"
    INNER JOIN "late" on "macho"."rnk" = "late"."ascending_rnk_a"
WHERE
    "macho"."rnk" < 11
)
SELECT
    "friendly"."rnk" as "rnk",
    "friendly"."ascending_best_performing" as "ascending_best_performing",
    "friendly"."descending_worst_performing" as "descending_worst_performing"
FROM
    "friendly"
ORDER BY 
    "friendly"."rnk" asc nulls first,
    "friendly"."ascending_best_performing" desc nulls first,
    "friendly"."descending_worst_performing" desc nulls first
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
    "ss_store_sales"."SS_STORE_SK" = 1

GROUP BY
    1),
cooperative as (
SELECT
    "ss_store_sales"."SS_ITEM_SK" as "ss_item_id"
FROM
    "memory"."store_sales" as "ss_store_sales"
WHERE
    "ss_store_sales"."SS_STORE_SK" = 1

GROUP BY
    1,
    "ss_store_sales"."SS_STORE_SK"),
highfalutin as (
SELECT
    avg("ss_store_sales"."SS_NET_PROFIT") as "addr_null_threshold_threshold"
FROM
    "memory"."store_sales" as "ss_store_sales"
WHERE
    "ss_store_sales"."SS_STORE_SK" = 1 and "ss_store_sales"."SS_ADDR_SK" is null
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
