# Query -1

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
| v4 | 2524 | 65 | 4.45 ms |
| reference | 2174 | 58 | 4.36 ms |
| v4 / ref | 1.16x | 1.12x | 1.02x |

## Preql

```
# q77 CATALOG branch, extracted as a standalone select (negative test id -1).
# Inlined aggregate * scalar-from-a-different-rowset â€” the shape that makes the
# v4 planner cross-join u_id_c off the raw scan against u_sales_c off the
# aggregate. v3 plans it correctly (single projection over the aggregate).
import catalog_sales as cs;
import catalog_returns as cr;

const period_start <- '2000-08-23'::date;
const period_end <- '2000-09-22'::date;

rowset cr_grouped <- where
    cr.date.date between period_start and period_end
select
    coalesce(cr.call_center.id, -1) as cr_cc_key,
    sum(cr.return_amount) as cr_returns_per_cc,
    sum(cr.net_loss) as cr_loss_per_cc,
;

rowset cr_totals <- select
    count(cr_grouped.cr_cc_key) as cr_n_groups,
    sum(cr_grouped.cr_returns_per_cc) as cr_total_returns,
    sum(cr_grouped.cr_loss_per_cc) as cr_total_loss,
;

where
    cs.date.date between period_start and period_end
select
    'catalog channel' as u_channel_c,
    cs.call_center.id as u_id_c,
    sum(cs.ext_sales_price) * cr_totals.cr_n_groups::numeric(15,2) as u_sales_c,
    cr_totals.cr_total_returns::numeric(15,2) as u_returns_c,
    sum(cs.net_profit) * cr_totals.cr_n_groups - cr_totals.cr_total_loss::numeric(15,2) as u_profit_c,
order by u_id_c asc nulls first
;
```

## v4 generated SQL

```sql
WITH 
wakeful as (
SELECT
    coalesce("cr_catalog_returns"."CR_CALL_CENTER_SK",-1) as "_cr_grouped_cr_cc_key",
    sum("cr_catalog_returns"."CR_NET_LOSS") as "_cr_grouped_cr_loss_per_cc",
    sum("cr_catalog_returns"."CR_RETURN_AMOUNT") as "_cr_grouped_cr_returns_per_cc"
FROM
    "memory"."catalog_returns" as "cr_catalog_returns"
    INNER JOIN "memory"."date_dim" as "cr_date_date" on "cr_catalog_returns"."CR_RETURNED_DATE_SK" = "cr_date_date"."D_DATE_SK"
WHERE
    cast("cr_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
concerned as (
SELECT
    "cs_catalog_sales"."CS_CALL_CENTER_SK" as "cs_call_center_id",
    sum("cs_catalog_sales"."CS_EXT_SALES_PRICE") as "_virt_agg_sum_6520591768854391",
    sum("cs_catalog_sales"."CS_NET_PROFIT") as "_virt_agg_sum_6226990944561419"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_date_date"."D_DATE_SK"
WHERE
    cast("cs_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
late as (
SELECT
    :u_channel_c as "u_channel_c"
),
questionable as (
SELECT
    count("wakeful"."_cr_grouped_cr_cc_key") as "_cr_totals_cr_n_groups",
    sum("wakeful"."_cr_grouped_cr_loss_per_cc") as "_cr_totals_cr_total_loss",
    sum("wakeful"."_cr_grouped_cr_returns_per_cc") as "_cr_totals_cr_total_returns"
FROM
    "wakeful"),
yummy as (
SELECT
    "questionable"."_cr_totals_cr_n_groups" as "cr_totals_cr_n_groups",
    "questionable"."_cr_totals_cr_total_loss" as "cr_totals_cr_total_loss",
    "questionable"."_cr_totals_cr_total_returns" as "cr_totals_cr_total_returns"
FROM
    "questionable"),
abhorrent as (
SELECT
    "concerned"."_virt_agg_sum_6520591768854391" * cast("yummy"."cr_totals_cr_n_groups" as numeric(15,2)) as "u_sales_c",
    "concerned"."cs_call_center_id" as "u_id_c",
    ( "concerned"."_virt_agg_sum_6226990944561419" * "yummy"."cr_totals_cr_n_groups" ) - cast("yummy"."cr_totals_cr_total_loss" as numeric(15,2)) as "u_profit_c",
    cast("yummy"."cr_totals_cr_total_returns" as numeric(15,2)) as "u_returns_c"
FROM
    "yummy"
    FULL JOIN "concerned" on 1=1)
SELECT
    "late"."u_channel_c" as "u_channel_c",
    "abhorrent"."u_id_c" as "u_id_c",
    "abhorrent"."u_sales_c" as "u_sales_c",
    "abhorrent"."u_returns_c" as "u_returns_c",
    "abhorrent"."u_profit_c" as "u_profit_c"
FROM
    "abhorrent"
    FULL JOIN "late" on 1=1
ORDER BY 
    "abhorrent"."u_id_c" asc nulls first
```

## Reference SQL (zquery log)

```sql
WITH 
wakeful as (
SELECT
    coalesce("cr_catalog_returns"."CR_CALL_CENTER_SK",-1) as "cr_grouped_cr_cc_key",
    sum("cr_catalog_returns"."CR_NET_LOSS") as "cr_grouped_cr_loss_per_cc",
    sum("cr_catalog_returns"."CR_RETURN_AMOUNT") as "cr_grouped_cr_returns_per_cc"
FROM
    "memory"."catalog_returns" as "cr_catalog_returns"
    INNER JOIN "memory"."date_dim" as "cr_date_date" on "cr_catalog_returns"."CR_RETURNED_DATE_SK" = "cr_date_date"."D_DATE_SK"
WHERE
    cast("cr_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
abundant as (
SELECT
    "cs_catalog_sales"."CS_CALL_CENTER_SK" as "cs_call_center_id",
    sum("cs_catalog_sales"."CS_EXT_SALES_PRICE") as "_virt_agg_sum_6520591768854391",
    sum("cs_catalog_sales"."CS_NET_PROFIT") as "_virt_agg_sum_6226990944561419"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_date_date"."D_DATE_SK"
WHERE
    cast("cs_date_date"."D_DATE" as date) BETWEEN :period_start AND :period_end

GROUP BY
    1),
juicy as (
SELECT
    cast(sum("wakeful"."cr_grouped_cr_returns_per_cc") as numeric(15,2)) as "u_returns_c"
FROM
    "wakeful"),
thoughtful as (
SELECT
    count("wakeful"."cr_grouped_cr_cc_key") as "cr_totals_cr_n_groups",
    sum("wakeful"."cr_grouped_cr_loss_per_cc") as "cr_totals_cr_total_loss"
FROM
    "wakeful"),
yummy as (
SELECT
    "abundant"."_virt_agg_sum_6520591768854391" * cast("thoughtful"."cr_totals_cr_n_groups" as numeric(15,2)) as "u_sales_c",
    "abundant"."cs_call_center_id" as "u_id_c",
    ( "abundant"."_virt_agg_sum_6226990944561419" * "thoughtful"."cr_totals_cr_n_groups" ) - cast("thoughtful"."cr_totals_cr_total_loss" as numeric(15,2)) as "u_profit_c",
    :u_channel_c as "u_channel_c"
FROM
    "thoughtful"
    FULL JOIN "abundant" on 1=1)
SELECT
    "yummy"."u_channel_c" as "u_channel_c",
    "yummy"."u_id_c" as "u_id_c",
    "yummy"."u_sales_c" as "u_sales_c",
    "juicy"."u_returns_c" as "u_returns_c",
    "yummy"."u_profit_c" as "u_profit_c"
FROM
    "yummy"
    FULL JOIN "juicy" on 1=1
ORDER BY 
    "yummy"."u_id_c" asc nulls first
```
