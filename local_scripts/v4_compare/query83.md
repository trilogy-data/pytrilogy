# Query 83

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (24 rows) |
| reference execution | OK (24 rows) |
| results identical | YES |

## Result comparison

v4 rows: 24 (24 distinct)
ref rows: 24 (24 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 7865 | 173 | 50.93 ms |
| reference | 7865 | 173 | 45.10 ms |
| v4 / ref | 1.00x | 1.00x | 1.13x |

## Preql

```
import all_sales as sales;
import date as date;

auto target_week_seqs <- date.week_seq ? date.date in ('2000-06-30'::date, '2000-09-27'::date, '2000-11-17'::date);

# Per-channel return quantities by item.text_id; the week_seq filter applies to
# the return date.
def channel_qty(channel) -> sum(sales.return_quantity ? sales.sales_channel = channel) by sales.item.text_id;
def channel_present(channel) -> count(sales.order_id ? sales.sales_channel = channel) by sales.item.text_id;

auto sr_item_qty <- sum(sales.return_quantity ? sales.sales_channel = 'STORE') by sales.item.text_id;
auto cr_item_qty <- sum(sales.return_quantity ? sales.sales_channel = 'CATALOG') by sales.item.text_id;
auto wr_item_qty <- sum(sales.return_quantity ? sales.sales_channel = 'WEB') by sales.item.text_id;
auto sr_item_present <- count(sales.order_id ? sales.sales_channel = 'STORE') by sales.item.text_id;
auto cr_item_present <- count(sales.order_id ? sales.sales_channel = 'CATALOG') by sales.item.text_id;
auto wr_item_present <- count(sales.order_id ? sales.sales_channel = 'WEB') by sales.item.text_id;
auto total_qty <- sr_item_qty + cr_item_qty + wr_item_qty;
auto avg_qty <- total_qty / 3.0;
auto sr_dev <- (sr_item_qty * 1.0) / total_qty / 3.0 * 100;
auto cr_dev <- (cr_item_qty * 1.0) / total_qty / 3.0 * 100;
auto wr_dev <- (wr_item_qty * 1.0) / total_qty / 3.0 * 100;

where
    sales.return_date.week_seq in target_week_seqs
select
    sales.item.text_id as item_id,
    sr_item_qty,
    sr_dev,
    cr_item_qty,
    cr_dev,
    wr_item_qty,
    wr_dev,
    avg_qty as average,
    --sr_item_present,
    --cr_item_present,
    --wr_item_present,
having
    sr_item_present > 0 and cr_item_present > 0 and wr_item_present > 0

order by
    item_id asc nulls first,
    sr_item_qty asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
highfalutin as (
SELECT
    CASE WHEN cast("date_date"."D_DATE" as date) in (date '2000-06-30',date '2000-09-27',date '2000-11-17') THEN "date_date"."D_WEEK_SEQ" ELSE NULL END as "target_week_seqs"
FROM
    "memory"."date_dim" as "date_date"),
cooperative as (
SELECT
    "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
    "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_returns_unified"."CR_RETURN_QUANTITY" as "sales_return_quantity",
     'CATALOG'  as "sales_sales_channel",
    "sales_return_date_date"."D_WEEK_SEQ" as "sales_return_date_week_seq"
FROM
    "memory"."catalog_returns" as "sales_catalog_returns_unified"
    INNER JOIN "memory"."date_dim" as "sales_return_date_date" on "sales_catalog_returns_unified"."CR_RETURNED_DATE_SK" = "sales_return_date_date"."D_DATE_SK"
WHERE
    "sales_return_date_date"."D_WEEK_SEQ" in (select highfalutin."target_week_seqs" from highfalutin where highfalutin."target_week_seqs" is not null)

UNION ALL
SELECT
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
    "sales_store_returns_unified"."SR_RETURN_QUANTITY" as "sales_return_quantity",
     'STORE'  as "sales_sales_channel",
    "sales_return_date_date"."D_WEEK_SEQ" as "sales_return_date_week_seq"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"
    INNER JOIN "memory"."date_dim" as "sales_return_date_date" on "sales_store_returns_unified"."SR_RETURNED_DATE_SK" = "sales_return_date_date"."D_DATE_SK"
WHERE
    "sales_return_date_date"."D_WEEK_SEQ" in (select highfalutin."target_week_seqs" from highfalutin where highfalutin."target_week_seqs" is not null)

UNION ALL
SELECT
    "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
    "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
    "sales_web_returns_unified"."WR_RETURN_QUANTITY" as "sales_return_quantity",
     'WEB'  as "sales_sales_channel",
    "sales_return_date_date"."D_WEEK_SEQ" as "sales_return_date_week_seq"
FROM
    "memory"."web_returns" as "sales_web_returns_unified"
    INNER JOIN "memory"."date_dim" as "sales_return_date_date" on "sales_web_returns_unified"."WR_RETURNED_DATE_SK" = "sales_return_date_date"."D_DATE_SK"
WHERE
    "sales_return_date_date"."D_WEEK_SEQ" in (select highfalutin."target_week_seqs" from highfalutin where highfalutin."target_week_seqs" is not null)
),
uneven as (
SELECT
    "cooperative"."sales_order_id" as "sales_order_id",
    "cooperative"."sales_return_date_week_seq" as "sales_return_date_week_seq",
    "cooperative"."sales_return_quantity" as "sales_return_quantity",
    "cooperative"."sales_sales_channel" as "sales_sales_channel",
    "sales_item_items"."I_ITEM_ID" as "sales_item_text_id"
FROM
    "memory"."item" as "sales_item_items"
    INNER JOIN "cooperative" on "sales_item_items"."I_ITEM_SK" = "cooperative"."sales_item_id"
WHERE
    "cooperative"."sales_return_date_week_seq" in (select highfalutin."target_week_seqs" from highfalutin where highfalutin."target_week_seqs" is not null)

GROUP BY
    1,
    2,
    3,
    4,
    5,
    "sales_item_items"."I_ITEM_SK"),
yummy as (
SELECT
    "uneven"."sales_item_text_id" as "sales_item_text_id",
    "uneven"."sales_order_id" as "sales_order_id",
    "uneven"."sales_return_quantity" as "sales_return_quantity",
    "uneven"."sales_sales_channel" as "sales_sales_channel"
FROM
    "uneven"
WHERE
    "uneven"."sales_return_date_week_seq" in (select highfalutin."target_week_seqs" from highfalutin where highfalutin."target_week_seqs" is not null)
),
juicy as (
SELECT
    "yummy"."sales_item_text_id" as "sales_item_text_id",
    "yummy"."sales_order_id" as "sales_order_id",
    CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN "yummy"."sales_order_id" ELSE NULL END as "_virt_filter_order_id_7518965045904948",
    CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN "yummy"."sales_return_quantity" ELSE NULL END as "_virt_filter_return_quantity_1904161637839137",
    CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_order_id" ELSE NULL END as "_virt_filter_order_id_5282889778133979",
    CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_return_quantity" ELSE NULL END as "_virt_filter_return_quantity_6293408465554798",
    CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_order_id" ELSE NULL END as "_virt_filter_order_id_4128599423878258",
    CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_return_quantity" ELSE NULL END as "_virt_filter_return_quantity_6234128225083739"
FROM
    "yummy"),
sparkling as (
SELECT
    "juicy"."_virt_filter_order_id_4128599423878258" as "_virt_filter_order_id_4128599423878258",
    "juicy"."_virt_filter_order_id_5282889778133979" as "_virt_filter_order_id_5282889778133979",
    "juicy"."_virt_filter_order_id_7518965045904948" as "_virt_filter_order_id_7518965045904948",
    "juicy"."sales_item_text_id" as "sales_item_text_id"
FROM
    "juicy"
GROUP BY
    1,
    2,
    3,
    4,
    "juicy"."sales_order_id"),
vacuous as (
SELECT
    "juicy"."sales_item_text_id" as "sales_item_text_id",
    sum("juicy"."_virt_filter_return_quantity_1904161637839137") as "cr_item_qty",
    sum("juicy"."_virt_filter_return_quantity_6234128225083739") as "wr_item_qty",
    sum("juicy"."_virt_filter_return_quantity_6293408465554798") as "sr_item_qty"
FROM
    "juicy"
GROUP BY
    1),
abhorrent as (
SELECT
    "sparkling"."sales_item_text_id" as "sales_item_text_id",
    count("sparkling"."_virt_filter_order_id_4128599423878258") as "wr_item_present",
    count("sparkling"."_virt_filter_order_id_5282889778133979") as "sr_item_present",
    count("sparkling"."_virt_filter_order_id_7518965045904948") as "cr_item_present"
FROM
    "sparkling"
GROUP BY
    1
HAVING
    "sr_item_present" > 0
),
young as (
SELECT
    "vacuous"."cr_item_qty" as "cr_item_qty",
    "vacuous"."sales_item_text_id" as "item_id",
    "vacuous"."sr_item_qty" as "sr_item_qty",
    "vacuous"."wr_item_qty" as "wr_item_qty",
    ( ( "vacuous"."sr_item_qty" + "vacuous"."cr_item_qty" ) + "vacuous"."wr_item_qty" ) / 3.0 as "average",
    ( ( ("vacuous"."cr_item_qty" * 1.0) / ( ( "vacuous"."sr_item_qty" + "vacuous"."cr_item_qty" ) + "vacuous"."wr_item_qty" ) ) / 3.0 ) * 100 as "cr_dev",
    ( ( ("vacuous"."sr_item_qty" * 1.0) / ( ( "vacuous"."sr_item_qty" + "vacuous"."cr_item_qty" ) + "vacuous"."wr_item_qty" ) ) / 3.0 ) * 100 as "sr_dev",
    ( ( ("vacuous"."wr_item_qty" * 1.0) / ( ( "vacuous"."sr_item_qty" + "vacuous"."cr_item_qty" ) + "vacuous"."wr_item_qty" ) ) / 3.0 ) * 100 as "wr_dev"
FROM
    "vacuous"),
sweltering as (
SELECT
    "abhorrent"."cr_item_present" as "cr_item_present",
    "abhorrent"."wr_item_present" as "wr_item_present",
    "young"."average" as "average",
    "young"."cr_dev" as "cr_dev",
    "young"."cr_item_qty" as "cr_item_qty",
    "young"."item_id" as "item_id",
    "young"."sr_dev" as "sr_dev",
    "young"."sr_item_qty" as "sr_item_qty",
    "young"."wr_dev" as "wr_dev",
    "young"."wr_item_qty" as "wr_item_qty"
FROM
    "abhorrent"
    INNER JOIN "young" on "abhorrent"."sales_item_text_id" = "young"."item_id"
WHERE
    "abhorrent"."sr_item_present" > 0
)
SELECT
    "sweltering"."item_id" as "item_id",
    "sweltering"."sr_item_qty" as "sr_item_qty",
    "sweltering"."sr_dev" as "sr_dev",
    "sweltering"."cr_item_qty" as "cr_item_qty",
    "sweltering"."cr_dev" as "cr_dev",
    "sweltering"."wr_item_qty" as "wr_item_qty",
    "sweltering"."wr_dev" as "wr_dev",
    "sweltering"."average" as "average"
FROM
    "sweltering"
WHERE
    "sweltering"."cr_item_present" > 0 and "sweltering"."wr_item_present" > 0

ORDER BY 
    "sweltering"."item_id" asc nulls first,
    "sweltering"."sr_item_qty" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
highfalutin as (
SELECT
    CASE WHEN cast("date_date"."D_DATE" as date) in (date '2000-06-30',date '2000-09-27',date '2000-11-17') THEN "date_date"."D_WEEK_SEQ" ELSE NULL END as "target_week_seqs"
FROM
    "memory"."date_dim" as "date_date"),
cooperative as (
SELECT
    "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
    "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_returns_unified"."CR_RETURN_QUANTITY" as "sales_return_quantity",
     'CATALOG'  as "sales_sales_channel",
    "sales_return_date_date"."D_WEEK_SEQ" as "sales_return_date_week_seq"
FROM
    "memory"."catalog_returns" as "sales_catalog_returns_unified"
    INNER JOIN "memory"."date_dim" as "sales_return_date_date" on "sales_catalog_returns_unified"."CR_RETURNED_DATE_SK" = "sales_return_date_date"."D_DATE_SK"
WHERE
    "sales_return_date_date"."D_WEEK_SEQ" in (select highfalutin."target_week_seqs" from highfalutin where highfalutin."target_week_seqs" is not null)

UNION ALL
SELECT
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
    "sales_store_returns_unified"."SR_RETURN_QUANTITY" as "sales_return_quantity",
     'STORE'  as "sales_sales_channel",
    "sales_return_date_date"."D_WEEK_SEQ" as "sales_return_date_week_seq"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"
    INNER JOIN "memory"."date_dim" as "sales_return_date_date" on "sales_store_returns_unified"."SR_RETURNED_DATE_SK" = "sales_return_date_date"."D_DATE_SK"
WHERE
    "sales_return_date_date"."D_WEEK_SEQ" in (select highfalutin."target_week_seqs" from highfalutin where highfalutin."target_week_seqs" is not null)

UNION ALL
SELECT
    "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
    "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
    "sales_web_returns_unified"."WR_RETURN_QUANTITY" as "sales_return_quantity",
     'WEB'  as "sales_sales_channel",
    "sales_return_date_date"."D_WEEK_SEQ" as "sales_return_date_week_seq"
FROM
    "memory"."web_returns" as "sales_web_returns_unified"
    INNER JOIN "memory"."date_dim" as "sales_return_date_date" on "sales_web_returns_unified"."WR_RETURNED_DATE_SK" = "sales_return_date_date"."D_DATE_SK"
WHERE
    "sales_return_date_date"."D_WEEK_SEQ" in (select highfalutin."target_week_seqs" from highfalutin where highfalutin."target_week_seqs" is not null)
),
uneven as (
SELECT
    "cooperative"."sales_order_id" as "sales_order_id",
    "cooperative"."sales_return_date_week_seq" as "sales_return_date_week_seq",
    "cooperative"."sales_return_quantity" as "sales_return_quantity",
    "cooperative"."sales_sales_channel" as "sales_sales_channel",
    "sales_item_items"."I_ITEM_ID" as "sales_item_text_id"
FROM
    "memory"."item" as "sales_item_items"
    INNER JOIN "cooperative" on "sales_item_items"."I_ITEM_SK" = "cooperative"."sales_item_id"
WHERE
    "cooperative"."sales_return_date_week_seq" in (select highfalutin."target_week_seqs" from highfalutin where highfalutin."target_week_seqs" is not null)

GROUP BY
    1,
    2,
    3,
    4,
    5,
    "sales_item_items"."I_ITEM_SK"),
yummy as (
SELECT
    "uneven"."sales_item_text_id" as "sales_item_text_id",
    "uneven"."sales_order_id" as "sales_order_id",
    "uneven"."sales_return_quantity" as "sales_return_quantity",
    "uneven"."sales_sales_channel" as "sales_sales_channel"
FROM
    "uneven"
WHERE
    "uneven"."sales_return_date_week_seq" in (select highfalutin."target_week_seqs" from highfalutin where highfalutin."target_week_seqs" is not null)
),
juicy as (
SELECT
    "yummy"."sales_item_text_id" as "sales_item_text_id",
    "yummy"."sales_order_id" as "sales_order_id",
    CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN "yummy"."sales_order_id" ELSE NULL END as "_virt_filter_order_id_7518965045904948",
    CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN "yummy"."sales_return_quantity" ELSE NULL END as "_virt_filter_return_quantity_1904161637839137",
    CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_order_id" ELSE NULL END as "_virt_filter_order_id_5282889778133979",
    CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_return_quantity" ELSE NULL END as "_virt_filter_return_quantity_6293408465554798",
    CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_order_id" ELSE NULL END as "_virt_filter_order_id_4128599423878258",
    CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_return_quantity" ELSE NULL END as "_virt_filter_return_quantity_6234128225083739"
FROM
    "yummy"),
sparkling as (
SELECT
    "juicy"."_virt_filter_order_id_4128599423878258" as "_virt_filter_order_id_4128599423878258",
    "juicy"."_virt_filter_order_id_5282889778133979" as "_virt_filter_order_id_5282889778133979",
    "juicy"."_virt_filter_order_id_7518965045904948" as "_virt_filter_order_id_7518965045904948",
    "juicy"."sales_item_text_id" as "sales_item_text_id"
FROM
    "juicy"
GROUP BY
    1,
    2,
    3,
    4,
    "juicy"."sales_order_id"),
vacuous as (
SELECT
    "juicy"."sales_item_text_id" as "sales_item_text_id",
    sum("juicy"."_virt_filter_return_quantity_1904161637839137") as "cr_item_qty",
    sum("juicy"."_virt_filter_return_quantity_6234128225083739") as "wr_item_qty",
    sum("juicy"."_virt_filter_return_quantity_6293408465554798") as "sr_item_qty"
FROM
    "juicy"
GROUP BY
    1),
abhorrent as (
SELECT
    "sparkling"."sales_item_text_id" as "sales_item_text_id",
    count("sparkling"."_virt_filter_order_id_4128599423878258") as "wr_item_present",
    count("sparkling"."_virt_filter_order_id_5282889778133979") as "sr_item_present",
    count("sparkling"."_virt_filter_order_id_7518965045904948") as "cr_item_present"
FROM
    "sparkling"
GROUP BY
    1
HAVING
    "sr_item_present" > 0
),
young as (
SELECT
    "vacuous"."cr_item_qty" as "cr_item_qty",
    "vacuous"."sales_item_text_id" as "item_id",
    "vacuous"."sr_item_qty" as "sr_item_qty",
    "vacuous"."wr_item_qty" as "wr_item_qty",
    ( ( "vacuous"."sr_item_qty" + "vacuous"."cr_item_qty" ) + "vacuous"."wr_item_qty" ) / 3.0 as "average",
    ( ( ("vacuous"."cr_item_qty" * 1.0) / ( ( "vacuous"."sr_item_qty" + "vacuous"."cr_item_qty" ) + "vacuous"."wr_item_qty" ) ) / 3.0 ) * 100 as "cr_dev",
    ( ( ("vacuous"."sr_item_qty" * 1.0) / ( ( "vacuous"."sr_item_qty" + "vacuous"."cr_item_qty" ) + "vacuous"."wr_item_qty" ) ) / 3.0 ) * 100 as "sr_dev",
    ( ( ("vacuous"."wr_item_qty" * 1.0) / ( ( "vacuous"."sr_item_qty" + "vacuous"."cr_item_qty" ) + "vacuous"."wr_item_qty" ) ) / 3.0 ) * 100 as "wr_dev"
FROM
    "vacuous"),
sweltering as (
SELECT
    "abhorrent"."cr_item_present" as "cr_item_present",
    "abhorrent"."wr_item_present" as "wr_item_present",
    "young"."average" as "average",
    "young"."cr_dev" as "cr_dev",
    "young"."cr_item_qty" as "cr_item_qty",
    "young"."item_id" as "item_id",
    "young"."sr_dev" as "sr_dev",
    "young"."sr_item_qty" as "sr_item_qty",
    "young"."wr_dev" as "wr_dev",
    "young"."wr_item_qty" as "wr_item_qty"
FROM
    "abhorrent"
    INNER JOIN "young" on "abhorrent"."sales_item_text_id" = "young"."item_id"
WHERE
    "abhorrent"."sr_item_present" > 0
)
SELECT
    "sweltering"."item_id" as "item_id",
    "sweltering"."sr_item_qty" as "sr_item_qty",
    "sweltering"."sr_dev" as "sr_dev",
    "sweltering"."cr_item_qty" as "cr_item_qty",
    "sweltering"."cr_dev" as "cr_dev",
    "sweltering"."wr_item_qty" as "wr_item_qty",
    "sweltering"."wr_dev" as "wr_dev",
    "sweltering"."average" as "average"
FROM
    "sweltering"
WHERE
    "sweltering"."cr_item_present" > 0 and "sweltering"."wr_item_present" > 0

ORDER BY 
    "sweltering"."item_id" asc nulls first,
    "sweltering"."sr_item_qty" asc nulls first
LIMIT (100)
```
