# Query 83

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (24 rows) |
| results identical | NO |

## Result comparison

v4 rows: 100 (100 distinct)
ref rows: 24 (24 distinct)
only in v4 (showing up to 5 of 100):
  1x  (154.66666666666666, 16.020114942528735, 223, 'AAAAAAAAAAABAAAA', 14.870689655172415, 207, 2.442528735632184, 34)
  1x  (446.0, 21.94818136522172, 881, 'AAAAAAAAAAABAAAA', 6.203288490284006, 249, 5.181863477827603, 208)
  1x  (248.33333333333334, 15.794183445190157, 353, 'AAAAAAAAAAABAAAA', 12.214765100671142, 273, 5.324384787472036, 119)
  1x  (378.0, 22.192827748383305, 755, 'AAAAAAAAAAABAAAA', 8.259847148736037, 281, 2.8806584362139915, 98)
  1x  (304.6666666666667, 7.950401167031364, 218, 'AAAAAAAAAAABAAAA', 10.612691466083152, 291, 14.770240700218821, 405)
only in ref (showing up to 5 of 24):
  1x  (48.333333333333336, 15.862068965517242, 69, 'AAAAAAAAAHFBAAAA', 17.24137931034483, 75, 0.22988505747126436, 1)
  1x  (22.666666666666668, 6.862745098039215, 14, 'AAAAAAAACLJBAAAA', 25.0, 51, 1.4705882352941178, 3)
  1x  (20.666666666666668, 13.440860215053762, 25, 'AAAAAAAACOPBAAAA', 5.913978494623656, 11, 13.978494623655916, 26)
  1x  (28.333333333333332, 1.9607843137254901, 5, 'AAAAAAAADJABAAAA', 27.84313725490196, 71, 3.5294117647058822, 9)
  1x  (34.333333333333336, 0.6472491909385113, 2, 'AAAAAAAADLOAAAAA', 30.42071197411003, 94, 2.2653721682847894, 7)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 5085 | 116 | 1.377 s |
| reference | 5946 | 132 | 73.60 ms |
| v4 / ref | 0.86x | 0.88x | 18.71x |

## Preql

```
import unified_sales as sales;
import date as date;

auto target_week_seqs <- date.week_seq ? date.date in ('2000-06-30'::date, '2000-09-27'::date, '2000-11-17'::date);

# Per-channel return quantities by item.name; the week_seq filter applies to
# the return date.
def channel_qty(channel) -> sum(sales.return_quantity ? sales.sales_channel = channel) by sales.item.name;
def channel_present(channel) -> count(sales.order_id ? sales.sales_channel = channel) by sales.item.name;

auto sr_item_qty <- sum(sales.return_quantity ? sales.sales_channel = 'STORE') by sales.item.name;
auto cr_item_qty <- sum(sales.return_quantity ? sales.sales_channel = 'CATALOG') by sales.item.name;
auto wr_item_qty <- sum(sales.return_quantity ? sales.sales_channel = 'WEB') by sales.item.name;
auto sr_item_present <- count(sales.order_id ? sales.sales_channel = 'STORE') by sales.item.name;
auto cr_item_present <- count(sales.order_id ? sales.sales_channel = 'CATALOG') by sales.item.name;
auto wr_item_present <- count(sales.order_id ? sales.sales_channel = 'WEB') by sales.item.name;
auto total_qty <- sr_item_qty + cr_item_qty + wr_item_qty;
auto avg_qty <- total_qty / 3.0;
auto sr_dev <- (sr_item_qty * 1.0) / total_qty / 3.0 * 100;
auto cr_dev <- (cr_item_qty * 1.0) / total_qty / 3.0 * 100;
auto wr_dev <- (wr_item_qty * 1.0) / total_qty / 3.0 * 100;

where
    sales.return_date.week_seq in target_week_seqs
select
    sales.item.name as item_id,
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
cooperative as (
SELECT
    "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
    "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_returns_unified"."CR_RETURNED_DATE_SK" as "sales_return_date_id",
    "sales_catalog_returns_unified"."CR_RETURN_QUANTITY" as "sales_return_quantity",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_returns" as "sales_catalog_returns_unified"
UNION ALL
SELECT
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
    "sales_store_returns_unified"."SR_RETURNED_DATE_SK" as "sales_return_date_id",
    "sales_store_returns_unified"."SR_RETURN_QUANTITY" as "sales_return_quantity",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"
UNION ALL
SELECT
    "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
    "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
    "sales_web_returns_unified"."WR_RETURNED_DATE_SK" as "sales_return_date_id",
    "sales_web_returns_unified"."WR_RETURN_QUANTITY" as "sales_return_quantity",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_returns" as "sales_web_returns_unified"),
highfalutin as (
SELECT
    CASE WHEN cast("date_date"."D_DATE" as date) in (date '2000-06-30',date '2000-09-27',date '2000-11-17') THEN "date_date"."D_WEEK_SEQ" ELSE NULL END as "target_week_seqs"
FROM
    "memory"."date_dim" as "date_date"),
uneven as (
SELECT
    "cooperative"."sales_order_id" as "sales_order_id",
    "cooperative"."sales_return_quantity" as "sales_return_quantity",
    "cooperative"."sales_sales_channel" as "sales_sales_channel",
    "sales_item_items"."I_ITEM_ID" as "sales_item_name",
    "sales_return_date_date"."D_WEEK_SEQ" as "sales_return_date_week_seq"
FROM
    "memory"."item" as "sales_item_items"
    LEFT OUTER JOIN "cooperative" on "sales_item_items"."I_ITEM_SK" = "cooperative"."sales_item_id"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_return_date_date" on "cooperative"."sales_return_date_id" = "sales_return_date_date"."D_DATE_SK"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    "sales_item_items"."I_ITEM_SK"),
juicy as (
SELECT
    count(CASE WHEN "uneven"."sales_sales_channel" = 'CATALOG' THEN "uneven"."sales_order_id" ELSE NULL END) as "cr_item_present",
    count(CASE WHEN "uneven"."sales_sales_channel" = 'STORE' THEN "uneven"."sales_order_id" ELSE NULL END) as "sr_item_present",
    count(CASE WHEN "uneven"."sales_sales_channel" = 'WEB' THEN "uneven"."sales_order_id" ELSE NULL END) as "wr_item_present",
    sum(CASE WHEN "uneven"."sales_sales_channel" = 'CATALOG' THEN "uneven"."sales_return_quantity" ELSE NULL END) as "cr_item_qty",
    sum(CASE WHEN "uneven"."sales_sales_channel" = 'STORE' THEN "uneven"."sales_return_quantity" ELSE NULL END) as "sr_item_qty",
    sum(CASE WHEN "uneven"."sales_sales_channel" = 'WEB' THEN "uneven"."sales_return_quantity" ELSE NULL END) as "wr_item_qty"
FROM
    "uneven"
GROUP BY
    "uneven"."sales_item_name"),
yummy as (
SELECT
    "uneven"."sales_item_name" as "item_id"
FROM
    "uneven"
WHERE
    "uneven"."sales_return_date_week_seq" in (select highfalutin."target_week_seqs" from highfalutin where highfalutin."target_week_seqs" is not null)
),
concerned as (
SELECT
    "juicy"."cr_item_present" as "cr_item_present",
    "juicy"."cr_item_qty" as "cr_item_qty",
    "juicy"."sr_item_present" as "sr_item_present",
    "juicy"."sr_item_qty" as "sr_item_qty",
    "juicy"."wr_item_present" as "wr_item_present",
    "juicy"."wr_item_qty" as "wr_item_qty"
FROM
    "juicy"
WHERE
    "juicy"."sr_item_present" > 0
),
young as (
SELECT
    "concerned"."cr_item_qty" as "cr_item_qty",
    "concerned"."sr_item_qty" as "sr_item_qty",
    "concerned"."wr_item_qty" as "wr_item_qty",
    "yummy"."item_id" as "item_id",
    coalesce("concerned"."cr_item_present",0) as "cr_item_present",
    coalesce("concerned"."wr_item_present",0) as "wr_item_present"
FROM
    "concerned"
    LEFT OUTER JOIN "yummy" on 1=1
WHERE
    coalesce("concerned"."sr_item_present",0) > 0
)
SELECT
    "young"."item_id" as "item_id",
    "young"."sr_item_qty" as "sr_item_qty",
    ( ( ("young"."sr_item_qty" * 1.0) / ( ( "young"."sr_item_qty" + "young"."cr_item_qty" ) + "young"."wr_item_qty" ) ) / 3.0 ) * 100 as "sr_dev",
    "young"."cr_item_qty" as "cr_item_qty",
    ( ( ("young"."cr_item_qty" * 1.0) / ( ( "young"."sr_item_qty" + "young"."cr_item_qty" ) + "young"."wr_item_qty" ) ) / 3.0 ) * 100 as "cr_dev",
    "young"."wr_item_qty" as "wr_item_qty",
    ( ( ("young"."wr_item_qty" * 1.0) / ( ( "young"."sr_item_qty" + "young"."cr_item_qty" ) + "young"."wr_item_qty" ) ) / 3.0 ) * 100 as "wr_dev",
    ( ( "young"."sr_item_qty" + "young"."cr_item_qty" ) + "young"."wr_item_qty" ) / 3.0 as "average"
FROM
    "young"
WHERE
    "young"."cr_item_present" > 0 and "young"."wr_item_present" > 0

ORDER BY 
    "young"."item_id" asc nulls first,
    "young"."sr_item_qty" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
quizzical as (
SELECT
    CASE WHEN cast("date_date"."D_DATE" as date) in (date '2000-06-30',date '2000-09-27',date '2000-11-17') THEN "date_date"."D_WEEK_SEQ" ELSE NULL END as "target_week_seqs"
FROM
    "memory"."date_dim" as "date_date"
GROUP BY
    1),
questionable as (
SELECT
    "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
    "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
     'CATALOG'  as "sales_sales_channel",
    "sales_catalog_returns_unified"."CR_RETURN_QUANTITY" as "sales_return_quantity"
FROM
    "memory"."catalog_returns" as "sales_catalog_returns_unified"
    INNER JOIN "memory"."date_dim" as "sales_return_date_date" on "sales_catalog_returns_unified"."CR_RETURNED_DATE_SK" = "sales_return_date_date"."D_DATE_SK"
WHERE
    "sales_return_date_date"."D_WEEK_SEQ" in (select quizzical."target_week_seqs" from quizzical where quizzical."target_week_seqs" is not null)

UNION ALL
SELECT
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
     'STORE'  as "sales_sales_channel",
    "sales_store_returns_unified"."SR_RETURN_QUANTITY" as "sales_return_quantity"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"
    INNER JOIN "memory"."date_dim" as "sales_return_date_date" on "sales_store_returns_unified"."SR_RETURNED_DATE_SK" = "sales_return_date_date"."D_DATE_SK"
WHERE
    "sales_return_date_date"."D_WEEK_SEQ" in (select quizzical."target_week_seqs" from quizzical where quizzical."target_week_seqs" is not null)

UNION ALL
SELECT
    "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
    "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
     'WEB'  as "sales_sales_channel",
    "sales_web_returns_unified"."WR_RETURN_QUANTITY" as "sales_return_quantity"
FROM
    "memory"."web_returns" as "sales_web_returns_unified"
    INNER JOIN "memory"."date_dim" as "sales_return_date_date" on "sales_web_returns_unified"."WR_RETURNED_DATE_SK" = "sales_return_date_date"."D_DATE_SK"
WHERE
    "sales_return_date_date"."D_WEEK_SEQ" in (select quizzical."target_week_seqs" from quizzical where quizzical."target_week_seqs" is not null)
),
concerned as (
SELECT
    "sales_item_items"."I_ITEM_ID" as "sales_item_name",
    CASE WHEN "questionable"."sales_sales_channel" = 'CATALOG' THEN "questionable"."sales_order_id" ELSE NULL END as "_virt_filter_order_id_7518965045904948",
    CASE WHEN "questionable"."sales_sales_channel" = 'STORE' THEN "questionable"."sales_order_id" ELSE NULL END as "_virt_filter_order_id_5282889778133979",
    CASE WHEN "questionable"."sales_sales_channel" = 'WEB' THEN "questionable"."sales_order_id" ELSE NULL END as "_virt_filter_order_id_4128599423878258"
FROM
    "memory"."item" as "sales_item_items"
    LEFT OUTER JOIN "questionable" on "sales_item_items"."I_ITEM_SK" = "questionable"."sales_item_id"
GROUP BY
    1,
    2,
    3,
    4,
    "questionable"."sales_order_id"),
yummy as (
SELECT
    "questionable"."sales_return_quantity" as "sales_return_quantity",
    "questionable"."sales_sales_channel" as "sales_sales_channel",
    "sales_item_items"."I_ITEM_ID" as "sales_item_name"
FROM
    "memory"."item" as "sales_item_items"
    LEFT OUTER JOIN "questionable" on "sales_item_items"."I_ITEM_SK" = "questionable"."sales_item_id"
GROUP BY
    1,
    2,
    3,
    "questionable"."sales_order_id",
    "sales_item_items"."I_ITEM_SK"),
abhorrent as (
SELECT
    "concerned"."sales_item_name" as "sales_item_name",
    count("concerned"."_virt_filter_order_id_4128599423878258") as "wr_item_present",
    count("concerned"."_virt_filter_order_id_5282889778133979") as "sr_item_present",
    count("concerned"."_virt_filter_order_id_7518965045904948") as "cr_item_present"
FROM
    "concerned"
GROUP BY
    1
HAVING
    "sr_item_present" > 0
),
juicy as (
SELECT
    "yummy"."sales_item_name" as "sales_item_name",
    sum(CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN "yummy"."sales_return_quantity" ELSE NULL END) as "cr_item_qty",
    sum(CASE WHEN "yummy"."sales_sales_channel" = 'STORE' THEN "yummy"."sales_return_quantity" ELSE NULL END) as "sr_item_qty",
    sum(CASE WHEN "yummy"."sales_sales_channel" = 'WEB' THEN "yummy"."sales_return_quantity" ELSE NULL END) as "wr_item_qty"
FROM
    "yummy"
GROUP BY
    1),
sweltering as (
SELECT
    "abhorrent"."cr_item_present" as "cr_item_present",
    "abhorrent"."wr_item_present" as "wr_item_present",
    "juicy"."cr_item_qty" as "cr_item_qty",
    "juicy"."sales_item_name" as "item_id",
    "juicy"."sr_item_qty" as "sr_item_qty",
    "juicy"."wr_item_qty" as "wr_item_qty",
    ( ( "juicy"."sr_item_qty" + "juicy"."cr_item_qty" ) + "juicy"."wr_item_qty" ) / 3.0 as "average",
    ( ( ("juicy"."cr_item_qty" * 1.0) / ( ( "juicy"."sr_item_qty" + "juicy"."cr_item_qty" ) + "juicy"."wr_item_qty" ) ) / 3.0 ) * 100 as "cr_dev",
    ( ( ("juicy"."sr_item_qty" * 1.0) / ( ( "juicy"."sr_item_qty" + "juicy"."cr_item_qty" ) + "juicy"."wr_item_qty" ) ) / 3.0 ) * 100 as "sr_dev",
    ( ( ("juicy"."wr_item_qty" * 1.0) / ( ( "juicy"."sr_item_qty" + "juicy"."cr_item_qty" ) + "juicy"."wr_item_qty" ) ) / 3.0 ) * 100 as "wr_dev"
FROM
    "abhorrent"
    INNER JOIN "juicy" on "abhorrent"."sales_item_name" = "juicy"."sales_item_name"
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
