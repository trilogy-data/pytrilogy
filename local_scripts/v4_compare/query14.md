# Query 14

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
| v4 | 10897 | 264 | 33.16 ms |
| reference | 6203 | 160 | 26.50 ms |
| v4 / ref | 1.76x | 1.65x | 1.25x |

## Preql

```
import all_sales as sales;

# Composite key for (brand_id, class_id, category_id) tuples â€” encoded so a
# single-column `in` filter can carry membership downstream.
auto tuple_key <- concat(
    sales.item.brand_id::string,
    '|',
    sales.item.class_id::string,
    '|',
    sales.item.category_id::string
);
# cross_tuples: tuples sold in all 3 channels during 1999-2001. Equivalent to
# the reference's 3-way INTERSECT on (i_brand_id, i_class_id, i_category_id)
# across store/catalog/web sales.
rowset cross_tuples <- where
    sales.date.year between 1999 and 2001
select
    tuple_key as ci_tuple_key,
    --cross_channel_count,
having
    cross_channel_count = 3

;

# avg_sales: scalar avg(quantity * list_price) over all 3 channels for 1999-2001.
rowset avg_sales <- where
    sales.date.year between 1999 and 2001
select
    avg(sales.quantity * sales.list_price) as average_sales,
;

# One per fact row (all_sales grain is (sales_channel, order_id, item.id)).
auto channel_label <- case
    when sales.sales_channel = 'STORE' then 'store'
    when sales.sales_channel = 'CATALOG' then 'catalog'
    when sales.sales_channel = 'WEB' then 'web'
    else null
end;

# Map all_sales' uppercase channel codes to the reference's lowercase labels.
auto cross_channel_count <- count_distinct(sales.sales_channel) by tuple_key;

# Per (channel, brand_id, class_id, category_id) aggregates for Nov 2001, items in
# cross_tuples, HAVING sum(quantity*list_price) > avg_sales.
rowset l0_filtered <- where
    sales.date.year = 2001
    and sales.date.month_of_year = 11
    and tuple_key in cross_tuples.ci_tuple_key
select
    channel_label as channel_l0,
    sales.item.brand_id as brand_l0,
    sales.item.class_id as class_l0,
    sales.item.category_id as category_l0,
    sum(sales.quantity * sales.list_price) as bucket_sum_l0,
    sum(sales.row_one) as bucket_count_l0,
    --avg_sales.average_sales,
having
    bucket_sum_l0 > avg_sales.average_sales

;

# Final ROLLUP(channel, brand_id, class_id, category_id) over the per-channel L0.
select
    l0_filtered.channel_l0 as channel,
    l0_filtered.brand_l0 as i_brand_id,
    l0_filtered.class_l0 as i_class_id,
    l0_filtered.category_l0 as i_category_id,
    sum(l0_filtered.bucket_sum_l0)
            by rollup() as sum_sales,
    sum(l0_filtered.bucket_count_l0)
            by rollup() as sum_number_sales,
order by
    channel asc nulls first,
    i_brand_id asc nulls first,
    i_class_id asc nulls first,
    i_category_id asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
abundant as (
SELECT
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_LIST_PRICE" as "sales_list_price",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     'CATALOG'  as "sales_sales_channel",
     1  as "sales_row_one"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_LIST_PRICE" as "sales_list_price",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
     'STORE'  as "sales_sales_channel",
     1  as "sales_row_one"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_LIST_PRICE" as "sales_list_price",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     'WEB'  as "sales_sales_channel",
     1  as "sales_row_one"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
cheerful as (
SELECT
    "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
    "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_returns" as "sales_catalog_returns_unified"
UNION ALL
SELECT
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"
UNION ALL
SELECT
    "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
    "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_returns" as "sales_web_returns_unified"),
busy as (
SELECT
    avg("abundant"."sales_quantity" * "abundant"."sales_list_price") as "_avg_sales_average_sales"
FROM
    "abundant"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "abundant"."sales_date_id" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" BETWEEN 1999 AND 2001
),
sweltering as (
SELECT
    "abundant"."sales_item_id" as "sales_item_id",
    "abundant"."sales_list_price" as "sales_list_price",
    "abundant"."sales_order_id" as "sales_order_id",
    "abundant"."sales_quantity" as "sales_quantity",
    "abundant"."sales_row_one" as "sales_row_one",
    "abundant"."sales_sales_channel" as "sales_sales_channel",
    "sales_item_items"."I_BRAND_ID" as "sales_item_brand_id",
    "sales_item_items"."I_CATEGORY_ID" as "sales_item_category_id",
    "sales_item_items"."I_CLASS_ID" as "sales_item_class_id",
    (cast("sales_item_items"."I_BRAND_ID" as string) || '|' || cast("sales_item_items"."I_CLASS_ID" as string) || '|' || cast("sales_item_items"."I_CATEGORY_ID" as string)) as "tuple_key",
    CASE
	WHEN "abundant"."sales_sales_channel" = 'STORE' THEN 'store'
	WHEN "abundant"."sales_sales_channel" = 'CATALOG' THEN 'catalog'
	WHEN "abundant"."sales_sales_channel" = 'WEB' THEN 'web'
	ELSE null
	END as "channel_label"
FROM
    "abundant"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "abundant"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "abundant"."sales_item_id" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 11
),
juicy as (
SELECT
    "abundant"."sales_sales_channel" as "sales_sales_channel",
    (cast("sales_item_items"."I_BRAND_ID" as string) || '|' || cast("sales_item_items"."I_CLASS_ID" as string) || '|' || cast("sales_item_items"."I_CATEGORY_ID" as string)) as "tuple_key"
FROM
    "abundant"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "abundant"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "cheerful" on "abundant"."sales_item_id" = "cheerful"."sales_item_id" AND "abundant"."sales_order_id" = "cheerful"."sales_order_id" AND "abundant"."sales_sales_channel" = "cheerful"."sales_sales_channel"
    INNER JOIN "memory"."item" as "sales_item_items" on "abundant"."sales_item_id" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_date_date"."D_YEAR" BETWEEN 1999 AND 2001

GROUP BY
    1,
    2),
protective as (
SELECT
    "busy"."_avg_sales_average_sales" as "avg_sales_average_sales"
FROM
    "busy"),
young as (
SELECT
    "juicy"."tuple_key" as "_cross_tuples_ci_tuple_key"
FROM
    "juicy"
GROUP BY
    1
HAVING
    count(distinct "juicy"."sales_sales_channel") = 3
),
sparkling as (
SELECT
    "young"."_cross_tuples_ci_tuple_key" as "_cross_tuples_ci_tuple_key"
FROM
    "young"),
abhorrent as (
SELECT
    "sparkling"."_cross_tuples_ci_tuple_key" as "cross_tuples_ci_tuple_key"
FROM
    "sparkling"),
macho as (
SELECT
    "sweltering"."channel_label" as "channel_label",
    "sweltering"."sales_item_brand_id" as "sales_item_brand_id",
    "sweltering"."sales_item_category_id" as "sales_item_category_id",
    "sweltering"."sales_item_class_id" as "sales_item_class_id",
    "sweltering"."sales_item_id" as "sales_item_id",
    "sweltering"."sales_list_price" as "sales_list_price",
    "sweltering"."sales_order_id" as "sales_order_id",
    "sweltering"."sales_quantity" as "sales_quantity",
    "sweltering"."sales_row_one" as "sales_row_one",
    "sweltering"."sales_sales_channel" as "sales_sales_channel"
FROM
    "sweltering"
WHERE
    "sweltering"."tuple_key" in (select abhorrent."cross_tuples_ci_tuple_key" from abhorrent where abhorrent."cross_tuples_ci_tuple_key" is not null)
),
friendly as (
SELECT
    "macho"."channel_label" as "channel_label",
    "macho"."sales_item_brand_id" as "sales_item_brand_id",
    "macho"."sales_item_category_id" as "sales_item_category_id",
    "macho"."sales_item_class_id" as "sales_item_class_id",
    "macho"."sales_row_one" as "sales_row_one"
FROM
    "macho"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    "macho"."sales_item_id",
    "macho"."sales_order_id",
    "macho"."sales_sales_channel"),
scrawny as (
SELECT
    "macho"."channel_label" as "_l0_filtered_channel_l0",
    "macho"."sales_item_brand_id" as "_l0_filtered_brand_l0",
    "macho"."sales_item_category_id" as "_l0_filtered_category_l0",
    "macho"."sales_item_class_id" as "_l0_filtered_class_l0",
    sum("macho"."sales_quantity" * "macho"."sales_list_price") as "_l0_filtered_bucket_sum_l0"
FROM
    "macho"
GROUP BY
    1,
    2,
    3,
    4),
kaput as (
SELECT
    "friendly"."channel_label" as "_l0_filtered_channel_l0",
    "friendly"."sales_item_brand_id" as "_l0_filtered_brand_l0",
    "friendly"."sales_item_category_id" as "_l0_filtered_category_l0",
    "friendly"."sales_item_class_id" as "_l0_filtered_class_l0",
    sum("friendly"."sales_row_one") as "_l0_filtered_bucket_count_l0"
FROM
    "friendly"
GROUP BY
    1,
    2,
    3,
    4),
divergent as (
SELECT
    "kaput"."_l0_filtered_bucket_count_l0" as "_l0_filtered_bucket_count_l0",
    "scrawny"."_l0_filtered_bucket_sum_l0" as "_l0_filtered_bucket_sum_l0",
    coalesce("kaput"."_l0_filtered_brand_l0","scrawny"."_l0_filtered_brand_l0") as "_l0_filtered_brand_l0",
    coalesce("kaput"."_l0_filtered_category_l0","scrawny"."_l0_filtered_category_l0") as "_l0_filtered_category_l0",
    coalesce("kaput"."_l0_filtered_channel_l0","scrawny"."_l0_filtered_channel_l0") as "_l0_filtered_channel_l0",
    coalesce("kaput"."_l0_filtered_class_l0","scrawny"."_l0_filtered_class_l0") as "_l0_filtered_class_l0"
FROM
    "kaput"
    INNER JOIN "scrawny" on "kaput"."_l0_filtered_brand_l0" = "scrawny"."_l0_filtered_brand_l0" AND "kaput"."_l0_filtered_category_l0" is not distinct from "scrawny"."_l0_filtered_category_l0" AND "kaput"."_l0_filtered_channel_l0" is not distinct from "scrawny"."_l0_filtered_channel_l0" AND "kaput"."_l0_filtered_class_l0" is not distinct from "scrawny"."_l0_filtered_class_l0"),
premium as (
SELECT
    "divergent"."_l0_filtered_brand_l0" as "_l0_filtered_brand_l0",
    "divergent"."_l0_filtered_bucket_count_l0" as "_l0_filtered_bucket_count_l0",
    "divergent"."_l0_filtered_bucket_sum_l0" as "_l0_filtered_bucket_sum_l0",
    "divergent"."_l0_filtered_category_l0" as "_l0_filtered_category_l0",
    "divergent"."_l0_filtered_channel_l0" as "_l0_filtered_channel_l0",
    "divergent"."_l0_filtered_class_l0" as "_l0_filtered_class_l0"
FROM
    "protective"
    INNER JOIN "divergent" on 1=1
WHERE
    "divergent"."_l0_filtered_bucket_sum_l0" > "protective"."avg_sales_average_sales"
),
puzzled as (
SELECT
    "premium"."_l0_filtered_brand_l0" as "_l0_filtered_brand_l0",
    "premium"."_l0_filtered_bucket_count_l0" as "_l0_filtered_bucket_count_l0",
    "premium"."_l0_filtered_bucket_sum_l0" as "_l0_filtered_bucket_sum_l0",
    "premium"."_l0_filtered_category_l0" as "_l0_filtered_category_l0",
    "premium"."_l0_filtered_channel_l0" as "_l0_filtered_channel_l0",
    "premium"."_l0_filtered_class_l0" as "_l0_filtered_class_l0"
FROM
    "premium"),
waggish as (
SELECT
    "puzzled"."_l0_filtered_brand_l0" as "l0_filtered_brand_l0",
    "puzzled"."_l0_filtered_bucket_count_l0" as "l0_filtered_bucket_count_l0",
    "puzzled"."_l0_filtered_bucket_sum_l0" as "l0_filtered_bucket_sum_l0",
    "puzzled"."_l0_filtered_category_l0" as "l0_filtered_category_l0",
    "puzzled"."_l0_filtered_channel_l0" as "l0_filtered_channel_l0",
    "puzzled"."_l0_filtered_class_l0" as "l0_filtered_class_l0"
FROM
    "puzzled"),
rambunctious as (
SELECT
    "waggish"."l0_filtered_brand_l0" as "l0_filtered_brand_l0",
    "waggish"."l0_filtered_category_l0" as "l0_filtered_category_l0",
    "waggish"."l0_filtered_channel_l0" as "l0_filtered_channel_l0",
    "waggish"."l0_filtered_class_l0" as "l0_filtered_class_l0",
    sum("waggish"."l0_filtered_bucket_count_l0") as "sum_number_sales",
    sum("waggish"."l0_filtered_bucket_sum_l0") as "sum_sales"
FROM
    "waggish"
GROUP BY
    ROLLUP (3, 1, 4, 2))
SELECT
    "rambunctious"."l0_filtered_channel_l0" as "channel",
    "rambunctious"."l0_filtered_brand_l0" as "i_brand_id",
    "rambunctious"."l0_filtered_class_l0" as "i_class_id",
    "rambunctious"."l0_filtered_category_l0" as "i_category_id",
    "rambunctious"."sum_sales" as "sum_sales",
    "rambunctious"."sum_number_sales" as "sum_number_sales"
FROM
    "rambunctious"
ORDER BY 
    "channel" asc nulls first,
    "i_brand_id" asc nulls first,
    "i_class_id" asc nulls first,
    "i_category_id" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_LIST_PRICE" as "sales_list_price",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     'CATALOG'  as "sales_sales_channel",
     1  as "sales_row_one"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_LIST_PRICE" as "sales_list_price",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
     'STORE'  as "sales_sales_channel",
     1  as "sales_row_one"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_LIST_PRICE" as "sales_list_price",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     'WEB'  as "sales_sales_channel",
     1  as "sales_row_one"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
vacuous as (
SELECT
    avg("cheerful"."sales_quantity" * "cheerful"."sales_list_price") as "avg_sales_average_sales"
FROM
    "cheerful"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" BETWEEN 1999 AND 2001
),
questionable as (
SELECT
    "cheerful"."sales_sales_channel" as "sales_sales_channel",
    (cast("sales_item_items"."I_BRAND_ID" as string) || '|' || cast("sales_item_items"."I_CLASS_ID" as string) || '|' || cast("sales_item_items"."I_CATEGORY_ID" as string)) as "tuple_key"
FROM
    "cheerful"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "cheerful"."sales_item_id" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_date_date"."D_YEAR" BETWEEN 1999 AND 2001

GROUP BY
    1,
    2),
abundant as (
SELECT
    "questionable"."tuple_key" as "_cross_tuples_ci_tuple_key"
FROM
    "questionable"
GROUP BY
    1
HAVING
    count(distinct "questionable"."sales_sales_channel") = 3
),
uneven as (
SELECT
    "abundant"."_cross_tuples_ci_tuple_key" as "cross_tuples_ci_tuple_key"
FROM
    "abundant"),
yummy as (
SELECT
    "cheerful"."sales_list_price" as "sales_list_price",
    "cheerful"."sales_quantity" as "sales_quantity",
    "cheerful"."sales_row_one" as "sales_row_one",
    "sales_item_items"."I_BRAND_ID" as "sales_item_brand_id",
    "sales_item_items"."I_CATEGORY_ID" as "sales_item_category_id",
    "sales_item_items"."I_CLASS_ID" as "sales_item_class_id",
    CASE
	WHEN "cheerful"."sales_sales_channel" = 'STORE' THEN 'store'
	WHEN "cheerful"."sales_sales_channel" = 'CATALOG' THEN 'catalog'
	WHEN "cheerful"."sales_sales_channel" = 'WEB' THEN 'web'
	ELSE null
	END as "channel_label"
FROM
    "cheerful"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "cheerful"."sales_item_id" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 11 and (cast("sales_item_items"."I_BRAND_ID" as string) || '|' || cast("sales_item_items"."I_CLASS_ID" as string) || '|' || cast("sales_item_items"."I_CATEGORY_ID" as string)) in (select uneven."cross_tuples_ci_tuple_key" from uneven where uneven."cross_tuples_ci_tuple_key" is not null)

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    "cheerful"."sales_item_id",
    "cheerful"."sales_order_id",
    "cheerful"."sales_sales_channel"),
juicy as (
SELECT
    "yummy"."channel_label" as "_l0_filtered_channel_l0",
    "yummy"."sales_item_brand_id" as "_l0_filtered_brand_l0",
    "yummy"."sales_item_category_id" as "_l0_filtered_category_l0",
    "yummy"."sales_item_class_id" as "_l0_filtered_class_l0",
    sum("yummy"."sales_quantity" * "yummy"."sales_list_price") as "_l0_filtered_bucket_sum_l0",
    sum("yummy"."sales_row_one") as "_l0_filtered_bucket_count_l0"
FROM
    "yummy"
GROUP BY
    1,
    2,
    3,
    4),
young as (
SELECT
    "juicy"."_l0_filtered_brand_l0" as "_l0_filtered_brand_l0",
    "juicy"."_l0_filtered_bucket_count_l0" as "_l0_filtered_bucket_count_l0",
    "juicy"."_l0_filtered_bucket_sum_l0" as "_l0_filtered_bucket_sum_l0",
    "juicy"."_l0_filtered_category_l0" as "_l0_filtered_category_l0",
    "juicy"."_l0_filtered_channel_l0" as "_l0_filtered_channel_l0",
    "juicy"."_l0_filtered_class_l0" as "_l0_filtered_class_l0"
FROM
    "vacuous"
    INNER JOIN "juicy" on 1=1
WHERE
    "juicy"."_l0_filtered_bucket_sum_l0" > "vacuous"."avg_sales_average_sales"
),
sparkling as (
SELECT
    "young"."_l0_filtered_brand_l0" as "l0_filtered_brand_l0",
    "young"."_l0_filtered_bucket_count_l0" as "l0_filtered_bucket_count_l0",
    "young"."_l0_filtered_bucket_sum_l0" as "l0_filtered_bucket_sum_l0",
    "young"."_l0_filtered_category_l0" as "l0_filtered_category_l0",
    "young"."_l0_filtered_channel_l0" as "l0_filtered_channel_l0",
    "young"."_l0_filtered_class_l0" as "l0_filtered_class_l0"
FROM
    "young")
SELECT
    "sparkling"."l0_filtered_channel_l0" as "channel",
    "sparkling"."l0_filtered_brand_l0" as "i_brand_id",
    "sparkling"."l0_filtered_class_l0" as "i_class_id",
    "sparkling"."l0_filtered_category_l0" as "i_category_id",
    sum("sparkling"."l0_filtered_bucket_sum_l0") as "sum_sales",
    sum("sparkling"."l0_filtered_bucket_count_l0") as "sum_number_sales"
FROM
    "sparkling"
GROUP BY
    ROLLUP (1, 2, 3, 4)
ORDER BY 
    "channel" asc nulls first,
    "i_brand_id" asc nulls first,
    "i_class_id" asc nulls first,
    "i_category_id" asc nulls first
LIMIT (100)
```
