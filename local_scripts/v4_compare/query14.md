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
| v4 | 10770 | 270 | 486.54 ms |
| reference | 6203 | 160 | 239.49 ms |
| v4 / ref | 1.74x | 1.69x | 2.03x |

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
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_LIST_PRICE" as "sales_list_price",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     1  as "sales_row_one",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_LIST_PRICE" as "sales_list_price",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
     1  as "sales_row_one",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_LIST_PRICE" as "sales_list_price",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     1  as "sales_row_one",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
cooperative as (
SELECT
    avg("cheerful"."sales_quantity" * "cheerful"."sales_list_price") as "_avg_sales_average_sales"
FROM
    "cheerful"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" BETWEEN 1999 AND 2001
),
yummy as (
SELECT
    "cheerful"."sales_list_price" as "sales_list_price",
    "cheerful"."sales_quantity" as "sales_quantity",
    "cheerful"."sales_row_one" as "sales_row_one",
    "cheerful"."sales_sales_channel" as "sales_sales_channel",
    "sales_item_items"."I_BRAND_ID" as "sales_item_brand_id",
    "sales_item_items"."I_CATEGORY_ID" as "sales_item_category_id",
    "sales_item_items"."I_CLASS_ID" as "sales_item_class_id"
FROM
    "cheerful"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "cheerful"."sales_item_id" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 11
),
vacuous as (
SELECT
    "cheerful"."sales_sales_channel" as "sales_sales_channel",
    "sales_item_items"."I_BRAND_ID" as "sales_item_brand_id",
    "sales_item_items"."I_CATEGORY_ID" as "sales_item_category_id",
    "sales_item_items"."I_CLASS_ID" as "sales_item_class_id"
FROM
    "cheerful"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "cheerful"."sales_item_id" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_date_date"."D_YEAR" BETWEEN 1999 AND 2001
),
abundant as (
SELECT
    "cooperative"."_avg_sales_average_sales" as "avg_sales_average_sales"
FROM
    "cooperative"),
busy as (
SELECT
    "yummy"."sales_item_brand_id" as "_l0_filtered_brand_l0",
    "yummy"."sales_item_category_id" as "_l0_filtered_category_l0",
    "yummy"."sales_item_class_id" as "_l0_filtered_class_l0",
    CASE
	WHEN "yummy"."sales_sales_channel" = 'STORE' THEN 'store'
	WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN 'catalog'
	WHEN "yummy"."sales_sales_channel" = 'WEB' THEN 'web'
	ELSE null
	END as "_l0_filtered_channel_l0"
FROM
    "yummy"),
juicy as (
SELECT
    "yummy"."sales_item_brand_id" as "sales_item_brand_id",
    "yummy"."sales_item_category_id" as "sales_item_category_id",
    "yummy"."sales_item_class_id" as "sales_item_class_id",
    "yummy"."sales_list_price" as "sales_list_price",
    "yummy"."sales_quantity" as "sales_quantity",
    "yummy"."sales_row_one" as "sales_row_one",
    (cast("yummy"."sales_item_brand_id" as string) || '|' || cast("yummy"."sales_item_class_id" as string) || '|' || cast("yummy"."sales_item_category_id" as string)) as "tuple_key",
    CASE
	WHEN "yummy"."sales_sales_channel" = 'STORE' THEN 'store'
	WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN 'catalog'
	WHEN "yummy"."sales_sales_channel" = 'WEB' THEN 'web'
	ELSE null
	END as "channel_label"
FROM
    "yummy"),
abhorrent as (
SELECT
    (cast("vacuous"."sales_item_brand_id" as string) || '|' || cast("vacuous"."sales_item_class_id" as string) || '|' || cast("vacuous"."sales_item_category_id" as string)) as "_cross_tuples_ci_tuple_key"
FROM
    "vacuous"),
concerned as (
SELECT
    "vacuous"."sales_sales_channel" as "sales_sales_channel",
    (cast("vacuous"."sales_item_brand_id" as string) || '|' || cast("vacuous"."sales_item_class_id" as string) || '|' || cast("vacuous"."sales_item_category_id" as string)) as "tuple_key"
FROM
    "vacuous"
GROUP BY
    1,
    2),
sparkling as (
SELECT
    "concerned"."tuple_key" as "tuple_key",
    count(distinct "concerned"."sales_sales_channel") as "cross_channel_count"
FROM
    "concerned"
GROUP BY
    1
HAVING
    "cross_channel_count" = 3
),
sweltering as (
SELECT
    "abhorrent"."_cross_tuples_ci_tuple_key" as "_cross_tuples_ci_tuple_key"
FROM
    "abhorrent"
    RIGHT OUTER JOIN "sparkling" on "abhorrent"."_cross_tuples_ci_tuple_key" is not distinct from "sparkling"."tuple_key"
GROUP BY
    1,
    "sparkling"."cross_channel_count"),
late as (
SELECT
    "sweltering"."_cross_tuples_ci_tuple_key" as "_cross_tuples_ci_tuple_key"
FROM
    "sweltering"),
macho as (
SELECT
    "late"."_cross_tuples_ci_tuple_key" as "cross_tuples_ci_tuple_key"
FROM
    "late"),
scrawny as (
SELECT
    "juicy"."channel_label" as "channel_label",
    "juicy"."sales_item_brand_id" as "sales_item_brand_id",
    "juicy"."sales_item_category_id" as "sales_item_category_id",
    "juicy"."sales_item_class_id" as "sales_item_class_id",
    "juicy"."sales_list_price" as "sales_list_price",
    "juicy"."sales_quantity" as "sales_quantity",
    "juicy"."sales_row_one" as "sales_row_one"
FROM
    "juicy"
WHERE
    "juicy"."tuple_key" in (select macho."cross_tuples_ci_tuple_key" from macho where macho."cross_tuples_ci_tuple_key" is not null)
),
divergent as (
SELECT
    "scrawny"."channel_label" as "channel_label",
    "scrawny"."sales_item_brand_id" as "sales_item_brand_id",
    "scrawny"."sales_item_category_id" as "sales_item_category_id",
    "scrawny"."sales_item_class_id" as "sales_item_class_id",
    sum("scrawny"."sales_quantity" * "scrawny"."sales_list_price") as "_l0_filtered_bucket_sum_l0"
FROM
    "scrawny"
GROUP BY
    1,
    2,
    3,
    4),
friendly as (
SELECT
    "scrawny"."channel_label" as "channel_label",
    "scrawny"."sales_item_brand_id" as "sales_item_brand_id",
    "scrawny"."sales_item_category_id" as "sales_item_category_id",
    "scrawny"."sales_item_class_id" as "sales_item_class_id",
    sum("scrawny"."sales_row_one") as "_l0_filtered_bucket_count_l0"
FROM
    "scrawny"
GROUP BY
    1,
    2,
    3,
    4),
charming as (
SELECT
    "busy"."_l0_filtered_brand_l0" as "_l0_filtered_brand_l0",
    "busy"."_l0_filtered_category_l0" as "_l0_filtered_category_l0",
    "busy"."_l0_filtered_channel_l0" as "_l0_filtered_channel_l0",
    "busy"."_l0_filtered_class_l0" as "_l0_filtered_class_l0",
    "divergent"."_l0_filtered_bucket_sum_l0" as "_l0_filtered_bucket_sum_l0",
    "friendly"."_l0_filtered_bucket_count_l0" as "_l0_filtered_bucket_count_l0"
FROM
    "divergent"
    INNER JOIN "friendly" on "divergent"."channel_label" is not distinct from "friendly"."channel_label" AND "divergent"."sales_item_brand_id" = "friendly"."sales_item_brand_id" AND "divergent"."sales_item_category_id" is not distinct from "friendly"."sales_item_category_id" AND "divergent"."sales_item_class_id" is not distinct from "friendly"."sales_item_class_id"
    FULL JOIN "busy" on "divergent"."channel_label" is not distinct from "busy"."_l0_filtered_channel_l0" AND "divergent"."sales_item_brand_id" = "busy"."_l0_filtered_brand_l0" AND "divergent"."sales_item_category_id" is not distinct from "busy"."_l0_filtered_category_l0" AND "divergent"."sales_item_class_id" is not distinct from "busy"."_l0_filtered_class_l0"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
protective as (
SELECT
    "charming"."_l0_filtered_brand_l0" as "_l0_filtered_brand_l0",
    "charming"."_l0_filtered_bucket_count_l0" as "_l0_filtered_bucket_count_l0",
    "charming"."_l0_filtered_bucket_sum_l0" as "_l0_filtered_bucket_sum_l0",
    "charming"."_l0_filtered_category_l0" as "_l0_filtered_category_l0",
    "charming"."_l0_filtered_channel_l0" as "_l0_filtered_channel_l0",
    "charming"."_l0_filtered_class_l0" as "_l0_filtered_class_l0"
FROM
    "abundant"
    INNER JOIN "charming" on 1=1
WHERE
    "charming"."_l0_filtered_bucket_sum_l0" > "abundant"."avg_sales_average_sales"
),
premium as (
SELECT
    "protective"."_l0_filtered_brand_l0" as "_l0_filtered_brand_l0",
    "protective"."_l0_filtered_bucket_count_l0" as "_l0_filtered_bucket_count_l0",
    "protective"."_l0_filtered_bucket_sum_l0" as "_l0_filtered_bucket_sum_l0",
    "protective"."_l0_filtered_category_l0" as "_l0_filtered_category_l0",
    "protective"."_l0_filtered_channel_l0" as "_l0_filtered_channel_l0",
    "protective"."_l0_filtered_class_l0" as "_l0_filtered_class_l0"
FROM
    "protective"),
puzzled as (
SELECT
    "premium"."_l0_filtered_brand_l0" as "l0_filtered_brand_l0",
    "premium"."_l0_filtered_bucket_count_l0" as "l0_filtered_bucket_count_l0",
    "premium"."_l0_filtered_bucket_sum_l0" as "l0_filtered_bucket_sum_l0",
    "premium"."_l0_filtered_category_l0" as "l0_filtered_category_l0",
    "premium"."_l0_filtered_channel_l0" as "l0_filtered_channel_l0",
    "premium"."_l0_filtered_class_l0" as "l0_filtered_class_l0"
FROM
    "premium"),
waggish as (
SELECT
    "puzzled"."l0_filtered_brand_l0" as "l0_filtered_brand_l0",
    "puzzled"."l0_filtered_category_l0" as "l0_filtered_category_l0",
    "puzzled"."l0_filtered_channel_l0" as "l0_filtered_channel_l0",
    "puzzled"."l0_filtered_class_l0" as "l0_filtered_class_l0",
    sum("puzzled"."l0_filtered_bucket_count_l0") as "sum_number_sales",
    sum("puzzled"."l0_filtered_bucket_sum_l0") as "sum_sales"
FROM
    "puzzled"
GROUP BY
    ROLLUP (3, 1, 4, 2))
SELECT
    "waggish"."l0_filtered_channel_l0" as "channel",
    "waggish"."l0_filtered_brand_l0" as "i_brand_id",
    "waggish"."l0_filtered_class_l0" as "i_class_id",
    "waggish"."l0_filtered_category_l0" as "i_category_id",
    "waggish"."sum_sales" as "sum_sales",
    "waggish"."sum_number_sales" as "sum_number_sales"
FROM
    "waggish"
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
