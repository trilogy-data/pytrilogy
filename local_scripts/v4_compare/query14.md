# Query 14

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | NO |

## Result comparison

v4 rows: 100 (100 distinct)
ref rows: 100 (100 distinct)
only in v4 (showing up to 5 of 99):
  1x  (None, None, None, None, 1294902, Decimal('5301925397.80000000'))
  1x  ('catalog', None, None, None, 288819, Decimal('1458006270.83000000'))
  1x  ('catalog', 1001001, None, None, 2020, Decimal('8908176.70000000'))
  1x  ('catalog', 1001001, None, 1, 844, Decimal('3854145.51000000'))
  1x  ('catalog', 1001001, 1, 1, 102, Decimal('444095.42000000'))
only in ref (showing up to 5 of 99):
  1x  (None, None, None, None, 155567, Decimal('673409655.64000000'))
  1x  ('catalog', None, None, None, 46359, Decimal('234830325.53000000'))
  1x  ('catalog', 1001001, None, None, 341, Decimal('1549222.39000000'))
  1x  ('catalog', 1001001, None, 1, 162, Decimal('742922.27000000'))
  1x  ('catalog', 1001001, 1, 1, 20, Decimal('87409.20000000'))

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 8419 | 222 | 326.15 ms |
| reference | 6203 | 160 | 240.15 ms |
| v4 / ref | 1.36x | 1.39x | 1.36x |

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
     'CATALOG'  as "sales_sales_channel",
    "sales_catalog_sales_unified"."CS_LIST_PRICE" as "sales_list_price",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     1  as "sales_row_one"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
     'STORE'  as "sales_sales_channel",
    "sales_store_sales_unified"."SS_LIST_PRICE" as "sales_list_price",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
     1  as "sales_row_one"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
     'WEB'  as "sales_sales_channel",
    "sales_web_sales_unified"."WS_LIST_PRICE" as "sales_list_price",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     1  as "sales_row_one"
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
concerned as (
SELECT
    "cheerful"."sales_item_id" as "sales_item_id",
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

GROUP BY
    1,
    2,
    3,
    4,
    5),
yummy as (
SELECT
    "cheerful"."sales_item_id" as "sales_item_id",
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
abundant as (
SELECT
    "cooperative"."_avg_sales_average_sales" as "avg_sales_average_sales"
FROM
    "cooperative"),
young as (
SELECT
    "concerned"."sales_item_id" as "sales_item_id",
    (cast("concerned"."sales_item_brand_id" as string) || '|' || cast("concerned"."sales_item_class_id" as string) || '|' || cast("concerned"."sales_item_category_id" as string)) as "tuple_key"
FROM
    "concerned"),
juicy as (
SELECT
    "yummy"."sales_item_id" as "sales_item_id",
    "yummy"."sales_sales_channel" as "sales_sales_channel",
    (cast("yummy"."sales_item_brand_id" as string) || '|' || cast("yummy"."sales_item_class_id" as string) || '|' || cast("yummy"."sales_item_category_id" as string)) as "tuple_key",
    CASE
	WHEN "yummy"."sales_sales_channel" = 'STORE' THEN 'store'
	WHEN "yummy"."sales_sales_channel" = 'CATALOG' THEN 'catalog'
	WHEN "yummy"."sales_sales_channel" = 'WEB' THEN 'web'
	ELSE null
	END as "channel_label"
FROM
    "yummy"),
sparkling as (
SELECT
    "concerned"."sales_sales_channel" as "sales_sales_channel",
    "young"."tuple_key" as "tuple_key"
FROM
    "concerned"
    INNER JOIN "young" on "concerned"."sales_item_id" = "young"."sales_item_id"
GROUP BY
    1,
    2),
sweltering as (
SELECT
    "sparkling"."tuple_key" as "_cross_tuples_ci_tuple_key"
FROM
    "sparkling"
GROUP BY
    1
HAVING
    count(distinct "sparkling"."sales_sales_channel") = 3
),
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
vacuous as (
SELECT
    "juicy"."channel_label" as "channel_label",
    "yummy"."sales_item_brand_id" as "sales_item_brand_id",
    "yummy"."sales_item_category_id" as "sales_item_category_id",
    "yummy"."sales_item_class_id" as "sales_item_class_id",
    "yummy"."sales_list_price" as "sales_list_price",
    "yummy"."sales_quantity" as "sales_quantity",
    "yummy"."sales_row_one" as "sales_row_one"
FROM
    "yummy"
    RIGHT OUTER JOIN "juicy" on "yummy"."sales_item_id" = "juicy"."sales_item_id" AND "yummy"."sales_sales_channel" = "juicy"."sales_sales_channel"
WHERE
    "juicy"."tuple_key" in (select macho."cross_tuples_ci_tuple_key" from macho where macho."cross_tuples_ci_tuple_key" is not null)
),
scrawny as (
SELECT
    "vacuous"."channel_label" as "_l0_filtered_channel_l0",
    "vacuous"."sales_item_brand_id" as "_l0_filtered_brand_l0",
    "vacuous"."sales_item_category_id" as "_l0_filtered_category_l0",
    "vacuous"."sales_item_class_id" as "_l0_filtered_class_l0",
    sum("vacuous"."sales_quantity" * "vacuous"."sales_list_price") as "_l0_filtered_bucket_sum_l0",
    sum("vacuous"."sales_row_one") as "_l0_filtered_bucket_count_l0"
FROM
    "vacuous"
GROUP BY
    1,
    2,
    3,
    4),
kaput as (
SELECT
    "scrawny"."_l0_filtered_brand_l0" as "_l0_filtered_brand_l0",
    "scrawny"."_l0_filtered_bucket_count_l0" as "_l0_filtered_bucket_count_l0",
    "scrawny"."_l0_filtered_bucket_sum_l0" as "_l0_filtered_bucket_sum_l0",
    "scrawny"."_l0_filtered_category_l0" as "_l0_filtered_category_l0",
    "scrawny"."_l0_filtered_channel_l0" as "_l0_filtered_channel_l0",
    "scrawny"."_l0_filtered_class_l0" as "_l0_filtered_class_l0"
FROM
    "abundant"
    INNER JOIN "scrawny" on 1=1
WHERE
    "scrawny"."_l0_filtered_bucket_sum_l0" > "abundant"."avg_sales_average_sales"
),
divergent as (
SELECT
    "kaput"."_l0_filtered_brand_l0" as "_l0_filtered_brand_l0",
    "kaput"."_l0_filtered_bucket_count_l0" as "_l0_filtered_bucket_count_l0",
    "kaput"."_l0_filtered_bucket_sum_l0" as "_l0_filtered_bucket_sum_l0",
    "kaput"."_l0_filtered_category_l0" as "_l0_filtered_category_l0",
    "kaput"."_l0_filtered_channel_l0" as "_l0_filtered_channel_l0",
    "kaput"."_l0_filtered_class_l0" as "_l0_filtered_class_l0"
FROM
    "kaput"),
busy as (
SELECT
    "divergent"."_l0_filtered_brand_l0" as "l0_filtered_brand_l0",
    "divergent"."_l0_filtered_bucket_count_l0" as "l0_filtered_bucket_count_l0",
    "divergent"."_l0_filtered_bucket_sum_l0" as "l0_filtered_bucket_sum_l0",
    "divergent"."_l0_filtered_category_l0" as "l0_filtered_category_l0",
    "divergent"."_l0_filtered_channel_l0" as "l0_filtered_channel_l0",
    "divergent"."_l0_filtered_class_l0" as "l0_filtered_class_l0"
FROM
    "divergent"),
charming as (
SELECT
    "busy"."l0_filtered_brand_l0" as "l0_filtered_brand_l0",
    "busy"."l0_filtered_category_l0" as "l0_filtered_category_l0",
    "busy"."l0_filtered_channel_l0" as "l0_filtered_channel_l0",
    "busy"."l0_filtered_class_l0" as "l0_filtered_class_l0",
    sum("busy"."l0_filtered_bucket_count_l0") as "sum_number_sales",
    sum("busy"."l0_filtered_bucket_sum_l0") as "sum_sales"
FROM
    "busy"
GROUP BY
    ROLLUP (3, 1, 4, 2))
SELECT
    "charming"."l0_filtered_channel_l0" as "channel",
    "charming"."l0_filtered_brand_l0" as "i_brand_id",
    "charming"."l0_filtered_class_l0" as "i_class_id",
    "charming"."l0_filtered_category_l0" as "i_category_id",
    "charming"."sum_sales" as "sum_sales",
    "charming"."sum_number_sales" as "sum_number_sales"
FROM
    "charming"
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
