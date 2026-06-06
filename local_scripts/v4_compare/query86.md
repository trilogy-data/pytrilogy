# Query 86

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
| v4 | 2387 | 59 | 55.88 ms |
| reference | 2138 | 52 | 56.92 ms |
| v4 / ref | 1.12x | 1.13x | 0.98x |

## Preql

```
import web_sales as web_sales;

# Single ROLLUP(category, class) with rank() over (partition by lochierarchy,
# partition_cat order by total_sum desc). The sum and the grouping() columns
# resolve into one rollup CTE and the window runs directly over it, so the
# NULL-keyed subtotal/total rows stay aligned. Real `grouping()` (not the
# rollup-NULL-pattern trick) because items can have NULL category/class.
auto total_sum <- sum(web_sales.net_paid) by rollup web_sales.item.category, web_sales.item.class;
auto g_cat <- grouping(web_sales.item.category) by rollup web_sales.item.category, web_sales.item.class;
auto g_class <- grouping(web_sales.item.class) by rollup web_sales.item.category, web_sales.item.class;
auto lochierarchy <- g_cat + g_class;
auto partition_cat <- case when g_class = 0 then web_sales.item.category else null end;
auto rank_within_parent <- rank(web_sales.item.category, web_sales.item.class)
    over (partition by lochierarchy, partition_cat order by total_sum desc);

where
    web_sales.date.month_seq between 1200 and 1211
select
    total_sum,
    web_sales.item.category as i_category,
    web_sales.item.class as i_class,
    lochierarchy,
    rank_within_parent,
order by
    lochierarchy desc nulls first,
    case
            when lochierarchy = 0 then web_sales.item.category
            else null
        end asc nulls first,
    rank_within_parent asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "web_sales_item_items"."I_CATEGORY" as "web_sales_item_category",
    "web_sales_item_items"."I_CLASS" as "web_sales_item_class",
    "web_sales_web_sales"."WS_NET_PAID" as "web_sales_net_paid"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
    INNER JOIN "memory"."date_dim" as "web_sales_date_date" on "web_sales_web_sales"."WS_SOLD_DATE_SK" = "web_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "web_sales_item_items" on "web_sales_web_sales"."WS_ITEM_SK" = "web_sales_item_items"."I_ITEM_SK"
WHERE
    "web_sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211
),
thoughtful as (
SELECT
    "cheerful"."web_sales_item_category" as "web_sales_item_category",
    "cheerful"."web_sales_item_class" as "web_sales_item_class",
    "cheerful"."web_sales_net_paid" as "web_sales_net_paid"
FROM
    "cheerful"),
cooperative as (
SELECT
    "thoughtful"."web_sales_item_category" as "web_sales_item_category",
    "thoughtful"."web_sales_item_class" as "web_sales_item_class",
    grouping("thoughtful"."web_sales_item_category") as "g_cat",
    grouping("thoughtful"."web_sales_item_class") as "g_class",
    sum("thoughtful"."web_sales_net_paid") as "total_sum"
FROM
    "thoughtful"
GROUP BY
    ROLLUP (1, 2)),
questionable as (
SELECT
    "cooperative"."g_cat" + "cooperative"."g_class" as "lochierarchy",
    "cooperative"."total_sum" as "total_sum",
    "cooperative"."web_sales_item_category" as "web_sales_item_category",
    "cooperative"."web_sales_item_class" as "web_sales_item_class",
    rank() over (partition by "cooperative"."g_cat" + "cooperative"."g_class",CASE
	WHEN "cooperative"."g_class" = 0 THEN "cooperative"."web_sales_item_category"
	ELSE null
	END order by "cooperative"."total_sum" desc ) as "rank_within_parent"
FROM
    "cooperative")
SELECT
    "questionable"."total_sum" as "total_sum",
    "questionable"."web_sales_item_category" as "i_category",
    "questionable"."web_sales_item_class" as "i_class",
    "questionable"."lochierarchy" as "lochierarchy",
    "questionable"."rank_within_parent" as "rank_within_parent"
FROM
    "questionable"
ORDER BY 
    "questionable"."lochierarchy" desc nulls first,
    CASE
	WHEN "questionable"."lochierarchy" = 0 THEN "questionable"."web_sales_item_category"
	ELSE null
	END asc nulls first,
    "questionable"."rank_within_parent" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "web_sales_item_items"."I_CATEGORY" as "web_sales_item_category",
    "web_sales_item_items"."I_CLASS" as "web_sales_item_class",
    "web_sales_web_sales"."WS_NET_PAID" as "web_sales_net_paid"
FROM
    "memory"."web_sales" as "web_sales_web_sales"
    INNER JOIN "memory"."date_dim" as "web_sales_date_date" on "web_sales_web_sales"."WS_SOLD_DATE_SK" = "web_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "web_sales_item_items" on "web_sales_web_sales"."WS_ITEM_SK" = "web_sales_item_items"."I_ITEM_SK"
WHERE
    "web_sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211
),
thoughtful as (
SELECT
    "cheerful"."web_sales_item_category" as "web_sales_item_category",
    "cheerful"."web_sales_item_class" as "web_sales_item_class",
    CASE
	WHEN grouping("cheerful"."web_sales_item_class") = 0 THEN "cheerful"."web_sales_item_category"
	ELSE null
	END as "partition_cat",
    grouping("cheerful"."web_sales_item_category") + grouping("cheerful"."web_sales_item_class") as "lochierarchy",
    sum("cheerful"."web_sales_net_paid") as "total_sum"
FROM
    "cheerful"
GROUP BY
    ROLLUP (1, 2)),
cooperative as (
SELECT
    "thoughtful"."lochierarchy" as "lochierarchy",
    "thoughtful"."total_sum" as "total_sum",
    "thoughtful"."web_sales_item_category" as "web_sales_item_category",
    "thoughtful"."web_sales_item_class" as "web_sales_item_class",
    rank() over (partition by "thoughtful"."lochierarchy","thoughtful"."partition_cat" order by "thoughtful"."total_sum" desc ) as "rank_within_parent"
FROM
    "thoughtful")
SELECT
    "cooperative"."total_sum" as "total_sum",
    "cooperative"."web_sales_item_category" as "i_category",
    "cooperative"."web_sales_item_class" as "i_class",
    "cooperative"."lochierarchy" as "lochierarchy",
    "cooperative"."rank_within_parent" as "rank_within_parent"
FROM
    "cooperative"
ORDER BY 
    "cooperative"."lochierarchy" desc nulls first,
    CASE
	WHEN "cooperative"."lochierarchy" = 0 THEN "cooperative"."web_sales_item_category"
	ELSE null
	END asc nulls first,
    "cooperative"."rank_within_parent" asc nulls first
LIMIT (100)
```
