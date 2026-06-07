# Query 36

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
| v4 | 2634 | 63 | 93.27 ms |
| reference | 2332 | 60 | 91.64 ms |
| v4 / ref | 1.13x | 1.05x | 1.02x |

## Preql

```
import physical_sales as ss;

# Single ROLLUP(category, class). Both rollup sums and the grouping() columns
# resolve into one rollup CTE, and the window (ordering by the gross-margin
# ratio of the two sums) runs directly over it, so the NULL-keyed subtotal/
# total rows stay aligned. Real `grouping()` (not the rollup-NULL-pattern
# trick) because some items have NULL category/class, so a NULL in the rollup
# output is ambiguous between "data NULL" and "rolled up".
auto profit_sum <- sum(ss.net_profit) by rollup ss.item.category, ss.item.class;
auto sales_sum <- sum(ss.ext_sales_price) by rollup ss.item.category, ss.item.class;
auto gross_margin <- profit_sum::numeric(15, 4) / sales_sum::numeric(15, 4);
auto g_cat <- grouping(ss.item.category) by rollup ss.item.category, ss.item.class;
auto g_class <- grouping(ss.item.class) by rollup ss.item.category, ss.item.class;
auto lochierarchy <- g_cat + g_class;
auto partition_cat <- case when g_class = 0 then ss.item.category else null end;
auto rank_within_parent <- rank(ss.item.category, ss.item.class)
    over (partition by lochierarchy, partition_cat order by gross_margin asc);

where
    ss.date.year = 2001 and ss.store.state = 'TN'
select
    gross_margin,
    ss.item.category as i_category,
    ss.item.class as i_class,
    lochierarchy,
    rank_within_parent,
order by
    lochierarchy desc nulls first,
    case
            when lochierarchy = 0 then ss.item.category
            else null
        end asc nulls first,
    rank_within_parent asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
thoughtful as (
SELECT
    "ss_item_items"."I_CATEGORY" as "ss_item_category",
    "ss_item_items"."I_CLASS" as "ss_item_class",
    "ss_store_sales"."SS_EXT_SALES_PRICE" as "ss_ext_sales_price",
    "ss_store_sales"."SS_NET_PROFIT" as "ss_net_profit"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
WHERE
    "ss_date_date"."D_YEAR" = 2001 and "ss_store_store"."S_STATE" = 'TN'
),
cooperative as (
SELECT
    "thoughtful"."ss_ext_sales_price" as "ss_ext_sales_price",
    "thoughtful"."ss_item_category" as "ss_item_category",
    "thoughtful"."ss_item_class" as "ss_item_class",
    "thoughtful"."ss_net_profit" as "ss_net_profit"
FROM
    "thoughtful"),
questionable as (
SELECT
    "cooperative"."ss_item_category" as "ss_item_category",
    "cooperative"."ss_item_class" as "ss_item_class",
    grouping("cooperative"."ss_item_category") as "g_cat",
    grouping("cooperative"."ss_item_class") as "g_class",
    sum("cooperative"."ss_ext_sales_price") as "sales_sum",
    sum("cooperative"."ss_net_profit") as "profit_sum"
FROM
    "cooperative"
GROUP BY
    ROLLUP (1, 2)),
abundant as (
SELECT
    "questionable"."g_cat" + "questionable"."g_class" as "lochierarchy",
    "questionable"."ss_item_category" as "ss_item_category",
    "questionable"."ss_item_class" as "ss_item_class",
    cast("questionable"."profit_sum" as numeric(15,4)) / cast("questionable"."sales_sum" as numeric(15,4)) as "gross_margin",
    rank() over (partition by "questionable"."g_cat" + "questionable"."g_class",CASE
	WHEN "questionable"."g_class" = 0 THEN "questionable"."ss_item_category"
	ELSE null
	END order by cast("questionable"."profit_sum" as numeric(15,4)) / cast("questionable"."sales_sum" as numeric(15,4)) asc ) as "rank_within_parent"
FROM
    "questionable")
SELECT
    "abundant"."gross_margin" as "gross_margin",
    "abundant"."ss_item_category" as "i_category",
    "abundant"."ss_item_class" as "i_class",
    "abundant"."lochierarchy" as "lochierarchy",
    "abundant"."rank_within_parent" as "rank_within_parent"
FROM
    "abundant"
ORDER BY 
    "abundant"."lochierarchy" desc nulls first,
    CASE
	WHEN "abundant"."lochierarchy" = 0 THEN "abundant"."ss_item_category"
	ELSE null
	END asc nulls first,
    "abundant"."rank_within_parent" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
thoughtful as (
SELECT
    "ss_item_items"."I_CATEGORY" as "ss_item_category",
    "ss_item_items"."I_CLASS" as "ss_item_class",
    "ss_store_sales"."SS_EXT_SALES_PRICE" as "ss_ext_sales_price",
    "ss_store_sales"."SS_NET_PROFIT" as "ss_net_profit"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
WHERE
    "ss_date_date"."D_YEAR" = 2001 and "ss_store_store"."S_STATE" = 'TN'
),
cooperative as (
SELECT
    "thoughtful"."ss_item_category" as "ss_item_category",
    "thoughtful"."ss_item_class" as "ss_item_class",
    CASE
	WHEN grouping("thoughtful"."ss_item_class") = 0 THEN "thoughtful"."ss_item_category"
	ELSE null
	END as "partition_cat",
    cast(sum("thoughtful"."ss_net_profit") as numeric(15,4)) / cast(sum("thoughtful"."ss_ext_sales_price") as numeric(15,4)) as "gross_margin",
    grouping("thoughtful"."ss_item_category") + grouping("thoughtful"."ss_item_class") as "lochierarchy"
FROM
    "thoughtful"
GROUP BY
    ROLLUP (1, 2)),
questionable as (
SELECT
    "cooperative"."gross_margin" as "gross_margin",
    "cooperative"."lochierarchy" as "lochierarchy",
    "cooperative"."ss_item_category" as "ss_item_category",
    "cooperative"."ss_item_class" as "ss_item_class",
    rank() over (partition by "cooperative"."lochierarchy","cooperative"."partition_cat" order by "cooperative"."gross_margin" asc ) as "rank_within_parent"
FROM
    "cooperative")
SELECT
    "questionable"."gross_margin" as "gross_margin",
    "questionable"."ss_item_category" as "i_category",
    "questionable"."ss_item_class" as "i_class",
    "questionable"."lochierarchy" as "lochierarchy",
    "questionable"."rank_within_parent" as "rank_within_parent"
FROM
    "questionable"
GROUP BY
    1,
    2,
    3,
    4,
    5
ORDER BY 
    "questionable"."lochierarchy" desc nulls first,
    CASE
	WHEN "questionable"."lochierarchy" = 0 THEN "questionable"."ss_item_category"
	ELSE null
	END asc nulls first,
    "questionable"."rank_within_parent" asc nulls first
LIMIT (100)
```
