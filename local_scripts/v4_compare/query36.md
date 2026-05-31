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
| v4 | 2510 | 54 | 130.49 ms |
| reference | 2553 | 60 | 142.05 ms |
| v4 / ref | 0.98x | 0.90x | 0.92x |

## Preql

```
import store_sales as ss;

# Single ROLLUP(category, class). All rollup aggregates AND grouping/CASE
# derivations are computed INSIDE the rowset SELECT (with the underlying sums
# and grouping bits hidden via `--`). Exposing only the derived
# `gross_margin`/`lochierarchy`/`partition_cat` keeps the planner's source-
# resolution from splitting the two sums across separate CTEs.
#
# We use real `grouping()` here (not the rollup-NULL-pattern trick from q70)
# because some items in TPC-DS have NULL category/class, so a NULL in the
# rollup output is ambiguous between "data NULL" and "rolled up".
rowset q36_rolled <- where
    ss.date.year = 2001 and ss.store.state = 'TN'
select
    --sum(ss.net_profit) by rollup ss.item.category, ss.item.class as profit_sum,
    --sum(ss.ext_sales_price) by rollup ss.item.category, ss.item.class as sales_sum,
    --grouping(ss.item.category) by rollup ss.item.category, ss.item.class as g_cat,
    --grouping(ss.item.class) by rollup ss.item.category, ss.item.class as g_class,
    profit_sum::numeric(15,4) / sales_sum::numeric(15,4) as gross_margin,
    g_cat + g_class as lochierarchy,
    case
            when g_class = 0 then ss.item.category
            else null
        end as partition_cat,
    ss.item.category as r_category,
    ss.item.class as r_class,
;

select
    q36_rolled.gross_margin,
    q36_rolled.r_category as i_category,
    q36_rolled.r_class as i_class,
    q36_rolled.lochierarchy,
    rank(q36_rolled.r_class, q36_rolled.r_category)
            over (partition by q36_rolled.lochierarchy, q36_rolled.partition_cat
                order by q36_rolled.gross_margin asc) as rank_within_parent,
order by
    q36_rolled.lochierarchy desc nulls first,
    case
            when q36_rolled.lochierarchy = 0 then i_category
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
    "thoughtful"."ss_item_category" as "q36_rolled_r_category",
    "thoughtful"."ss_item_class" as "q36_rolled_r_class",
    CASE
	WHEN grouping("thoughtful"."ss_item_class") = 0 THEN "thoughtful"."ss_item_category"
	ELSE null
	END as "q36_rolled_partition_cat",
    cast(sum("thoughtful"."ss_net_profit") as numeric(15,4)) / cast(sum("thoughtful"."ss_ext_sales_price") as numeric(15,4)) as "q36_rolled_gross_margin",
    grouping("thoughtful"."ss_item_category") + grouping("thoughtful"."ss_item_class") as "q36_rolled_lochierarchy"
FROM
    "thoughtful"
GROUP BY
    ROLLUP (1, 2)),
questionable as (
SELECT
    "cooperative"."q36_rolled_gross_margin" as "q36_rolled_gross_margin",
    "cooperative"."q36_rolled_lochierarchy" as "q36_rolled_lochierarchy",
    "cooperative"."q36_rolled_r_category" as "q36_rolled_r_category",
    "cooperative"."q36_rolled_r_class" as "q36_rolled_r_class",
    rank() over (partition by "cooperative"."q36_rolled_lochierarchy","cooperative"."q36_rolled_partition_cat" order by "cooperative"."q36_rolled_gross_margin" asc ) as "rank_within_parent"
FROM
    "cooperative")
SELECT
    "questionable"."q36_rolled_r_category" as "i_category",
    "questionable"."q36_rolled_r_class" as "i_class",
    "questionable"."rank_within_parent" as "rank_within_parent",
    "questionable"."q36_rolled_gross_margin" as "q36_rolled_gross_margin",
    "questionable"."q36_rolled_lochierarchy" as "q36_rolled_lochierarchy"
FROM
    "questionable"
ORDER BY 
    "questionable"."q36_rolled_lochierarchy" desc nulls first,
    CASE
	WHEN "questionable"."q36_rolled_lochierarchy" = 0 THEN "questionable"."q36_rolled_r_category"
	ELSE null
	END asc nulls first,
    "questionable"."rank_within_parent" asc nulls first
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
    "thoughtful"."ss_item_category" as "q36_rolled_r_category",
    "thoughtful"."ss_item_class" as "q36_rolled_r_class",
    CASE
	WHEN grouping("thoughtful"."ss_item_class") = 0 THEN "thoughtful"."ss_item_category"
	ELSE null
	END as "q36_rolled_partition_cat",
    cast(sum("thoughtful"."ss_net_profit") as numeric(15,4)) / cast(sum("thoughtful"."ss_ext_sales_price") as numeric(15,4)) as "q36_rolled_gross_margin",
    grouping("thoughtful"."ss_item_category") + grouping("thoughtful"."ss_item_class") as "q36_rolled_lochierarchy"
FROM
    "thoughtful"
GROUP BY
    ROLLUP (1, 2)),
questionable as (
SELECT
    "cooperative"."q36_rolled_gross_margin" as "q36_rolled_gross_margin",
    "cooperative"."q36_rolled_lochierarchy" as "q36_rolled_lochierarchy",
    "cooperative"."q36_rolled_r_category" as "q36_rolled_r_category",
    "cooperative"."q36_rolled_r_class" as "q36_rolled_r_class",
    rank() over (partition by "cooperative"."q36_rolled_lochierarchy","cooperative"."q36_rolled_partition_cat" order by "cooperative"."q36_rolled_gross_margin" asc ) as "rank_within_parent"
FROM
    "cooperative")
SELECT
    "questionable"."q36_rolled_gross_margin" as "q36_rolled_gross_margin",
    "questionable"."q36_rolled_r_category" as "i_category",
    "questionable"."q36_rolled_r_class" as "i_class",
    "questionable"."q36_rolled_lochierarchy" as "q36_rolled_lochierarchy",
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
    "questionable"."q36_rolled_lochierarchy" desc nulls first,
    CASE
	WHEN "questionable"."q36_rolled_lochierarchy" = 0 THEN "questionable"."q36_rolled_r_category"
	ELSE null
	END asc nulls first,
    "questionable"."rank_within_parent" asc nulls first
LIMIT (100)
```
