# Query 28

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (1 rows) |
| reference execution | OK (1 rows) |
| results identical | YES |

## Result comparison

v4 rows: 1 (1 distinct)
ref rows: 1 (1 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 15298 | 212 | 67.98 ms |
| reference | 15298 | 212 | 66.28 ms |
| v4 / ref | 1.00x | 1.00x | 1.03x |

## Preql

```
import store_sales as store_sales;

# Quantity ranges are disjoint, so each row contributes to exactly one bucket.
# Assigning bucket_id once and grouping lets a single scan + per-group
# count(DISTINCT) replace six count(DISTINCT)s on overlapping CASE-filtered
# subsets of the same column -- the outer max(CASE) pivots back to wide.
auto bucket_id <- case
    when store_sales.quantity between 0 and 5 then 1
    when store_sales.quantity between 6 and 10 then 2
    when store_sales.quantity between 11 and 15 then 3
    when store_sales.quantity between 16 and 20 then 4
    when store_sales.quantity between 21 and 25 then 5
    when store_sales.quantity between 26 and 30 then 6
    else null
end;
auto filtered_lp <- case
    when bucket_id = 1
    and (
        store_sales.list_price between 8 and 18
        or store_sales.coupon_amt between 459 and 1459
        or store_sales.wholesale_cost between 57 and 77
    ) then store_sales.list_price
    when bucket_id = 2
    and (
        store_sales.list_price between 90 and 100
        or store_sales.coupon_amt between 2323 and 3323
        or store_sales.wholesale_cost between 31 and 51
    ) then store_sales.list_price
    when bucket_id = 3
    and (
        store_sales.list_price between 142 and 152
        or store_sales.coupon_amt between 12214 and 13214
        or store_sales.wholesale_cost between 79 and 99
    ) then store_sales.list_price
    when bucket_id = 4
    and (
        store_sales.list_price between 135 and 145
        or store_sales.coupon_amt between 6071 and 7071
        or store_sales.wholesale_cost between 38 and 58
    ) then store_sales.list_price
    when bucket_id = 5
    and (
        store_sales.list_price between 122 and 132
        or store_sales.coupon_amt between 836 and 1836
        or store_sales.wholesale_cost between 17 and 37
    ) then store_sales.list_price
    when bucket_id = 6
    and (
        store_sales.list_price between 154 and 164
        or store_sales.coupon_amt between 7326 and 8326
        or store_sales.wholesale_cost between 7 and 27
    ) then store_sales.list_price
    else null
end;
auto lp_avg <- avg(filtered_lp) by bucket_id;
auto lp_cnt <- count(filtered_lp) by bucket_id;
auto lp_cntd <- count_distinct(filtered_lp) by bucket_id;

def pivot(metric, b) -> max(metric ? bucket_id = b);

where
    store_sales.quantity between 0 and 30
select
    @pivot(lp_avg, 1) as B1_LP,
    @pivot(lp_cnt, 1) as B1_CNT,
    @pivot(lp_cntd, 1) as B1_CNTD,
    @pivot(lp_avg, 2) as B2_LP,
    @pivot(lp_cnt, 2) as B2_CNT,
    @pivot(lp_cntd, 2) as B2_CNTD,
    @pivot(lp_avg, 3) as B3_LP,
    @pivot(lp_cnt, 3) as B3_CNT,
    @pivot(lp_cntd, 3) as B3_CNTD,
    @pivot(lp_avg, 4) as B4_LP,
    @pivot(lp_cnt, 4) as B4_CNT,
    @pivot(lp_cntd, 4) as B4_CNTD,
    @pivot(lp_avg, 5) as B5_LP,
    @pivot(lp_cnt, 5) as B5_CNT,
    @pivot(lp_cntd, 5) as B5_CNTD,
    @pivot(lp_avg, 6) as B6_LP,
    @pivot(lp_cnt, 6) as B6_CNT,
    @pivot(lp_cntd, 6) as B6_CNTD,
limit 100
;
```

## v4 generated SQL

```sql
WITH 
quizzical as (
SELECT
    CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END as "bucket_id",
    avg(CASE
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 1 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 8 AND 18 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 459 AND 1459 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 57 AND 77 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 2 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 90 AND 100 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 2323 AND 3323 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 31 AND 51 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 3 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 142 AND 152 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 12214 AND 13214 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 79 AND 99 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 4 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 135 AND 145 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 6071 AND 7071 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 38 AND 58 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 5 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 122 AND 132 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 836 AND 1836 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 17 AND 37 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 6 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 154 AND 164 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 7326 AND 8326 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 7 AND 27 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	ELSE null
	END) as "lp_avg",
    count(CASE
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 1 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 8 AND 18 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 459 AND 1459 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 57 AND 77 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 2 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 90 AND 100 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 2323 AND 3323 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 31 AND 51 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 3 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 142 AND 152 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 12214 AND 13214 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 79 AND 99 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 4 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 135 AND 145 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 6071 AND 7071 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 38 AND 58 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 5 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 122 AND 132 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 836 AND 1836 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 17 AND 37 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 6 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 154 AND 164 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 7326 AND 8326 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 7 AND 27 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	ELSE null
	END) as "lp_cnt",
    count(distinct CASE
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 1 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 8 AND 18 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 459 AND 1459 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 57 AND 77 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 2 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 90 AND 100 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 2323 AND 3323 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 31 AND 51 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 3 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 142 AND 152 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 12214 AND 13214 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 79 AND 99 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 4 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 135 AND 145 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 6071 AND 7071 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 38 AND 58 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 5 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 122 AND 132 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 836 AND 1836 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 17 AND 37 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 6 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 154 AND 164 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 7326 AND 8326 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 7 AND 27 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	ELSE null
	END) as "lp_cntd"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
WHERE
    "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 30

GROUP BY
    1)
SELECT
    max(CASE WHEN "quizzical"."bucket_id" = 1 THEN "quizzical"."lp_cnt" ELSE NULL END) as "B1_CNT",
    max(CASE WHEN "quizzical"."bucket_id" = 1 THEN "quizzical"."lp_cntd" ELSE NULL END) as "B1_CNTD",
    max(CASE WHEN "quizzical"."bucket_id" = 1 THEN "quizzical"."lp_avg" ELSE NULL END) as "B1_LP",
    max(CASE WHEN "quizzical"."bucket_id" = 2 THEN "quizzical"."lp_cnt" ELSE NULL END) as "B2_CNT",
    max(CASE WHEN "quizzical"."bucket_id" = 2 THEN "quizzical"."lp_cntd" ELSE NULL END) as "B2_CNTD",
    max(CASE WHEN "quizzical"."bucket_id" = 2 THEN "quizzical"."lp_avg" ELSE NULL END) as "B2_LP",
    max(CASE WHEN "quizzical"."bucket_id" = 3 THEN "quizzical"."lp_cnt" ELSE NULL END) as "B3_CNT",
    max(CASE WHEN "quizzical"."bucket_id" = 3 THEN "quizzical"."lp_cntd" ELSE NULL END) as "B3_CNTD",
    max(CASE WHEN "quizzical"."bucket_id" = 3 THEN "quizzical"."lp_avg" ELSE NULL END) as "B3_LP",
    max(CASE WHEN "quizzical"."bucket_id" = 4 THEN "quizzical"."lp_cnt" ELSE NULL END) as "B4_CNT",
    max(CASE WHEN "quizzical"."bucket_id" = 4 THEN "quizzical"."lp_cntd" ELSE NULL END) as "B4_CNTD",
    max(CASE WHEN "quizzical"."bucket_id" = 4 THEN "quizzical"."lp_avg" ELSE NULL END) as "B4_LP",
    max(CASE WHEN "quizzical"."bucket_id" = 5 THEN "quizzical"."lp_cnt" ELSE NULL END) as "B5_CNT",
    max(CASE WHEN "quizzical"."bucket_id" = 5 THEN "quizzical"."lp_cntd" ELSE NULL END) as "B5_CNTD",
    max(CASE WHEN "quizzical"."bucket_id" = 5 THEN "quizzical"."lp_avg" ELSE NULL END) as "B5_LP",
    max(CASE WHEN "quizzical"."bucket_id" = 6 THEN "quizzical"."lp_cnt" ELSE NULL END) as "B6_CNT",
    max(CASE WHEN "quizzical"."bucket_id" = 6 THEN "quizzical"."lp_cntd" ELSE NULL END) as "B6_CNTD",
    max(CASE WHEN "quizzical"."bucket_id" = 6 THEN "quizzical"."lp_avg" ELSE NULL END) as "B6_LP"
FROM
    "quizzical"
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
quizzical as (
SELECT
    CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END as "bucket_id",
    avg(CASE
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 1 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 8 AND 18 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 459 AND 1459 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 57 AND 77 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 2 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 90 AND 100 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 2323 AND 3323 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 31 AND 51 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 3 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 142 AND 152 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 12214 AND 13214 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 79 AND 99 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 4 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 135 AND 145 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 6071 AND 7071 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 38 AND 58 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 5 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 122 AND 132 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 836 AND 1836 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 17 AND 37 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 6 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 154 AND 164 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 7326 AND 8326 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 7 AND 27 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	ELSE null
	END) as "lp_avg",
    count(CASE
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 1 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 8 AND 18 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 459 AND 1459 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 57 AND 77 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 2 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 90 AND 100 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 2323 AND 3323 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 31 AND 51 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 3 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 142 AND 152 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 12214 AND 13214 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 79 AND 99 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 4 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 135 AND 145 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 6071 AND 7071 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 38 AND 58 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 5 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 122 AND 132 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 836 AND 1836 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 17 AND 37 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 6 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 154 AND 164 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 7326 AND 8326 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 7 AND 27 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	ELSE null
	END) as "lp_cnt",
    count(distinct CASE
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 1 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 8 AND 18 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 459 AND 1459 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 57 AND 77 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 2 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 90 AND 100 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 2323 AND 3323 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 31 AND 51 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 3 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 142 AND 152 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 12214 AND 13214 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 79 AND 99 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 4 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 135 AND 145 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 6071 AND 7071 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 38 AND 58 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 5 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 122 AND 132 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 836 AND 1836 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 17 AND 37 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 6 and ( "store_sales_store_sales"."SS_LIST_PRICE" BETWEEN 154 AND 164 or "store_sales_store_sales"."SS_COUPON_AMT" BETWEEN 7326 AND 8326 or "store_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 7 AND 27 ) THEN "store_sales_store_sales"."SS_LIST_PRICE"
	ELSE null
	END) as "lp_cntd"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
WHERE
    "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 30

GROUP BY
    1)
SELECT
    max(CASE WHEN "quizzical"."bucket_id" = 1 THEN "quizzical"."lp_avg" ELSE NULL END) as "B1_LP",
    max(CASE WHEN "quizzical"."bucket_id" = 1 THEN "quizzical"."lp_cnt" ELSE NULL END) as "B1_CNT",
    max(CASE WHEN "quizzical"."bucket_id" = 1 THEN "quizzical"."lp_cntd" ELSE NULL END) as "B1_CNTD",
    max(CASE WHEN "quizzical"."bucket_id" = 2 THEN "quizzical"."lp_avg" ELSE NULL END) as "B2_LP",
    max(CASE WHEN "quizzical"."bucket_id" = 2 THEN "quizzical"."lp_cnt" ELSE NULL END) as "B2_CNT",
    max(CASE WHEN "quizzical"."bucket_id" = 2 THEN "quizzical"."lp_cntd" ELSE NULL END) as "B2_CNTD",
    max(CASE WHEN "quizzical"."bucket_id" = 3 THEN "quizzical"."lp_avg" ELSE NULL END) as "B3_LP",
    max(CASE WHEN "quizzical"."bucket_id" = 3 THEN "quizzical"."lp_cnt" ELSE NULL END) as "B3_CNT",
    max(CASE WHEN "quizzical"."bucket_id" = 3 THEN "quizzical"."lp_cntd" ELSE NULL END) as "B3_CNTD",
    max(CASE WHEN "quizzical"."bucket_id" = 4 THEN "quizzical"."lp_avg" ELSE NULL END) as "B4_LP",
    max(CASE WHEN "quizzical"."bucket_id" = 4 THEN "quizzical"."lp_cnt" ELSE NULL END) as "B4_CNT",
    max(CASE WHEN "quizzical"."bucket_id" = 4 THEN "quizzical"."lp_cntd" ELSE NULL END) as "B4_CNTD",
    max(CASE WHEN "quizzical"."bucket_id" = 5 THEN "quizzical"."lp_avg" ELSE NULL END) as "B5_LP",
    max(CASE WHEN "quizzical"."bucket_id" = 5 THEN "quizzical"."lp_cnt" ELSE NULL END) as "B5_CNT",
    max(CASE WHEN "quizzical"."bucket_id" = 5 THEN "quizzical"."lp_cntd" ELSE NULL END) as "B5_CNTD",
    max(CASE WHEN "quizzical"."bucket_id" = 6 THEN "quizzical"."lp_avg" ELSE NULL END) as "B6_LP",
    max(CASE WHEN "quizzical"."bucket_id" = 6 THEN "quizzical"."lp_cnt" ELSE NULL END) as "B6_CNT",
    max(CASE WHEN "quizzical"."bucket_id" = 6 THEN "quizzical"."lp_cntd" ELSE NULL END) as "B6_CNTD"
FROM
    "quizzical"
LIMIT (100)
```
