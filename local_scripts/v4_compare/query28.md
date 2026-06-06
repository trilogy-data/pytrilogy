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
| v4 | 20778 | 323 | 19.68 ms |
| reference | 15862 | 212 | 16.80 ms |
| v4 / ref | 1.31x | 1.52x | 1.17x |

## Preql

```
import physical_sales as physical_sales;

# Quantity ranges are disjoint, so each row contributes to exactly one bucket.
# Assigning bucket_id once and grouping lets a single scan + per-group
# count(DISTINCT) replace six count(DISTINCT)s on overlapping CASE-filtered
# subsets of the same column -- the outer max(CASE) pivots back to wide.
auto bucket_id <- case
    when physical_sales.quantity between 0 and 5 then 1
    when physical_sales.quantity between 6 and 10 then 2
    when physical_sales.quantity between 11 and 15 then 3
    when physical_sales.quantity between 16 and 20 then 4
    when physical_sales.quantity between 21 and 25 then 5
    when physical_sales.quantity between 26 and 30 then 6
    else null
end;
auto filtered_lp <- case
    when bucket_id = 1
    and (
        physical_sales.list_price between 8 and 18
        or physical_sales.coupon_amt between 459 and 1459
        or physical_sales.wholesale_cost between 57 and 77
    ) then physical_sales.list_price
    when bucket_id = 2
    and (
        physical_sales.list_price between 90 and 100
        or physical_sales.coupon_amt between 2323 and 3323
        or physical_sales.wholesale_cost between 31 and 51
    ) then physical_sales.list_price
    when bucket_id = 3
    and (
        physical_sales.list_price between 142 and 152
        or physical_sales.coupon_amt between 12214 and 13214
        or physical_sales.wholesale_cost between 79 and 99
    ) then physical_sales.list_price
    when bucket_id = 4
    and (
        physical_sales.list_price between 135 and 145
        or physical_sales.coupon_amt between 6071 and 7071
        or physical_sales.wholesale_cost between 38 and 58
    ) then physical_sales.list_price
    when bucket_id = 5
    and (
        physical_sales.list_price between 122 and 132
        or physical_sales.coupon_amt between 836 and 1836
        or physical_sales.wholesale_cost between 17 and 37
    ) then physical_sales.list_price
    when bucket_id = 6
    and (
        physical_sales.list_price between 154 and 164
        or physical_sales.coupon_amt between 7326 and 8326
        or physical_sales.wholesale_cost between 7 and 27
    ) then physical_sales.list_price
    else null
end;
auto lp_avg <- avg(filtered_lp) by bucket_id;
auto lp_cnt <- count(filtered_lp) by bucket_id;
auto lp_cntd <- count_distinct(filtered_lp) by bucket_id;

def pivot(metric, b) -> max(metric ? bucket_id = b);

where
    physical_sales.quantity between 0 and 30
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
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END as "bucket_id",
    avg(CASE
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 1 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 8 AND 18 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 459 AND 1459 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 57 AND 77 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 2 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 90 AND 100 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 2323 AND 3323 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 31 AND 51 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 3 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 142 AND 152 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 12214 AND 13214 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 79 AND 99 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 4 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 135 AND 145 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 6071 AND 7071 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 38 AND 58 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 5 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 122 AND 132 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 836 AND 1836 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 17 AND 37 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 6 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 154 AND 164 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 7326 AND 8326 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 7 AND 27 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	ELSE null
	END) as "lp_avg",
    count(CASE
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 1 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 8 AND 18 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 459 AND 1459 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 57 AND 77 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 2 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 90 AND 100 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 2323 AND 3323 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 31 AND 51 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 3 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 142 AND 152 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 12214 AND 13214 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 79 AND 99 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 4 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 135 AND 145 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 6071 AND 7071 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 38 AND 58 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 5 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 122 AND 132 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 836 AND 1836 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 17 AND 37 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 6 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 154 AND 164 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 7326 AND 8326 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 7 AND 27 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	ELSE null
	END) as "lp_cnt",
    count(distinct CASE
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 1 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 8 AND 18 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 459 AND 1459 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 57 AND 77 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 2 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 90 AND 100 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 2323 AND 3323 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 31 AND 51 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 3 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 142 AND 152 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 12214 AND 13214 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 79 AND 99 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 4 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 135 AND 145 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 6071 AND 7071 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 38 AND 58 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 5 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 122 AND 132 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 836 AND 1836 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 17 AND 37 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 6 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 154 AND 164 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 7326 AND 8326 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 7 AND 27 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	ELSE null
	END) as "lp_cntd"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
WHERE
    "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 30

GROUP BY
    1),
thoughtful as (
SELECT
    "quizzical"."lp_avg" as "lp_avg",
    "quizzical"."lp_cnt" as "lp_cnt",
    "quizzical"."lp_cntd" as "lp_cntd",
    CASE WHEN "quizzical"."bucket_id" = 1 THEN "quizzical"."lp_avg" ELSE NULL END as "_virt_filter_lp_avg_9807684414024497",
    CASE WHEN "quizzical"."bucket_id" = 1 THEN "quizzical"."lp_cnt" ELSE NULL END as "_virt_filter_lp_cnt_2224516900884374",
    CASE WHEN "quizzical"."bucket_id" = 1 THEN "quizzical"."lp_cntd" ELSE NULL END as "_virt_filter_lp_cntd_5327557161933280",
    CASE WHEN "quizzical"."bucket_id" = 2 THEN "quizzical"."lp_avg" ELSE NULL END as "_virt_filter_lp_avg_2861588206160358",
    CASE WHEN "quizzical"."bucket_id" = 2 THEN "quizzical"."lp_cnt" ELSE NULL END as "_virt_filter_lp_cnt_6858115141264011",
    CASE WHEN "quizzical"."bucket_id" = 2 THEN "quizzical"."lp_cntd" ELSE NULL END as "_virt_filter_lp_cntd_7846943073851735",
    CASE WHEN "quizzical"."bucket_id" = 3 THEN "quizzical"."lp_avg" ELSE NULL END as "_virt_filter_lp_avg_2850673729313768",
    CASE WHEN "quizzical"."bucket_id" = 3 THEN "quizzical"."lp_cnt" ELSE NULL END as "_virt_filter_lp_cnt_894357222675893",
    CASE WHEN "quizzical"."bucket_id" = 3 THEN "quizzical"."lp_cntd" ELSE NULL END as "_virt_filter_lp_cntd_5736325163174944",
    CASE WHEN "quizzical"."bucket_id" = 4 THEN "quizzical"."lp_avg" ELSE NULL END as "_virt_filter_lp_avg_4880359626544039",
    CASE WHEN "quizzical"."bucket_id" = 4 THEN "quizzical"."lp_cnt" ELSE NULL END as "_virt_filter_lp_cnt_3577078747089769",
    CASE WHEN "quizzical"."bucket_id" = 4 THEN "quizzical"."lp_cntd" ELSE NULL END as "_virt_filter_lp_cntd_7690745280661633",
    CASE WHEN "quizzical"."bucket_id" = 5 THEN "quizzical"."lp_avg" ELSE NULL END as "_virt_filter_lp_avg_284549201004209",
    CASE WHEN "quizzical"."bucket_id" = 5 THEN "quizzical"."lp_cnt" ELSE NULL END as "_virt_filter_lp_cnt_1394773238248231",
    CASE WHEN "quizzical"."bucket_id" = 5 THEN "quizzical"."lp_cntd" ELSE NULL END as "_virt_filter_lp_cntd_3739216114026085",
    CASE WHEN "quizzical"."bucket_id" = 6 THEN "quizzical"."lp_avg" ELSE NULL END as "_virt_filter_lp_avg_9446332470240154",
    CASE WHEN "quizzical"."bucket_id" = 6 THEN "quizzical"."lp_cnt" ELSE NULL END as "_virt_filter_lp_cnt_8966209673081292",
    CASE WHEN "quizzical"."bucket_id" = 6 THEN "quizzical"."lp_cntd" ELSE NULL END as "_virt_filter_lp_cntd_3811294399709285"
FROM
    "quizzical"),
yummy as (
SELECT
    "thoughtful"."_virt_filter_lp_cntd_3739216114026085" as "_virt_filter_lp_cntd_3739216114026085",
    "thoughtful"."_virt_filter_lp_cntd_3811294399709285" as "_virt_filter_lp_cntd_3811294399709285",
    "thoughtful"."_virt_filter_lp_cntd_5327557161933280" as "_virt_filter_lp_cntd_5327557161933280",
    "thoughtful"."_virt_filter_lp_cntd_5736325163174944" as "_virt_filter_lp_cntd_5736325163174944",
    "thoughtful"."_virt_filter_lp_cntd_7690745280661633" as "_virt_filter_lp_cntd_7690745280661633",
    "thoughtful"."_virt_filter_lp_cntd_7846943073851735" as "_virt_filter_lp_cntd_7846943073851735"
FROM
    "thoughtful"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    "thoughtful"."lp_cntd"),
abundant as (
SELECT
    "thoughtful"."_virt_filter_lp_cnt_1394773238248231" as "_virt_filter_lp_cnt_1394773238248231",
    "thoughtful"."_virt_filter_lp_cnt_2224516900884374" as "_virt_filter_lp_cnt_2224516900884374",
    "thoughtful"."_virt_filter_lp_cnt_3577078747089769" as "_virt_filter_lp_cnt_3577078747089769",
    "thoughtful"."_virt_filter_lp_cnt_6858115141264011" as "_virt_filter_lp_cnt_6858115141264011",
    "thoughtful"."_virt_filter_lp_cnt_894357222675893" as "_virt_filter_lp_cnt_894357222675893",
    "thoughtful"."_virt_filter_lp_cnt_8966209673081292" as "_virt_filter_lp_cnt_8966209673081292"
FROM
    "thoughtful"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    "thoughtful"."lp_cnt"),
cooperative as (
SELECT
    "thoughtful"."_virt_filter_lp_avg_284549201004209" as "_virt_filter_lp_avg_284549201004209",
    "thoughtful"."_virt_filter_lp_avg_2850673729313768" as "_virt_filter_lp_avg_2850673729313768",
    "thoughtful"."_virt_filter_lp_avg_2861588206160358" as "_virt_filter_lp_avg_2861588206160358",
    "thoughtful"."_virt_filter_lp_avg_4880359626544039" as "_virt_filter_lp_avg_4880359626544039",
    "thoughtful"."_virt_filter_lp_avg_9446332470240154" as "_virt_filter_lp_avg_9446332470240154",
    "thoughtful"."_virt_filter_lp_avg_9807684414024497" as "_virt_filter_lp_avg_9807684414024497"
FROM
    "thoughtful"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    "thoughtful"."lp_avg"),
juicy as (
SELECT
    max("yummy"."_virt_filter_lp_cntd_3739216114026085") as "B5_CNTD",
    max("yummy"."_virt_filter_lp_cntd_3811294399709285") as "B6_CNTD",
    max("yummy"."_virt_filter_lp_cntd_5327557161933280") as "B1_CNTD",
    max("yummy"."_virt_filter_lp_cntd_5736325163174944") as "B3_CNTD",
    max("yummy"."_virt_filter_lp_cntd_7690745280661633") as "B4_CNTD",
    max("yummy"."_virt_filter_lp_cntd_7846943073851735") as "B2_CNTD"
FROM
    "yummy"),
uneven as (
SELECT
    max("abundant"."_virt_filter_lp_cnt_1394773238248231") as "B5_CNT",
    max("abundant"."_virt_filter_lp_cnt_2224516900884374") as "B1_CNT",
    max("abundant"."_virt_filter_lp_cnt_3577078747089769") as "B4_CNT",
    max("abundant"."_virt_filter_lp_cnt_6858115141264011") as "B2_CNT",
    max("abundant"."_virt_filter_lp_cnt_894357222675893") as "B3_CNT",
    max("abundant"."_virt_filter_lp_cnt_8966209673081292") as "B6_CNT"
FROM
    "abundant"),
questionable as (
SELECT
    max("cooperative"."_virt_filter_lp_avg_284549201004209") as "B5_LP",
    max("cooperative"."_virt_filter_lp_avg_2850673729313768") as "B3_LP",
    max("cooperative"."_virt_filter_lp_avg_2861588206160358") as "B2_LP",
    max("cooperative"."_virt_filter_lp_avg_4880359626544039") as "B4_LP",
    max("cooperative"."_virt_filter_lp_avg_9446332470240154") as "B6_LP",
    max("cooperative"."_virt_filter_lp_avg_9807684414024497") as "B1_LP"
FROM
    "cooperative")
SELECT
    "questionable"."B1_LP" as "B1_LP",
    "uneven"."B1_CNT" as "B1_CNT",
    "juicy"."B1_CNTD" as "B1_CNTD",
    "questionable"."B2_LP" as "B2_LP",
    "uneven"."B2_CNT" as "B2_CNT",
    "juicy"."B2_CNTD" as "B2_CNTD",
    "questionable"."B3_LP" as "B3_LP",
    "uneven"."B3_CNT" as "B3_CNT",
    "juicy"."B3_CNTD" as "B3_CNTD",
    "questionable"."B4_LP" as "B4_LP",
    "uneven"."B4_CNT" as "B4_CNT",
    "juicy"."B4_CNTD" as "B4_CNTD",
    "questionable"."B5_LP" as "B5_LP",
    "uneven"."B5_CNT" as "B5_CNT",
    "juicy"."B5_CNTD" as "B5_CNTD",
    "questionable"."B6_LP" as "B6_LP",
    "uneven"."B6_CNT" as "B6_CNT",
    "juicy"."B6_CNTD" as "B6_CNTD"
FROM
    "questionable"
    FULL JOIN "uneven" on 1=1
    FULL JOIN "juicy" on 1=1
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
quizzical as (
SELECT
    CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END as "bucket_id",
    avg(CASE
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 1 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 8 AND 18 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 459 AND 1459 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 57 AND 77 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 2 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 90 AND 100 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 2323 AND 3323 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 31 AND 51 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 3 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 142 AND 152 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 12214 AND 13214 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 79 AND 99 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 4 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 135 AND 145 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 6071 AND 7071 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 38 AND 58 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 5 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 122 AND 132 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 836 AND 1836 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 17 AND 37 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 6 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 154 AND 164 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 7326 AND 8326 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 7 AND 27 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	ELSE null
	END) as "lp_avg",
    count(CASE
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 1 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 8 AND 18 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 459 AND 1459 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 57 AND 77 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 2 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 90 AND 100 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 2323 AND 3323 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 31 AND 51 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 3 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 142 AND 152 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 12214 AND 13214 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 79 AND 99 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 4 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 135 AND 145 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 6071 AND 7071 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 38 AND 58 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 5 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 122 AND 132 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 836 AND 1836 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 17 AND 37 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 6 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 154 AND 164 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 7326 AND 8326 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 7 AND 27 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	ELSE null
	END) as "lp_cnt",
    count(distinct CASE
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 1 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 8 AND 18 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 459 AND 1459 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 57 AND 77 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 2 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 90 AND 100 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 2323 AND 3323 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 31 AND 51 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 3 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 142 AND 152 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 12214 AND 13214 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 79 AND 99 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 4 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 135 AND 145 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 6071 AND 7071 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 38 AND 58 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 5 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 122 AND 132 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 836 AND 1836 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 17 AND 37 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	WHEN CASE
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 5 THEN 1
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 6 AND 10 THEN 2
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 11 AND 15 THEN 3
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 16 AND 20 THEN 4
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 25 THEN 5
	WHEN "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 6 and ( "physical_sales_store_sales"."SS_LIST_PRICE" BETWEEN 154 AND 164 or "physical_sales_store_sales"."SS_COUPON_AMT" BETWEEN 7326 AND 8326 or "physical_sales_store_sales"."SS_WHOLESALE_COST" BETWEEN 7 AND 27 ) THEN "physical_sales_store_sales"."SS_LIST_PRICE"
	ELSE null
	END) as "lp_cntd"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
WHERE
    "physical_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 30

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
