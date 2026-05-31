# Query 09

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
| v4 | 3357 | 91 | 41.05 ms |
| reference | 2991 | 108 | 39.53 ms |
| v4 / ref | 1.12x | 0.84x | 1.04x |

## Preql

```
import store_sales as store_sales;

def bucket_count(low, high) -> sum(
    case
        when store_sales.quantity between low and high then 1
        else 0
    end
);
def bucket_pick(cnt, threshold, low, high) -> case
    when cnt > threshold then avg(
            case
                when store_sales.quantity between low and high then store_sales.ext_discount_amount
                else null
            end
        )
    else avg(
            case
                when store_sales.quantity between low and high then store_sales.net_paid
                else null
            end
        )
end;

auto count1 <- @bucket_count(1, 20);
auto count2 <- @bucket_count(21, 40);
auto count3 <- @bucket_count(41, 60);
auto count4 <- @bucket_count(61, 80);
auto count5 <- @bucket_count(81, 100);

where
    store_sales.quantity between 1 and 100
select
    @bucket_pick(count1, 74129, 1, 20) as bucket1,
    @bucket_pick(count2, 122840, 21, 40) as bucket2,
    @bucket_pick(count3, 56580, 41, 60) as bucket3,
    @bucket_pick(count4, 10097, 61, 80) as bucket4,
    @bucket_pick(count5, 165306, 81, 100) as bucket5,
;
```

## v4 generated SQL

```sql
WITH 
quizzical as (
SELECT
    avg(CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 1 AND 20 THEN "store_sales_store_sales"."SS_EXT_DISCOUNT_AMT"
	ELSE null
	END) as "_virt_agg_avg_6330081077547932",
    avg(CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 1 AND 20 THEN "store_sales_store_sales"."SS_NET_PAID"
	ELSE null
	END) as "_virt_agg_avg_4794444300151277",
    avg(CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 40 THEN "store_sales_store_sales"."SS_EXT_DISCOUNT_AMT"
	ELSE null
	END) as "_virt_agg_avg_4659580449698061",
    avg(CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 40 THEN "store_sales_store_sales"."SS_NET_PAID"
	ELSE null
	END) as "_virt_agg_avg_1613688246980292",
    avg(CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 41 AND 60 THEN "store_sales_store_sales"."SS_EXT_DISCOUNT_AMT"
	ELSE null
	END) as "_virt_agg_avg_7477983213274050",
    avg(CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 41 AND 60 THEN "store_sales_store_sales"."SS_NET_PAID"
	ELSE null
	END) as "_virt_agg_avg_684038137247375",
    avg(CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 61 AND 80 THEN "store_sales_store_sales"."SS_EXT_DISCOUNT_AMT"
	ELSE null
	END) as "_virt_agg_avg_2142335324413561",
    avg(CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 61 AND 80 THEN "store_sales_store_sales"."SS_NET_PAID"
	ELSE null
	END) as "_virt_agg_avg_838076703794071",
    avg(CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 81 AND 100 THEN "store_sales_store_sales"."SS_EXT_DISCOUNT_AMT"
	ELSE null
	END) as "_virt_agg_avg_9933913877002720",
    avg(CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 81 AND 100 THEN "store_sales_store_sales"."SS_NET_PAID"
	ELSE null
	END) as "_virt_agg_avg_5605106007967002",
    sum(CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 1 AND 20 THEN 1
	ELSE 0
	END) as "count1",
    sum(CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 21 AND 40 THEN 1
	ELSE 0
	END) as "count2",
    sum(CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 41 AND 60 THEN 1
	ELSE 0
	END) as "count3",
    sum(CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 61 AND 80 THEN 1
	ELSE 0
	END) as "count4",
    sum(CASE
	WHEN "store_sales_store_sales"."SS_QUANTITY" BETWEEN 81 AND 100 THEN 1
	ELSE 0
	END) as "count5"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
WHERE
    "store_sales_store_sales"."SS_QUANTITY" BETWEEN 1 AND 100
)
SELECT
    CASE
	WHEN "quizzical"."count1" > 74129 THEN "quizzical"."_virt_agg_avg_6330081077547932"
	ELSE "quizzical"."_virt_agg_avg_4794444300151277"
	END as "bucket1",
    CASE
	WHEN "quizzical"."count2" > 122840 THEN "quizzical"."_virt_agg_avg_4659580449698061"
	ELSE "quizzical"."_virt_agg_avg_1613688246980292"
	END as "bucket2",
    CASE
	WHEN "quizzical"."count3" > 56580 THEN "quizzical"."_virt_agg_avg_7477983213274050"
	ELSE "quizzical"."_virt_agg_avg_684038137247375"
	END as "bucket3",
    CASE
	WHEN "quizzical"."count4" > 10097 THEN "quizzical"."_virt_agg_avg_2142335324413561"
	ELSE "quizzical"."_virt_agg_avg_838076703794071"
	END as "bucket4",
    CASE
	WHEN "quizzical"."count5" > 165306 THEN "quizzical"."_virt_agg_avg_9933913877002720"
	ELSE "quizzical"."_virt_agg_avg_5605106007967002"
	END as "bucket5"
FROM
    "quizzical"
```

## Reference SQL (zquery log)

```sql
WITH 
quizzical as (
SELECT
    "store_sales_store_sales"."SS_EXT_DISCOUNT_AMT" as "store_sales_ext_discount_amount",
    "store_sales_store_sales"."SS_NET_PAID" as "store_sales_net_paid",
    "store_sales_store_sales"."SS_QUANTITY" as "store_sales_quantity"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
WHERE
    "store_sales_store_sales"."SS_QUANTITY" BETWEEN 1 AND 100
),
cooperative as (
SELECT
    CASE
	WHEN sum(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 81 AND 100 THEN 1
	ELSE 0
	END) > 165306 THEN avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 81 AND 100 THEN "quizzical"."store_sales_ext_discount_amount"
	ELSE null
	END)
	ELSE avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 81 AND 100 THEN "quizzical"."store_sales_net_paid"
	ELSE null
	END)
	END as "bucket5"
FROM
    "quizzical"),
thoughtful as (
SELECT
    CASE
	WHEN sum(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 61 AND 80 THEN 1
	ELSE 0
	END) > 10097 THEN avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 61 AND 80 THEN "quizzical"."store_sales_ext_discount_amount"
	ELSE null
	END)
	ELSE avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 61 AND 80 THEN "quizzical"."store_sales_net_paid"
	ELSE null
	END)
	END as "bucket4"
FROM
    "quizzical"),
cheerful as (
SELECT
    CASE
	WHEN sum(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 41 AND 60 THEN 1
	ELSE 0
	END) > 56580 THEN avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 41 AND 60 THEN "quizzical"."store_sales_ext_discount_amount"
	ELSE null
	END)
	ELSE avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 41 AND 60 THEN "quizzical"."store_sales_net_paid"
	ELSE null
	END)
	END as "bucket3"
FROM
    "quizzical"),
wakeful as (
SELECT
    CASE
	WHEN sum(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 40 THEN 1
	ELSE 0
	END) > 122840 THEN avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 40 THEN "quizzical"."store_sales_ext_discount_amount"
	ELSE null
	END)
	ELSE avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 40 THEN "quizzical"."store_sales_net_paid"
	ELSE null
	END)
	END as "bucket2"
FROM
    "quizzical"),
highfalutin as (
SELECT
    CASE
	WHEN sum(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 1 AND 20 THEN 1
	ELSE 0
	END) > 74129 THEN avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 1 AND 20 THEN "quizzical"."store_sales_ext_discount_amount"
	ELSE null
	END)
	ELSE avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 1 AND 20 THEN "quizzical"."store_sales_net_paid"
	ELSE null
	END)
	END as "bucket1"
FROM
    "quizzical")
SELECT
    "highfalutin"."bucket1" as "bucket1",
    "wakeful"."bucket2" as "bucket2",
    "cheerful"."bucket3" as "bucket3",
    "thoughtful"."bucket4" as "bucket4",
    "cooperative"."bucket5" as "bucket5"
FROM
    "highfalutin"
    FULL JOIN "wakeful" on 1=1
    FULL JOIN "cheerful" on 1=1
    FULL JOIN "thoughtful" on 1=1
    FULL JOIN "cooperative" on 1=1
```
