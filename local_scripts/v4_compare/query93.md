# Query 93

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (100 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 1713 | 48 | — |
| reference | 914 | 19 | 27.26 ms |
| v4 / ref | 1.87x | 2.53x | — |

## Preql

```
import store_sales as ss;

auto act_sales <- case
    when ss.return_quantity is not null then (ss.quantity - ss.return_quantity) * ss.sales_price
    else ss.quantity * ss.sales_price
end;

where
    ss.return_reason.desc = 'reason 28'
select
    ss.customer.id as customer_sk,
    sum(act_sales) by ss.customer.id as sumsales,
order by
    sumsales asc nulls first,
    customer_sk asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "ss_store_returns"."SR_RETURN_QUANTITY" as "ss_return_quantity",
    "ss_store_sales"."SS_CUSTOMER_SK" as "ss_customer_id",
    "ss_store_sales"."SS_QUANTITY" as "ss_quantity",
    "ss_store_sales"."SS_SALES_PRICE" as "ss_sales_price"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."reason" as "ss_return_reason_reason" on "ss_store_returns"."SR_REASON_SK" = "ss_return_reason_reason"."R_REASON_SK"
WHERE
    "ss_return_reason_reason"."R_REASON_DESC" = 'reason 28'

GROUP BY
    1,
    2,
    3,
    4,
    "ss_return_reason_reason"."R_REASON_DESC"),
thoughtful as (
SELECT
    "cheerful"."ss_customer_id" as "customer_sk",
    "cheerful"."ss_customer_id" as "ss_customer_id",
    CASE
	WHEN "cheerful"."ss_return_quantity" is not null THEN ("cheerful"."ss_quantity" - "cheerful"."ss_return_quantity") * "cheerful"."ss_sales_price"
	ELSE "cheerful"."ss_quantity" * "cheerful"."ss_sales_price"
	END as "act_sales"
FROM
    "cheerful"),
cooperative as (
SELECT
    "cheerful"."ss_customer_id" as "ss_customer_id",
    sum("thoughtful"."act_sales") as "sumsales"
FROM
    "cheerful"
GROUP BY
    1)
SELECT
    "thoughtful"."customer_sk" as "customer_sk",
    "cooperative"."sumsales" as "sumsales"
FROM
    "cooperative"
    FULL JOIN "thoughtful" on "cooperative"."ss_customer_id" is not distinct from "thoughtful"."ss_customer_id"
ORDER BY 
    "cooperative"."sumsales" asc nulls first,
    "thoughtful"."customer_sk" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    "ss_store_sales"."SS_CUSTOMER_SK" as "customer_sk",
    sum(CASE
	WHEN "ss_store_returns"."SR_RETURN_QUANTITY" is not null THEN ("ss_store_sales"."SS_QUANTITY" - "ss_store_returns"."SR_RETURN_QUANTITY") * "ss_store_sales"."SS_SALES_PRICE"
	ELSE "ss_store_sales"."SS_QUANTITY" * "ss_store_sales"."SS_SALES_PRICE"
	END) as "sumsales"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."reason" as "ss_return_reason_reason" on "ss_store_returns"."SR_REASON_SK" = "ss_return_reason_reason"."R_REASON_SK"
WHERE
    "ss_return_reason_reason"."R_REASON_DESC" = 'reason 28'

GROUP BY
    1
ORDER BY 
    "sumsales" asc nulls first,
    "customer_sk" asc nulls first
LIMIT (100)
```

## v4 execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 179, in run_one
    result.v4_exec_seconds, result.v4_rows = _time(
                                             ~~~~~^
        lambda: execute(con, v4_sql)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 45, in _time
    value = fn()
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 180, in <lambda>
    lambda: execute(con, v4_sql)
            ~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 120, in execute
    cursor = con.execute(sql)
_duckdb.BinderException: Binder Error: Referenced table "thoughtful" not found!
Candidate tables: "cheerful"

LINE 34:     sum("thoughtful"."act_sales") as "sumsales"
                 ^
```
