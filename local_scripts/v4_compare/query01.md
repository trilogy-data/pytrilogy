# Query 01

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (100 rows) |

## Preql

```
import store_returns as returns;

where
    returns.store.state = 'TN' and returns.return_date.year = 2000
select
    returns.customer.text_id,
    --sum(returns.return_amount) by returns.customer.id, returns.store.id as total_returns,
    --returns.store.id,
    --avg(total_returns) by returns.store.id as avg_store_returns,
having
    total_returns > (1.2 * avg_store_returns)

order by
    returns.customer.text_id asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "returns_store_returns"."SR_CUSTOMER_SK" as "returns_customer_id",
    "returns_store_returns"."SR_RETURNED_DATE_SK" as "returns_return_date_id",
    "returns_store_returns"."SR_RETURN_AMT" as "returns_return_amount",
    "returns_store_returns"."SR_STORE_SK" as "returns_store_id"
FROM
    "memory"."store_returns" as "returns_store_returns"),
wakeful as (
SELECT
    "returns_store_store"."S_STATE" as "returns_store_state",
    "returns_store_store"."S_STORE_SK" as "returns_store_id"
FROM
    "memory"."store" as "returns_store_store"),
highfalutin as (
SELECT
    "returns_return_date_date"."D_DATE_SK" as "returns_return_date_id",
    "returns_return_date_date"."D_YEAR" as "returns_return_date_year"
FROM
    "memory"."date_dim" as "returns_return_date_date"),
quizzical as (
SELECT
    "returns_customer_customers"."C_CUSTOMER_ID" as "returns_customer_text_id",
    "returns_customer_customers"."C_CUSTOMER_SK" as "returns_customer_id"
FROM
    "memory"."customer" as "returns_customer_customers"),
thoughtful as (
SELECT
    "cheerful"."returns_return_amount" as "returns_return_amount",
    "highfalutin"."returns_return_date_year" as "returns_return_date_year",
    "quizzical"."returns_customer_id" as "returns_customer_id",
    "quizzical"."returns_customer_text_id" as "returns_customer_text_id",
    "wakeful"."returns_store_id" as "returns_store_id",
    "wakeful"."returns_store_state" as "returns_store_state"
FROM
    "cheerful"
    INNER JOIN "wakeful" on "cheerful"."returns_store_id" = "wakeful"."returns_store_id"
    INNER JOIN "quizzical" on "cheerful"."returns_customer_id" = "quizzical"."returns_customer_id"
    INNER JOIN "highfalutin" on "cheerful"."returns_return_date_id" = "highfalutin"."returns_return_date_id"
WHERE
    "wakeful"."returns_store_state" = 'TN' and "highfalutin"."returns_return_date_year" = 2000
),
cooperative as (
SELECT
    "thoughtful"."returns_customer_id" as "returns_customer_id",
    "thoughtful"."returns_store_id" as "returns_store_id",
    sum("thoughtful"."returns_return_amount") as "total_returns"
FROM
    "thoughtful"
WHERE
    "thoughtful"."returns_store_state" = 'TN' and "thoughtful"."returns_return_date_year" = 2000

GROUP BY
    1,
    2)
SELECT
    avg("cooperative"."total_returns") as "avg_store_returns",
    "cooperative"."returns_store_id" as "returns_store_id"
FROM
    "cooperative"
WHERE
    "thoughtful"."returns_store_state" = 'TN' and "thoughtful"."returns_return_date_year" = 2000

GROUP BY
    2
```

## Reference SQL (zquery log)

```sql
WITH 
thoughtful as (
SELECT
    "returns_store_returns"."SR_CUSTOMER_SK" as "returns_customer_id",
    "returns_store_store"."S_STORE_SK" as "returns_store_id",
    sum("returns_store_returns"."SR_RETURN_AMT") as "total_returns"
FROM
    "memory"."store_returns" as "returns_store_returns"
    INNER JOIN "memory"."store" as "returns_store_store" on "returns_store_returns"."SR_STORE_SK" = "returns_store_store"."S_STORE_SK"
    INNER JOIN "memory"."date_dim" as "returns_return_date_date" on "returns_store_returns"."SR_RETURNED_DATE_SK" = "returns_return_date_date"."D_DATE_SK"
WHERE
    "returns_store_store"."S_STATE" = 'TN' and "returns_return_date_date"."D_YEAR" = 2000

GROUP BY
    1,
    2),
abundant as (
SELECT
    "thoughtful"."returns_store_id" as "returns_store_id",
    avg("thoughtful"."total_returns") as "avg_store_returns"
FROM
    "thoughtful"
GROUP BY
    1),
questionable as (
SELECT
    "returns_customer_customers"."C_CUSTOMER_ID" as "returns_customer_text_id",
    "thoughtful"."returns_store_id" as "returns_store_id",
    "thoughtful"."total_returns" as "total_returns"
FROM
    "thoughtful"
    INNER JOIN "memory"."customer" as "returns_customer_customers" on "thoughtful"."returns_customer_id" = "returns_customer_customers"."C_CUSTOMER_SK"),
uneven as (
SELECT
    "questionable"."returns_customer_text_id" as "returns_customer_text_id"
FROM
    "questionable"
    INNER JOIN "abundant" on "questionable"."returns_store_id" = "abundant"."returns_store_id"
WHERE
    "questionable"."total_returns" > ( 1.2 * "abundant"."avg_store_returns" )
)
SELECT
    "uneven"."returns_customer_text_id" as "returns_customer_text_id"
FROM
    "uneven"
ORDER BY 
    "uneven"."returns_customer_text_id" asc
LIMIT (100)
```

## v4 execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 151, in run_one
    result.v4_rows = execute(con, v4_sql)
                     ~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 98, in execute
    return list(con.execute(sql).fetchall())
                ~~~~~~~~~~~^^^^^
_duckdb.BinderException: Binder Error: Referenced table "thoughtful" not found!
Candidate tables: "cooperative"

LINE 63:     "thoughtful"."returns_store_state" = 'TN' and "thoughtful...
             ^
```
