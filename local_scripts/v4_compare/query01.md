# Query 01

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
only in v4 (showing up to 5 of 1):
  1x  ('AAAAAAAAHKBAAAAA',)
only in ref (showing up to 5 of 1):
  1x  ('AAAAAAAAPEBAAAAA',)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 1652 | 42 | 8.96 ms |
| reference | 1822 | 48 | 4.26 ms |
| v4 / ref | 0.91x | 0.88x | 2.10x |

## Preql

```
import physical_returns as returns;

where
    returns.store.state = 'TN' and returns.return_date.year = 2000
select
    returns.billing_customer.text_id,
    --sum(returns.return_amount) by returns.billing_customer.id, returns.store.id as total_returns,
    --returns.store.id,
    --avg(total_returns) by returns.store.id as avg_store_returns,
having
    total_returns > (1.2 * avg_store_returns)

order by
    returns.billing_customer.text_id asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
thoughtful as (
SELECT
    "returns_billing_customer_customers"."C_CUSTOMER_ID" as "returns_billing_customer_text_id",
    "returns_store_store"."S_STORE_SK" as "returns_store_id",
    sum("returns_store_returns"."SR_RETURN_AMT") as "total_returns"
FROM
    "memory"."store_returns" as "returns_store_returns"
    INNER JOIN "memory"."store" as "returns_store_store" on "returns_store_returns"."SR_STORE_SK" = "returns_store_store"."S_STORE_SK"
    INNER JOIN "memory"."date_dim" as "returns_return_date_date" on "returns_store_returns"."SR_RETURNED_DATE_SK" = "returns_return_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer" as "returns_billing_customer_customers" on "returns_store_returns"."SR_CUSTOMER_SK" = "returns_billing_customer_customers"."C_CUSTOMER_SK"
WHERE
    "returns_store_store"."S_STATE" = 'TN' and "returns_return_date_date"."D_YEAR" = 2000

GROUP BY
    1,
    2,
    "returns_billing_customer_customers"."C_CUSTOMER_SK"),
questionable as (
SELECT
    "thoughtful"."returns_store_id" as "returns_store_id",
    avg("thoughtful"."total_returns") as "avg_store_returns"
FROM
    "thoughtful"
GROUP BY
    1)
SELECT
    "thoughtful"."returns_billing_customer_text_id" as "returns_billing_customer_text_id"
FROM
    "thoughtful"
    INNER JOIN "questionable" on "thoughtful"."returns_store_id" = "questionable"."returns_store_id"
WHERE
    "thoughtful"."total_returns" > ( 1.2 * "questionable"."avg_store_returns" )

GROUP BY
    1,
    "questionable"."avg_store_returns",
    "thoughtful"."returns_store_id",
    "thoughtful"."total_returns"
ORDER BY 
    "thoughtful"."returns_billing_customer_text_id" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
thoughtful as (
SELECT
    "returns_store_returns"."SR_CUSTOMER_SK" as "returns_billing_customer_id",
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
    "returns_billing_customer_customers"."C_CUSTOMER_ID" as "returns_billing_customer_text_id",
    "thoughtful"."returns_store_id" as "returns_store_id",
    "thoughtful"."total_returns" as "total_returns"
FROM
    "thoughtful"
    INNER JOIN "memory"."customer" as "returns_billing_customer_customers" on "thoughtful"."returns_billing_customer_id" = "returns_billing_customer_customers"."C_CUSTOMER_SK"),
uneven as (
SELECT
    "questionable"."returns_billing_customer_text_id" as "returns_billing_customer_text_id"
FROM
    "questionable"
    INNER JOIN "abundant" on "questionable"."returns_store_id" = "abundant"."returns_store_id"
WHERE
    "questionable"."total_returns" > ( 1.2 * "abundant"."avg_store_returns" )
)
SELECT
    "uneven"."returns_billing_customer_text_id" as "returns_billing_customer_text_id"
FROM
    "uneven"
ORDER BY 
    "uneven"."returns_billing_customer_text_id" asc
LIMIT (100)
```
