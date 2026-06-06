# Query 01

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | YES |

## Result comparison

v4 rows: 100 (92 distinct)
ref rows: 100 (92 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2425 | 66 | 38.06 ms |
| reference | 1822 | 48 | 26.23 ms |
| v4 / ref | 1.33x | 1.38x | 1.45x |

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
abundant as (
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
wakeful as (
SELECT
    "returns_billing_customer_customers"."C_CUSTOMER_ID" as "returns_billing_customer_text_id",
    "returns_billing_customer_customers"."C_CUSTOMER_SK" as "returns_billing_customer_id",
    "returns_store_returns"."SR_STORE_SK" as "returns_store_id"
FROM
    "memory"."store_returns" as "returns_store_returns"
    INNER JOIN "memory"."customer" as "returns_billing_customer_customers" on "returns_store_returns"."SR_CUSTOMER_SK" = "returns_billing_customer_customers"."C_CUSTOMER_SK"),
juicy as (
SELECT
    "abundant"."returns_store_id" as "returns_store_id",
    avg("abundant"."total_returns") as "avg_store_returns"
FROM
    "abundant"
GROUP BY
    1),
thoughtful as (
SELECT
    "wakeful"."returns_store_id" as "returns_store_id"
FROM
    "wakeful"
GROUP BY
    1),
cheerful as (
SELECT
    "wakeful"."returns_billing_customer_id" as "returns_billing_customer_id",
    "wakeful"."returns_billing_customer_text_id" as "returns_billing_customer_text_id"
FROM
    "wakeful"
GROUP BY
    1,
    2),
concerned as (
SELECT
    "cheerful"."returns_billing_customer_text_id" as "returns_billing_customer_text_id"
FROM
    "abundant"
    INNER JOIN "cheerful" on "abundant"."returns_billing_customer_id" = "cheerful"."returns_billing_customer_id"
    INNER JOIN "juicy" on "abundant"."returns_store_id" = "juicy"."returns_store_id"
    INNER JOIN "thoughtful" on "abundant"."returns_store_id" = "thoughtful"."returns_store_id"
WHERE
    "abundant"."total_returns" > ( 1.2 * "juicy"."avg_store_returns" )
)
SELECT
    "concerned"."returns_billing_customer_text_id" as "returns_billing_customer_text_id"
FROM
    "concerned"
ORDER BY 
    "concerned"."returns_billing_customer_text_id" asc
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
