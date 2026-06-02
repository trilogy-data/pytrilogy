# Query 93

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (0 rows) |
| reference execution | OK (0 rows) |
| results identical | YES |

## Result comparison

v4 rows: 0 (0 distinct)
ref rows: 0 (0 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 914 | 19 | 1.26 ms |
| reference | 914 | 19 | 1.25 ms |
| v4 / ref | 1.00x | 1.00x | 1.01x |

## Preql

```
import physical_sales as ss;

auto act_sales <- case
    when ss.return_quantity is not null then (ss.quantity - ss.return_quantity) * ss.sales_price
    else ss.quantity * ss.sales_price
end;

where
    ss.return_reason.desc = 'reason 28'
select
    ss.billing_customer.id as customer_sk,
    sum(act_sales) by ss.billing_customer.id as sumsales,
order by
    sumsales asc nulls first,
    customer_sk asc nulls first
limit 100
;
```

## v4 generated SQL

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
