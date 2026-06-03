# Query 24

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
| v4 | 3890 | 67 | 53.12 ms |
| reference | 4127 | 52 | 93.02 ms |
| v4 / ref | 0.94x | 1.29x | 0.57x |

## Preql

```
import physical_sales as physical_sales;

where
    physical_sales.store.market = 8
    and physical_sales.billing_customer.birth_country != upper(physical_sales.billing_customer.address.country)
    and physical_sales.is_returned is True
    and physical_sales.store.zip = physical_sales.billing_customer.address.zip
select
    physical_sales.billing_customer.last_name,
    physical_sales.billing_customer.first_name,
    physical_sales.store.name,
    --avg(
            sum(physical_sales.net_paid)
                by physical_sales.billing_customer.id, physical_sales.item.id, physical_sales.store.id
        )
            by * as avg_store_customer_sales,
    sum(physical_sales.net_paid ? physical_sales.item.color = 'peach') as peach_sales,
having
    peach_sales > 0.05 * avg_store_customer_sales

;
```

## v4 generated SQL

```sql
WITH 
questionable as (
SELECT
    "physical_sales_billing_customer_customers"."C_FIRST_NAME" as "physical_sales_billing_customer_first_name",
    "physical_sales_billing_customer_customers"."C_LAST_NAME" as "physical_sales_billing_customer_last_name",
    "physical_sales_item_items"."I_COLOR" as "physical_sales_item_color",
    "physical_sales_item_items"."I_ITEM_SK" as "physical_sales_item_id",
    "physical_sales_store_sales"."SS_CUSTOMER_SK" as "physical_sales_billing_customer_id",
    "physical_sales_store_sales"."SS_NET_PAID" as "physical_sales_net_paid",
    "physical_sales_store_sales"."SS_STORE_SK" as "physical_sales_store_id",
    "physical_sales_store_store"."S_STORE_NAME" as "physical_sales_store_name"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    LEFT OUTER JOIN "memory"."store_returns" as "physical_sales_store_returns" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_store_returns"."SR_ITEM_SK" AND "physical_sales_store_sales"."SS_TICKET_NUMBER" = "physical_sales_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."customer" as "physical_sales_billing_customer_customers" on "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "physical_sales_billing_customer_address_customer_address" on "physical_sales_billing_customer_customers"."C_CURRENT_ADDR_SK" = "physical_sales_billing_customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
WHERE
    "physical_sales_store_store"."S_MARKET_ID" = 8 and "physical_sales_billing_customer_customers"."C_BIRTH_COUNTRY" != UPPER("physical_sales_billing_customer_address_customer_address"."CA_COUNTRY")  and SR_RETURN_TIME_SK IS NOT NULL is True and "physical_sales_store_store"."S_ZIP" = "physical_sales_billing_customer_address_customer_address"."CA_ZIP"
),
juicy as (
SELECT
    1 as "__preql_internal_all_rows"
),
yummy as (
SELECT
    sum("questionable"."physical_sales_net_paid") as "_virt_agg_sum_9743820591646949"
FROM
    "questionable"
GROUP BY
    "questionable"."physical_sales_billing_customer_first_name",
    "questionable"."physical_sales_billing_customer_id",
    "questionable"."physical_sales_billing_customer_last_name",
    "questionable"."physical_sales_item_id",
    "questionable"."physical_sales_store_id",
    "questionable"."physical_sales_store_name"),
abundant as (
SELECT
    "questionable"."physical_sales_billing_customer_first_name" as "physical_sales_billing_customer_first_name",
    "questionable"."physical_sales_billing_customer_last_name" as "physical_sales_billing_customer_last_name",
    "questionable"."physical_sales_store_name" as "physical_sales_store_name",
    sum(CASE WHEN "questionable"."physical_sales_item_color" = 'peach' THEN "questionable"."physical_sales_net_paid" ELSE NULL END) as "peach_sales"
FROM
    "questionable"
GROUP BY
    1,
    2,
    3),
vacuous as (
SELECT
    avg("yummy"."_virt_agg_sum_9743820591646949") as "avg_store_customer_sales"
FROM
    "juicy"
    FULL JOIN "yummy" on 1=1
GROUP BY
    "juicy"."__preql_internal_all_rows")
SELECT
    "abundant"."physical_sales_billing_customer_last_name" as "physical_sales_billing_customer_last_name",
    "abundant"."physical_sales_billing_customer_first_name" as "physical_sales_billing_customer_first_name",
    "abundant"."physical_sales_store_name" as "physical_sales_store_name",
    "abundant"."peach_sales" as "peach_sales"
FROM
    "vacuous"
    INNER JOIN "abundant" on 1=1
WHERE
    "abundant"."peach_sales" > 0.05 * "vacuous"."avg_store_customer_sales"
```

## Reference SQL (zquery log)

```sql
WITH 
yummy as (
SELECT
    sum("physical_sales_store_sales"."SS_NET_PAID") as "_virt_agg_sum_9743820591646949"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    LEFT OUTER JOIN "memory"."store_returns" as "physical_sales_store_returns" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_store_returns"."SR_ITEM_SK" AND "physical_sales_store_sales"."SS_TICKET_NUMBER" = "physical_sales_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "physical_sales_billing_customer_customers" on "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "physical_sales_billing_customer_address_customer_address" on "physical_sales_billing_customer_customers"."C_CURRENT_ADDR_SK" = "physical_sales_billing_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "physical_sales_store_store"."S_MARKET_ID" = 8 and "physical_sales_billing_customer_customers"."C_BIRTH_COUNTRY" != UPPER("physical_sales_billing_customer_address_customer_address"."CA_COUNTRY")  and SR_RETURN_TIME_SK IS NOT NULL is True and "physical_sales_store_store"."S_ZIP" = "physical_sales_billing_customer_address_customer_address"."CA_ZIP"

GROUP BY
    "physical_sales_store_sales"."SS_CUSTOMER_SK",
    "physical_sales_store_sales"."SS_ITEM_SK",
    "physical_sales_store_sales"."SS_STORE_SK"),
questionable as (
SELECT
    "physical_sales_billing_customer_customers"."C_FIRST_NAME" as "physical_sales_billing_customer_first_name",
    "physical_sales_billing_customer_customers"."C_LAST_NAME" as "physical_sales_billing_customer_last_name",
    "physical_sales_store_store"."S_STORE_NAME" as "physical_sales_store_name",
    sum(CASE WHEN "physical_sales_item_items"."I_COLOR" = 'peach' THEN "physical_sales_store_sales"."SS_NET_PAID" ELSE NULL END) as "peach_sales"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    LEFT OUTER JOIN "memory"."store_returns" as "physical_sales_store_returns" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_store_returns"."SR_ITEM_SK" AND "physical_sales_store_sales"."SS_TICKET_NUMBER" = "physical_sales_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."customer" as "physical_sales_billing_customer_customers" on "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "physical_sales_billing_customer_address_customer_address" on "physical_sales_billing_customer_customers"."C_CURRENT_ADDR_SK" = "physical_sales_billing_customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
WHERE
    "physical_sales_store_store"."S_MARKET_ID" = 8 and "physical_sales_billing_customer_customers"."C_BIRTH_COUNTRY" != UPPER("physical_sales_billing_customer_address_customer_address"."CA_COUNTRY")  and SR_RETURN_TIME_SK IS NOT NULL is True and "physical_sales_store_store"."S_ZIP" = "physical_sales_billing_customer_address_customer_address"."CA_ZIP"

GROUP BY
    1,
    2,
    3),
vacuous as (
SELECT
    avg("yummy"."_virt_agg_sum_9743820591646949") as "avg_store_customer_sales"
FROM
    "yummy")
SELECT
    "questionable"."physical_sales_billing_customer_last_name" as "physical_sales_billing_customer_last_name",
    "questionable"."physical_sales_billing_customer_first_name" as "physical_sales_billing_customer_first_name",
    "questionable"."physical_sales_store_name" as "physical_sales_store_name",
    "questionable"."peach_sales" as "peach_sales"
FROM
    "vacuous"
    INNER JOIN "questionable" on 1=1
WHERE
    "questionable"."peach_sales" > 0.05 * "vacuous"."avg_store_customer_sales"
```
