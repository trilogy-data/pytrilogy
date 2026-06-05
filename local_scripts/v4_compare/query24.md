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
| v4 | 4610 | 79 | 25.36 ms |
| reference | 4610 | 79 | 25.39 ms |
| v4 / ref | 1.00x | 1.00x | 1.00x |

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
vacuous as (
SELECT
    1 as "__preql_internal_all_rows"
),
abundant as (
SELECT
    "questionable"."physical_sales_billing_customer_first_name" as "physical_sales_billing_customer_first_name",
    "questionable"."physical_sales_billing_customer_id" as "physical_sales_billing_customer_id",
    "questionable"."physical_sales_billing_customer_last_name" as "physical_sales_billing_customer_last_name",
    "questionable"."physical_sales_item_id" as "physical_sales_item_id",
    "questionable"."physical_sales_net_paid" as "physical_sales_net_paid",
    "questionable"."physical_sales_store_id" as "physical_sales_store_id",
    "questionable"."physical_sales_store_name" as "physical_sales_store_name",
    CASE WHEN "questionable"."physical_sales_item_color" = 'peach' THEN "questionable"."physical_sales_net_paid" ELSE NULL END as "_virt_filter_net_paid_6433461400918323"
FROM
    "questionable"),
juicy as (
SELECT
    sum("abundant"."physical_sales_net_paid") as "_virt_agg_sum_9743820591646949"
FROM
    "abundant"
GROUP BY
    "abundant"."physical_sales_billing_customer_first_name",
    "abundant"."physical_sales_billing_customer_id",
    "abundant"."physical_sales_billing_customer_last_name",
    "abundant"."physical_sales_item_id",
    "abundant"."physical_sales_store_id",
    "abundant"."physical_sales_store_name"),
uneven as (
SELECT
    "abundant"."physical_sales_billing_customer_first_name" as "physical_sales_billing_customer_first_name",
    "abundant"."physical_sales_billing_customer_last_name" as "physical_sales_billing_customer_last_name",
    "abundant"."physical_sales_store_name" as "physical_sales_store_name",
    sum("abundant"."_virt_filter_net_paid_6433461400918323") as "peach_sales"
FROM
    "abundant"
GROUP BY
    1,
    2,
    3),
concerned as (
SELECT
    avg("juicy"."_virt_agg_sum_9743820591646949") as "avg_store_customer_sales"
FROM
    "vacuous"
    FULL JOIN "juicy" on 1=1
GROUP BY
    "vacuous"."__preql_internal_all_rows")
SELECT
    "uneven"."physical_sales_billing_customer_last_name" as "physical_sales_billing_customer_last_name",
    "uneven"."physical_sales_billing_customer_first_name" as "physical_sales_billing_customer_first_name",
    "uneven"."physical_sales_store_name" as "physical_sales_store_name",
    "uneven"."peach_sales" as "peach_sales"
FROM
    "concerned"
    INNER JOIN "uneven" on 1=1
WHERE
    "uneven"."peach_sales" > 0.05 * "concerned"."avg_store_customer_sales"
```

## Reference SQL (zquery log)

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
vacuous as (
SELECT
    1 as "__preql_internal_all_rows"
),
abundant as (
SELECT
    "questionable"."physical_sales_billing_customer_first_name" as "physical_sales_billing_customer_first_name",
    "questionable"."physical_sales_billing_customer_id" as "physical_sales_billing_customer_id",
    "questionable"."physical_sales_billing_customer_last_name" as "physical_sales_billing_customer_last_name",
    "questionable"."physical_sales_item_id" as "physical_sales_item_id",
    "questionable"."physical_sales_net_paid" as "physical_sales_net_paid",
    "questionable"."physical_sales_store_id" as "physical_sales_store_id",
    "questionable"."physical_sales_store_name" as "physical_sales_store_name",
    CASE WHEN "questionable"."physical_sales_item_color" = 'peach' THEN "questionable"."physical_sales_net_paid" ELSE NULL END as "_virt_filter_net_paid_6433461400918323"
FROM
    "questionable"),
juicy as (
SELECT
    sum("abundant"."physical_sales_net_paid") as "_virt_agg_sum_9743820591646949"
FROM
    "abundant"
GROUP BY
    "abundant"."physical_sales_billing_customer_first_name",
    "abundant"."physical_sales_billing_customer_id",
    "abundant"."physical_sales_billing_customer_last_name",
    "abundant"."physical_sales_item_id",
    "abundant"."physical_sales_store_id",
    "abundant"."physical_sales_store_name"),
uneven as (
SELECT
    "abundant"."physical_sales_billing_customer_first_name" as "physical_sales_billing_customer_first_name",
    "abundant"."physical_sales_billing_customer_last_name" as "physical_sales_billing_customer_last_name",
    "abundant"."physical_sales_store_name" as "physical_sales_store_name",
    sum("abundant"."_virt_filter_net_paid_6433461400918323") as "peach_sales"
FROM
    "abundant"
GROUP BY
    1,
    2,
    3),
concerned as (
SELECT
    avg("juicy"."_virt_agg_sum_9743820591646949") as "avg_store_customer_sales"
FROM
    "vacuous"
    FULL JOIN "juicy" on 1=1
GROUP BY
    "vacuous"."__preql_internal_all_rows")
SELECT
    "uneven"."physical_sales_billing_customer_last_name" as "physical_sales_billing_customer_last_name",
    "uneven"."physical_sales_billing_customer_first_name" as "physical_sales_billing_customer_first_name",
    "uneven"."physical_sales_store_name" as "physical_sales_store_name",
    "uneven"."peach_sales" as "peach_sales"
FROM
    "concerned"
    INNER JOIN "uneven" on 1=1
WHERE
    "uneven"."peach_sales" > 0.05 * "concerned"."avg_store_customer_sales"
```
