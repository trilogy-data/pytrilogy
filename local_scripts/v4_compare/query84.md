# Query 84

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (16 rows) |
| results identical | NO |

## Result comparison

v4 rows: 100 (1 distinct)
ref rows: 16 (15 distinct)
only in v4 (showing up to 5 of 1):
  98x  ('AAAAAAAAAAPDAAAA', 'Benson, Floyd')
only in ref (showing up to 5 of 14):
  1x  ('AAAAAAAACBNCBAAA', 'Ferraro, Timothy')
  1x  ('AAAAAAAADMMCBAAA', 'Sandoval, Rosemary')
  1x  ('AAAAAAAAEDFBAAAA', 'Smallwood, Betty')
  1x  ('AAAAAAAAEEGCBAAA', 'Mattson, Alyson')
  1x  ('AAAAAAAAGLHFBAAA', 'Fisher, David')

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2546 | 47 | 15.07 ms |
| reference | 1772 | 20 | 14.09 ms |
| v4 / ref | 1.44x | 2.35x | 1.07x |

## Preql

```
import customer as customer;
import store_returns as returns;

# sr_cdemo_sk = cd_demo_sk in the reference; cross-table value join.
merge customer.demographics.id into returns.customer_demographic.id;

where
    customer.address.city = 'Edgewood'
    and returns.customer_demographic.id is not null
    and customer.household_demographic.id is not null
    and customer.household_demographic.income_band.lower_bound >= 38128
    and customer.household_demographic.income_band.upper_bound <= 38128 + 50000
select
    customer.text_id,
    concat(coalesce(customer.last_name, ''), ', ', coalesce(customer.first_name, '')) as customername,
    --returns.store_sales.ticket_number,
    --returns.item.id,
order by
    customer.text_id asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cooperative as (
SELECT
    "customer_customers"."C_CUSTOMER_ID" as "customer_text_id",
    "customer_customers"."C_FIRST_NAME" as "customer_first_name",
    "customer_customers"."C_LAST_NAME" as "customer_last_name",
    "returns_store_returns"."SR_ITEM_SK" as "returns_item_id",
    "returns_store_returns"."SR_TICKET_NUMBER" as "returns_store_sales_ticket_number"
FROM
    "memory"."customer" as "customer_customers"
    INNER JOIN "memory"."customer_address" as "customer_address_customer_address" on "customer_customers"."C_CURRENT_ADDR_SK" = "customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."store_returns" as "returns_store_returns" on "customer_customers"."C_CURRENT_CDEMO_SK" is not distinct from "returns_store_returns"."SR_CDEMO_SK"
    INNER JOIN "memory"."household_demographics" as "customer_household_demographic_household_demographics" on "customer_customers"."C_CURRENT_HDEMO_SK" = "customer_household_demographic_household_demographics"."HD_DEMO_SK"
    INNER JOIN "memory"."income_band" as "customer_household_demographic_income_band_income_band" on "customer_household_demographic_household_demographics"."HD_INCOME_BAND_SK" = "customer_household_demographic_income_band_income_band"."IB_INCOME_BAND_SK"
WHERE
    "customer_address_customer_address"."CA_CITY" = 'Edgewood' and coalesce("customer_customers"."C_CURRENT_CDEMO_SK","returns_store_returns"."SR_CDEMO_SK") is not null and coalesce("customer_customers"."C_CURRENT_HDEMO_SK","customer_household_demographic_household_demographics"."HD_DEMO_SK") is not null and "customer_household_demographic_income_band_income_band"."IB_LOWER_BOUND" >= 38128 and "customer_household_demographic_income_band_income_band"."IB_UPPER_BOUND" <= 38128 + 50000
),
uneven as (
SELECT
    "cooperative"."returns_store_sales_ticket_number" as "returns_store_sales_ticket_number"
FROM
    "cooperative"
GROUP BY
    1),
abundant as (
SELECT
    "cooperative"."returns_item_id" as "returns_item_id"
FROM
    "cooperative"
GROUP BY
    1),
questionable as (
SELECT
    "cooperative"."customer_text_id" as "customer_text_id",
    (coalesce("cooperative"."customer_last_name",'') || ', ' || coalesce("cooperative"."customer_first_name",'')) as "customername"
FROM
    "cooperative")
SELECT
    "questionable"."customer_text_id" as "customer_text_id",
    "questionable"."customername" as "customername"
FROM
    "questionable"
    FULL JOIN "abundant" on 1=1
    FULL JOIN "uneven" on 1=1
ORDER BY 
    "questionable"."customer_text_id" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    "customer_customers"."C_CUSTOMER_ID" as "customer_text_id",
    (coalesce("customer_customers"."C_LAST_NAME",'') || ', ' || coalesce("customer_customers"."C_FIRST_NAME",'')) as "customername"
FROM
    "memory"."customer" as "customer_customers"
    INNER JOIN "memory"."customer_address" as "customer_address_customer_address" on "customer_customers"."C_CURRENT_ADDR_SK" = "customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."store_returns" as "returns_store_returns" on "customer_customers"."C_CURRENT_CDEMO_SK" is not distinct from "returns_store_returns"."SR_CDEMO_SK"
    INNER JOIN "memory"."household_demographics" as "customer_household_demographic_household_demographics" on "customer_customers"."C_CURRENT_HDEMO_SK" = "customer_household_demographic_household_demographics"."HD_DEMO_SK"
    INNER JOIN "memory"."income_band" as "customer_household_demographic_income_band_income_band" on "customer_household_demographic_household_demographics"."HD_INCOME_BAND_SK" = "customer_household_demographic_income_band_income_band"."IB_INCOME_BAND_SK"
WHERE
    "customer_address_customer_address"."CA_CITY" = 'Edgewood' and coalesce("customer_customers"."C_CURRENT_CDEMO_SK","returns_store_returns"."SR_CDEMO_SK") is not null and coalesce("customer_customers"."C_CURRENT_HDEMO_SK","customer_household_demographic_household_demographics"."HD_DEMO_SK") is not null and "customer_household_demographic_income_band_income_band"."IB_LOWER_BOUND" >= 38128 and "customer_household_demographic_income_band_income_band"."IB_UPPER_BOUND" <= 38128 + 50000

GROUP BY
    1,
    2,
    "returns_store_returns"."SR_ITEM_SK",
    "returns_store_returns"."SR_TICKET_NUMBER"
ORDER BY 
    "customer_customers"."C_CUSTOMER_ID" asc nulls first
LIMIT (100)
```
