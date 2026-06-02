# Query 84

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
| v4 | 1660 | 15 | 2.23 ms |
| reference | 1772 | 20 | 4.05 ms |
| v4 / ref | 0.94x | 0.75x | 0.55x |

## Preql

```
import customer as customer;
import physical_returns as returns;

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
    --returns.ticket_number,
    --returns.item.id,
order by
    customer.text_id asc nulls first
limit 100
;
```

## v4 generated SQL

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

ORDER BY 
    "customer_customers"."C_CUSTOMER_ID" asc nulls first
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
