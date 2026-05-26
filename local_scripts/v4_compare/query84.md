# Query 84

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (16 rows) |
| reference execution | OK (16 rows) |
| results identical | NO |

## Result comparison

v4 rows: 16 (15 distinct)
ref rows: 16 (15 distinct)
only in v4 (showing up to 5 of 15):
  2x  ('Edgewood', 'Floyd', 1626, 60001, 70000, 'Benson', 'AAAAAAAAAAPDAAAA', 'Benson, Floyd', 1902498)
  1x  ('Edgewood', 'Timothy', 644, 40001, 50000, 'Ferraro', 'AAAAAAAACBNCBAAA', 'Ferraro, Timothy', 1008396)
  1x  ('Edgewood', 'Rosemary', 6326, 60001, 70000, 'Sandoval', 'AAAAAAAADMMCBAAA', 'Sandoval, Rosemary', 1298685)
  1x  ('Edgewood', 'Betty', 2665, 50001, 60000, 'Smallwood', 'AAAAAAAAEDFBAAAA', 'Smallwood, Betty', 1168678)
  1x  ('Edgewood', 'Alyson', 3786, 60001, 70000, 'Mattson', 'AAAAAAAAEEGCBAAA', 'Mattson, Alyson', 72894)
only in ref (showing up to 5 of 15):
  2x  ('AAAAAAAAAAPDAAAA', 'Benson, Floyd')
  1x  ('AAAAAAAACBNCBAAA', 'Ferraro, Timothy')
  1x  ('AAAAAAAADMMCBAAA', 'Sandoval, Rosemary')
  1x  ('AAAAAAAAEDFBAAAA', 'Smallwood, Betty')
  1x  ('AAAAAAAAEEGCBAAA', 'Mattson, Alyson')

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2443 | 22 | 11.97 ms |
| reference | 1772 | 20 | 20.26 ms |
| v4 / ref | 1.38x | 1.10x | 0.59x |

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
SELECT
    (coalesce("customer_customers"."C_LAST_NAME",'') || ', ' || coalesce("customer_customers"."C_FIRST_NAME",'')) as "customername",
    "customer_customers"."C_LAST_NAME" as "customer_last_name",
    "customer_customers"."C_FIRST_NAME" as "customer_first_name",
    coalesce("customer_customers"."C_CURRENT_CDEMO_SK","returns_store_returns"."SR_CDEMO_SK") as "returns_customer_demographic_id",
    coalesce("customer_customers"."C_CURRENT_HDEMO_SK","customer_household_demographic_household_demographics"."HD_DEMO_SK") as "customer_household_demographic_id",
    "customer_household_demographic_income_band_income_band"."IB_UPPER_BOUND" as "customer_household_demographic_income_band_upper_bound",
    "customer_customers"."C_CUSTOMER_ID" as "customer_text_id",
    "customer_household_demographic_income_band_income_band"."IB_LOWER_BOUND" as "customer_household_demographic_income_band_lower_bound",
    "customer_address_customer_address"."CA_CITY" as "customer_address_city"
FROM
    "memory"."customer_address" as "customer_address_customer_address"
    INNER JOIN "memory"."customer" as "customer_customers" on "customer_address_customer_address"."CA_ADDRESS_SK" = "customer_customers"."C_CURRENT_ADDR_SK"
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
    "memory"."customer_address" as "customer_address_customer_address"
    INNER JOIN "memory"."customer" as "customer_customers" on "customer_address_customer_address"."CA_ADDRESS_SK" = "customer_customers"."C_CURRENT_ADDR_SK"
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
