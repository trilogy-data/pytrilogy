# Query 84

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (16 rows) |
| results identical | NO |

## Result comparison

v4 rows: 100 (99 distinct)
ref rows: 16 (15 distinct)
only in v4 (showing up to 5 of 99):
  1x  ('Edgewood', 'Robert', 2924, 40001, 50000, 'Cox', 'AAAAAAAAAAAPAAAA', 'Cox, Robert', 179829)
  1x  ('Edgewood', 'Jeffery', 2586, 60001, 70000, 'Kelly', 'AAAAAAAAAACFAAAA', 'Kelly, Jeffery', 1004871)
  1x  ('Edgewood', 'Ronald', 5766, 60001, 70000, 'Moreno', 'AAAAAAAAAAILAAAA', 'Moreno, Ronald', 1455081)
  2x  ('Edgewood', 'Floyd', 1626, 60001, 70000, 'Benson', 'AAAAAAAAAAPDAAAA', 'Benson, Floyd', 1902498)
  1x  ('Edgewood', 'Quinn', 1926, 60001, 70000, 'Parish', 'AAAAAAAAADJCAAAA', 'Parish, Quinn', 620152)
only in ref (showing up to 5 of 15):
  2x  ('AAAAAAAAAAPDAAAA', 'Benson, Floyd')
  1x  ('AAAAAAAACBNCBAAA', 'Ferraro, Timothy')
  1x  ('AAAAAAAADMMCBAAA', 'Sandoval, Rosemary')
  1x  ('AAAAAAAAEDFBAAAA', 'Smallwood, Betty')
  1x  ('AAAAAAAAEEGCBAAA', 'Mattson, Alyson')

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 5014 | 74 |
| reference | 1772 | 20 |
| v4 / ref | 2.83x | 3.70x |

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
thoughtful as (
SELECT
    "returns_store_returns"."SR_CDEMO_SK" as "returns_customer_demographic_id",
    "returns_store_returns"."SR_ITEM_SK" as "returns_item_id",
    "returns_store_returns"."SR_TICKET_NUMBER" as "returns_store_sales_ticket_number"
FROM
    "memory"."store_returns" as "returns_store_returns"),
cheerful as (
SELECT
    "customer_household_demographic_income_band_income_band"."IB_INCOME_BAND_SK" as "customer_household_demographic_income_band_id",
    "customer_household_demographic_income_band_income_band"."IB_LOWER_BOUND" as "customer_household_demographic_income_band_lower_bound",
    "customer_household_demographic_income_band_income_band"."IB_UPPER_BOUND" as "customer_household_demographic_income_band_upper_bound"
FROM
    "memory"."income_band" as "customer_household_demographic_income_band_income_band"),
wakeful as (
SELECT
    "customer_household_demographic_household_demographics"."HD_DEMO_SK" as "customer_household_demographic_id",
    "customer_household_demographic_household_demographics"."HD_INCOME_BAND_SK" as "customer_household_demographic_income_band_id"
FROM
    "memory"."household_demographics" as "customer_household_demographic_household_demographics"),
highfalutin as (
SELECT
    "customer_customers"."C_CURRENT_ADDR_SK" as "customer_address_id",
    "customer_customers"."C_CURRENT_CDEMO_SK" as "returns_customer_demographic_id",
    "customer_customers"."C_CURRENT_HDEMO_SK" as "customer_household_demographic_id",
    "customer_customers"."C_CUSTOMER_ID" as "customer_text_id",
    "customer_customers"."C_CUSTOMER_SK" as "customer_id",
    "customer_customers"."C_FIRST_NAME" as "customer_first_name",
    "customer_customers"."C_LAST_NAME" as "customer_last_name"
FROM
    "memory"."customer" as "customer_customers"),
quizzical as (
SELECT
    "customer_address_customer_address"."CA_ADDRESS_SK" as "customer_address_id",
    "customer_address_customer_address"."CA_CITY" as "customer_address_city"
FROM
    "memory"."customer_address" as "customer_address_customer_address"),
cooperative as (
SELECT
    "cheerful"."customer_household_demographic_income_band_lower_bound" as "customer_household_demographic_income_band_lower_bound",
    "cheerful"."customer_household_demographic_income_band_upper_bound" as "customer_household_demographic_income_band_upper_bound",
    "highfalutin"."customer_first_name" as "customer_first_name",
    "highfalutin"."customer_last_name" as "customer_last_name",
    "highfalutin"."customer_text_id" as "customer_text_id",
    "quizzical"."customer_address_city" as "customer_address_city",
    "thoughtful"."returns_item_id" as "returns_item_id",
    "thoughtful"."returns_store_sales_ticket_number" as "returns_store_sales_ticket_number",
    coalesce("highfalutin"."customer_household_demographic_id","wakeful"."customer_household_demographic_id") as "customer_household_demographic_id",
    coalesce("highfalutin"."returns_customer_demographic_id","thoughtful"."returns_customer_demographic_id") as "returns_customer_demographic_id"
FROM
    "quizzical"
    INNER JOIN "highfalutin" on "quizzical"."customer_address_id" = "highfalutin"."customer_address_id"
    FULL JOIN "thoughtful" on "highfalutin"."returns_customer_demographic_id" is not distinct from "thoughtful"."returns_customer_demographic_id"
    FULL JOIN "wakeful" on "highfalutin"."customer_household_demographic_id" = "wakeful"."customer_household_demographic_id"
    LEFT OUTER JOIN "cheerful" on "wakeful"."customer_household_demographic_income_band_id" = "cheerful"."customer_household_demographic_income_band_id"
WHERE
    "quizzical"."customer_address_city" = 'Edgewood' and coalesce("highfalutin"."returns_customer_demographic_id","thoughtful"."returns_customer_demographic_id") is not null and coalesce("highfalutin"."customer_household_demographic_id","wakeful"."customer_household_demographic_id") is not null and "cheerful"."customer_household_demographic_income_band_lower_bound" >= 38128 and "cheerful"."customer_household_demographic_income_band_upper_bound" <= 38128 + 50000
)
SELECT
    (coalesce("cooperative"."customer_last_name",'') || ', ' || coalesce("cooperative"."customer_first_name",'')) as "customername",
    "cooperative"."customer_household_demographic_income_band_upper_bound" as "customer_household_demographic_income_band_upper_bound",
    "cooperative"."customer_text_id" as "customer_text_id",
    "cooperative"."customer_household_demographic_id" as "customer_household_demographic_id",
    "cooperative"."customer_household_demographic_income_band_lower_bound" as "customer_household_demographic_income_band_lower_bound",
    "cooperative"."returns_customer_demographic_id" as "returns_customer_demographic_id",
    "cooperative"."customer_first_name" as "customer_first_name",
    "cooperative"."customer_last_name" as "customer_last_name",
    "cooperative"."customer_address_city" as "customer_address_city"
FROM
    "cooperative"
ORDER BY 
    "cooperative"."customer_text_id" asc nulls first
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
