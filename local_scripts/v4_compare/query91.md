# Query 91

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (3 rows) |
| reference execution | OK (2 rows) |
| results identical | NO |

## Result comparison

v4 rows: 3 (2 distinct)
ref rows: 2 (2 distinct)
only in v4 (showing up to 5 of 1):
  1x  ('AAAAAAAABAAAAAAA', 'NY Metro', 'Bob Belcher', Decimal('4725.36'))

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 7074 | 141 |
| reference | 2219 | 24 |
| v4 / ref | 3.19x | 5.88x |

## Preql

```
import catalog_returns as cr;

where
    cr.date.year = 1998
    and cr.date.month_of_year = 11
    and (
        (
            cr.customer.demographics.marital_status = 'M'
            and cr.customer.demographics.education_status = 'Unknown'
        )
        or (
            cr.customer.demographics.marital_status = 'W'
            and cr.customer.demographics.education_status = 'Advanced Degree'
        )
    )
    and cr.customer.household_demographic.buy_potential like 'Unknown%'
    and cr.customer.address.gmt_offset = -7
select
    cr.call_center.text_id as call_center,
    cr.call_center.name as call_center_name,
    cr.call_center.manager as manager,
    sum(cr.net_loss)
            by cr.call_center.text_id, cr.call_center.name, cr.call_center.manager, cr.customer.demographics.marital_status, cr.customer.demographics.education_status as returns_loss,
order by
    returns_loss desc nulls first
;
```

## v4 generated SQL

```sql
WITH 
abundant as (
SELECT
    "cr_date_date"."D_DATE_SK" as "cr_date_id",
    "cr_date_date"."D_MOY" as "cr_date_month_of_year",
    "cr_date_date"."D_YEAR" as "cr_date_year"
FROM
    "memory"."date_dim" as "cr_date_date"),
questionable as (
SELECT
    "cr_customer_household_demographic_household_demographics"."HD_BUY_POTENTIAL" as "cr_customer_household_demographic_buy_potential",
    "cr_customer_household_demographic_household_demographics"."HD_DEMO_SK" as "cr_customer_household_demographic_id"
FROM
    "memory"."household_demographics" as "cr_customer_household_demographic_household_demographics"),
cooperative as (
SELECT
    "cr_customer_demographics_customer_demographics"."CD_DEMO_SK" as "cr_customer_demographics_id",
    "cr_customer_demographics_customer_demographics"."CD_EDUCATION_STATUS" as "cr_customer_demographics_education_status",
    "cr_customer_demographics_customer_demographics"."CD_MARITAL_STATUS" as "cr_customer_demographics_marital_status"
FROM
    "memory"."customer_demographics" as "cr_customer_demographics_customer_demographics"),
thoughtful as (
SELECT
    "cr_customer_customers"."C_CURRENT_ADDR_SK" as "cr_customer_address_id",
    "cr_customer_customers"."C_CURRENT_CDEMO_SK" as "cr_customer_demographics_id",
    "cr_customer_customers"."C_CURRENT_HDEMO_SK" as "cr_customer_household_demographic_id",
    "cr_customer_customers"."C_CUSTOMER_SK" as "cr_customer_id"
FROM
    "memory"."customer" as "cr_customer_customers"),
cheerful as (
SELECT
    "cr_customer_address_customer_address"."CA_ADDRESS_SK" as "cr_customer_address_id",
    "cr_customer_address_customer_address"."CA_GMT_OFFSET" as "cr_customer_address_gmt_offset"
FROM
    "memory"."customer_address" as "cr_customer_address_customer_address"),
highfalutin as (
SELECT
    "cr_catalog_returns"."CR_CALL_CENTER_SK" as "cr_call_center_id",
    "cr_catalog_returns"."CR_NET_LOSS" as "cr_net_loss",
    "cr_catalog_returns"."CR_RETURNED_DATE_SK" as "cr_date_id",
    "cr_catalog_returns"."CR_RETURNING_CUSTOMER_SK" as "cr_customer_id"
FROM
    "memory"."catalog_returns" as "cr_catalog_returns"),
wakeful as (
SELECT
    "highfalutin"."cr_call_center_id" as "cr_call_center_id",
    "highfalutin"."cr_customer_id" as "cr_customer_id",
    "highfalutin"."cr_date_id" as "cr_date_id",
    "highfalutin"."cr_net_loss" as "cr_net_loss"
FROM
    "highfalutin"
GROUP BY
    1,
    2,
    3,
    4),
quizzical as (
SELECT
    "cr_call_center_call_center"."CC_CALL_CENTER_ID" as "cr_call_center_text_id",
    "cr_call_center_call_center"."CC_CALL_CENTER_SK" as "cr_call_center_id",
    "cr_call_center_call_center"."CC_MANAGER" as "cr_call_center_manager",
    "cr_call_center_call_center"."CC_NAME" as "cr_call_center_name"
FROM
    "memory"."call_center" as "cr_call_center_call_center"),
uneven as (
SELECT
    "abundant"."cr_date_month_of_year" as "cr_date_month_of_year",
    "abundant"."cr_date_year" as "cr_date_year",
    "cheerful"."cr_customer_address_gmt_offset" as "cr_customer_address_gmt_offset",
    "cooperative"."cr_customer_demographics_education_status" as "cr_customer_demographics_education_status",
    "cooperative"."cr_customer_demographics_marital_status" as "cr_customer_demographics_marital_status",
    "questionable"."cr_customer_household_demographic_buy_potential" as "cr_customer_household_demographic_buy_potential",
    "quizzical"."cr_call_center_manager" as "cr_call_center_manager",
    "quizzical"."cr_call_center_name" as "cr_call_center_name",
    "quizzical"."cr_call_center_text_id" as "cr_call_center_text_id",
    "wakeful"."cr_net_loss" as "cr_net_loss"
FROM
    "wakeful"
    INNER JOIN "abundant" on "wakeful"."cr_date_id" = "abundant"."cr_date_id"
    INNER JOIN "thoughtful" on "wakeful"."cr_customer_id" = "thoughtful"."cr_customer_id"
    INNER JOIN "quizzical" on "wakeful"."cr_call_center_id" = "quizzical"."cr_call_center_id"
    INNER JOIN "cheerful" on "thoughtful"."cr_customer_address_id" = "cheerful"."cr_customer_address_id"
    LEFT OUTER JOIN "cooperative" on "thoughtful"."cr_customer_demographics_id" = "cooperative"."cr_customer_demographics_id"
    LEFT OUTER JOIN "questionable" on "thoughtful"."cr_customer_household_demographic_id" = "questionable"."cr_customer_household_demographic_id"
WHERE
    "abundant"."cr_date_year" = 1998 and "abundant"."cr_date_month_of_year" = 11 and ( ( "cooperative"."cr_customer_demographics_marital_status" = 'M' and "cooperative"."cr_customer_demographics_education_status" = 'Unknown' ) or ( "cooperative"."cr_customer_demographics_marital_status" = 'W' and "cooperative"."cr_customer_demographics_education_status" = 'Advanced Degree' ) ) and "questionable"."cr_customer_household_demographic_buy_potential" like 'Unknown%' and "cheerful"."cr_customer_address_gmt_offset" = -7

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10),
juicy as (
SELECT
    "uneven"."cr_call_center_manager" as "cr_call_center_manager",
    "uneven"."cr_call_center_name" as "cr_call_center_name",
    "uneven"."cr_call_center_text_id" as "cr_call_center_text_id",
    "uneven"."cr_customer_demographics_education_status" as "cr_customer_demographics_education_status",
    "uneven"."cr_customer_demographics_marital_status" as "cr_customer_demographics_marital_status",
    sum("uneven"."cr_net_loss") as "returns_loss"
FROM
    "uneven"
GROUP BY
    1,
    2,
    3,
    4,
    5),
yummy as (
SELECT
    "uneven"."cr_call_center_manager" as "cr_call_center_manager",
    "uneven"."cr_call_center_manager" as "manager",
    "uneven"."cr_call_center_name" as "call_center_name",
    "uneven"."cr_call_center_name" as "cr_call_center_name",
    "uneven"."cr_call_center_text_id" as "call_center",
    "uneven"."cr_call_center_text_id" as "cr_call_center_text_id",
    "uneven"."cr_customer_address_gmt_offset" as "cr_customer_address_gmt_offset",
    "uneven"."cr_customer_demographics_education_status" as "cr_customer_demographics_education_status",
    "uneven"."cr_customer_demographics_marital_status" as "cr_customer_demographics_marital_status",
    "uneven"."cr_customer_household_demographic_buy_potential" as "cr_customer_household_demographic_buy_potential",
    "uneven"."cr_date_month_of_year" as "cr_date_month_of_year",
    "uneven"."cr_date_year" as "cr_date_year",
    "uneven"."cr_net_loss" as "cr_net_loss"
FROM
    "uneven")
SELECT
    "yummy"."call_center" as "call_center",
    "yummy"."call_center_name" as "call_center_name",
    "yummy"."manager" as "manager",
    "juicy"."returns_loss" as "returns_loss"
FROM
    "juicy"
    FULL JOIN "yummy" on "juicy"."cr_call_center_manager" is not distinct from "yummy"."cr_call_center_manager" AND "juicy"."cr_call_center_name" = "yummy"."cr_call_center_name" AND "juicy"."cr_call_center_text_id" = "yummy"."cr_call_center_text_id" AND "juicy"."cr_customer_demographics_education_status" = "yummy"."cr_customer_demographics_education_status" AND "juicy"."cr_customer_demographics_marital_status" = "yummy"."cr_customer_demographics_marital_status"
ORDER BY 
    "juicy"."returns_loss" desc nulls first
```

## Reference SQL (zquery log)

```sql
SELECT
    "cr_call_center_call_center"."CC_CALL_CENTER_ID" as "call_center",
    "cr_call_center_call_center"."CC_NAME" as "call_center_name",
    "cr_call_center_call_center"."CC_MANAGER" as "manager",
    sum("cr_catalog_returns"."CR_NET_LOSS") as "returns_loss"
FROM
    "memory"."catalog_returns" as "cr_catalog_returns"
    INNER JOIN "memory"."date_dim" as "cr_date_date" on "cr_catalog_returns"."CR_RETURNED_DATE_SK" = "cr_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer" as "cr_customer_customers" on "cr_catalog_returns"."CR_RETURNING_CUSTOMER_SK" = "cr_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."call_center" as "cr_call_center_call_center" on "cr_catalog_returns"."CR_CALL_CENTER_SK" = "cr_call_center_call_center"."CC_CALL_CENTER_SK"
    INNER JOIN "memory"."customer_address" as "cr_customer_address_customer_address" on "cr_customer_customers"."C_CURRENT_ADDR_SK" = "cr_customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "cr_customer_demographics_customer_demographics" on "cr_customer_customers"."C_CURRENT_CDEMO_SK" = "cr_customer_demographics_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."household_demographics" as "cr_customer_household_demographic_household_demographics" on "cr_customer_customers"."C_CURRENT_HDEMO_SK" = "cr_customer_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "cr_date_date"."D_YEAR" = 1998 and "cr_date_date"."D_MOY" = 11 and ( ( "cr_customer_demographics_customer_demographics"."CD_MARITAL_STATUS" = 'M' and "cr_customer_demographics_customer_demographics"."CD_EDUCATION_STATUS" = 'Unknown' ) or ( "cr_customer_demographics_customer_demographics"."CD_MARITAL_STATUS" = 'W' and "cr_customer_demographics_customer_demographics"."CD_EDUCATION_STATUS" = 'Advanced Degree' ) ) and "cr_customer_household_demographic_household_demographics"."HD_BUY_POTENTIAL" like 'Unknown%' and "cr_customer_address_customer_address"."CA_GMT_OFFSET" = -7

GROUP BY
    1,
    2,
    3,
    "cr_customer_demographics_customer_demographics"."CD_EDUCATION_STATUS",
    "cr_customer_demographics_customer_demographics"."CD_MARITAL_STATUS"
ORDER BY 
    "returns_loss" desc nulls first
```
