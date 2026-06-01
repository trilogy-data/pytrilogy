# Query 91

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (4 rows) |
| reference execution | OK (2 rows) |
| results identical | NO |

## Result comparison

v4 rows: 4 (2 distinct)
ref rows: 2 (2 distinct)
only in v4 (showing up to 5 of 2):
  1x  ('AAAAAAAABAAAAAAA', 'NY Metro', 'Bob Belcher', Decimal('4725.36'))
  1x  ('AAAAAAAABAAAAAAA', 'NY Metro', 'Bob Belcher', Decimal('574.43'))

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2930 | 41 | 19.15 ms |
| reference | 2219 | 24 | 18.06 ms |
| v4 / ref | 1.32x | 1.71x | 1.06x |

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
    "cr_call_center_call_center"."CC_CALL_CENTER_ID" as "cr_call_center_text_id",
    "cr_call_center_call_center"."CC_MANAGER" as "cr_call_center_manager",
    "cr_call_center_call_center"."CC_NAME" as "cr_call_center_name",
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
    "cr_customer_demographics_customer_demographics"."CD_MARITAL_STATUS"),
yummy as (
SELECT
    "abundant"."cr_call_center_manager" as "manager",
    "abundant"."cr_call_center_name" as "call_center_name",
    "abundant"."cr_call_center_text_id" as "call_center"
FROM
    "abundant")
SELECT
    "yummy"."call_center" as "call_center",
    "yummy"."call_center_name" as "call_center_name",
    "yummy"."manager" as "manager",
    "abundant"."returns_loss" as "returns_loss"
FROM
    "abundant"
    FULL JOIN "yummy" on "abundant"."cr_call_center_manager" is not distinct from "yummy"."manager" AND "abundant"."cr_call_center_name" = "yummy"."call_center_name" AND "abundant"."cr_call_center_text_id" = "yummy"."call_center"
ORDER BY 
    "abundant"."returns_loss" desc nulls first
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
