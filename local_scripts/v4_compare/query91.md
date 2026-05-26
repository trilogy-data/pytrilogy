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

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 4181 | 58 | 26.22 ms |
| reference | 2219 | 24 | 26.82 ms |
| v4 / ref | 1.88x | 2.42x | 0.98x |

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
    "cr_catalog_returns"."CR_NET_LOSS" as "cr_net_loss",
    "cr_customer_demographics_customer_demographics"."CD_EDUCATION_STATUS" as "cr_customer_demographics_education_status",
    "cr_customer_demographics_customer_demographics"."CD_MARITAL_STATUS" as "cr_customer_demographics_marital_status"
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
),
yummy as (
SELECT
    "abundant"."cr_call_center_manager" as "cr_call_center_manager",
    "abundant"."cr_call_center_name" as "cr_call_center_name",
    "abundant"."cr_call_center_text_id" as "cr_call_center_text_id",
    "abundant"."cr_customer_demographics_education_status" as "cr_customer_demographics_education_status",
    "abundant"."cr_customer_demographics_marital_status" as "cr_customer_demographics_marital_status",
    sum("abundant"."cr_net_loss") as "returns_loss"
FROM
    "abundant"
GROUP BY
    1,
    2,
    3,
    4,
    5),
uneven as (
SELECT
    "abundant"."cr_call_center_manager" as "cr_call_center_manager",
    "abundant"."cr_call_center_manager" as "manager",
    "abundant"."cr_call_center_name" as "call_center_name",
    "abundant"."cr_call_center_name" as "cr_call_center_name",
    "abundant"."cr_call_center_text_id" as "call_center",
    "abundant"."cr_call_center_text_id" as "cr_call_center_text_id",
    "abundant"."cr_customer_demographics_education_status" as "cr_customer_demographics_education_status",
    "abundant"."cr_customer_demographics_marital_status" as "cr_customer_demographics_marital_status"
FROM
    "abundant")
SELECT
    "uneven"."call_center" as "call_center",
    "uneven"."call_center_name" as "call_center_name",
    "uneven"."manager" as "manager",
    "yummy"."returns_loss" as "returns_loss"
FROM
    "yummy"
    FULL JOIN "uneven" on "yummy"."cr_call_center_manager" is not distinct from "uneven"."cr_call_center_manager" AND "yummy"."cr_call_center_name" = "uneven"."cr_call_center_name" AND "yummy"."cr_call_center_text_id" = "uneven"."cr_call_center_text_id" AND "yummy"."cr_customer_demographics_education_status" = "uneven"."cr_customer_demographics_education_status" AND "yummy"."cr_customer_demographics_marital_status" = "uneven"."cr_customer_demographics_marital_status"
ORDER BY 
    "yummy"."returns_loss" desc nulls first
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
