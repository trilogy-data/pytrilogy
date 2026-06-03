# Query 91

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (2 rows) |
| reference execution | OK (2 rows) |
| results identical | YES |

## Result comparison

v4 rows: 2 (2 distinct)
ref rows: 2 (2 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2801 | 35 | 37.33 ms |
| reference | 2371 | 24 | 41.74 ms |
| v4 / ref | 1.18x | 1.46x | 0.89x |

## Preql

```
import catalog_returns as cr;

where
    cr.date.year = 1998
    and cr.date.month_of_year = 11
    and (
        (
            cr.billing_customer.demographics.marital_status = 'M'
            and cr.billing_customer.demographics.education_status = 'Unknown'
        )
        or (
            cr.billing_customer.demographics.marital_status = 'W'
            and cr.billing_customer.demographics.education_status = 'Advanced Degree'
        )
    )
    and cr.billing_customer.household_demographic.buy_potential like 'Unknown%'
    and cr.billing_customer.address.gmt_offset = -7
select
    cr.call_center.text_id as call_center,
    cr.call_center.name as call_center_name,
    cr.call_center.manager as manager,
    sum(cr.net_loss)
            by cr.call_center.text_id, cr.call_center.name, cr.call_center.manager, cr.billing_customer.demographics.marital_status, cr.billing_customer.demographics.education_status as returns_loss,
order by
    returns_loss desc nulls first
;
```

## v4 generated SQL

```sql
WITH 
abundant as (
SELECT
    "cr_billing_customer_demographics_customer_demographics"."CD_EDUCATION_STATUS" as "cr_billing_customer_demographics_education_status",
    "cr_billing_customer_demographics_customer_demographics"."CD_MARITAL_STATUS" as "cr_billing_customer_demographics_marital_status",
    "cr_call_center_call_center"."CC_CALL_CENTER_ID" as "cr_call_center_text_id",
    "cr_call_center_call_center"."CC_MANAGER" as "cr_call_center_manager",
    "cr_call_center_call_center"."CC_NAME" as "cr_call_center_name",
    sum("cr_catalog_returns"."CR_NET_LOSS") as "returns_loss"
FROM
    "memory"."catalog_returns" as "cr_catalog_returns"
    INNER JOIN "memory"."date_dim" as "cr_date_date" on "cr_catalog_returns"."CR_RETURNED_DATE_SK" = "cr_date_date"."D_DATE_SK"
    INNER JOIN "memory"."call_center" as "cr_call_center_call_center" on "cr_catalog_returns"."CR_CALL_CENTER_SK" = "cr_call_center_call_center"."CC_CALL_CENTER_SK"
    INNER JOIN "memory"."customer" as "cr_billing_customer_customers" on "cr_catalog_returns"."CR_RETURNING_CUSTOMER_SK" = "cr_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "cr_billing_customer_address_customer_address" on "cr_billing_customer_customers"."C_CURRENT_ADDR_SK" = "cr_billing_customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "cr_billing_customer_demographics_customer_demographics" on "cr_billing_customer_customers"."C_CURRENT_CDEMO_SK" = "cr_billing_customer_demographics_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."household_demographics" as "cr_billing_customer_household_demographic_household_demographics" on "cr_billing_customer_customers"."C_CURRENT_HDEMO_SK" = "cr_billing_customer_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "cr_date_date"."D_YEAR" = 1998 and "cr_date_date"."D_MOY" = 11 and ( ( "cr_billing_customer_demographics_customer_demographics"."CD_MARITAL_STATUS" = 'M' and "cr_billing_customer_demographics_customer_demographics"."CD_EDUCATION_STATUS" = 'Unknown' ) or ( "cr_billing_customer_demographics_customer_demographics"."CD_MARITAL_STATUS" = 'W' and "cr_billing_customer_demographics_customer_demographics"."CD_EDUCATION_STATUS" = 'Advanced Degree' ) ) and "cr_billing_customer_household_demographic_household_demographics"."HD_BUY_POTENTIAL" like 'Unknown%' and "cr_billing_customer_address_customer_address"."CA_GMT_OFFSET" = -7

GROUP BY
    1,
    2,
    3,
    4,
    5)
SELECT
    "abundant"."cr_call_center_text_id" as "call_center",
    "abundant"."cr_call_center_name" as "call_center_name",
    "abundant"."cr_call_center_manager" as "manager",
    "abundant"."returns_loss" as "returns_loss"
FROM
    "abundant"
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
    INNER JOIN "memory"."call_center" as "cr_call_center_call_center" on "cr_catalog_returns"."CR_CALL_CENTER_SK" = "cr_call_center_call_center"."CC_CALL_CENTER_SK"
    INNER JOIN "memory"."customer" as "cr_billing_customer_customers" on "cr_catalog_returns"."CR_RETURNING_CUSTOMER_SK" = "cr_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "cr_billing_customer_address_customer_address" on "cr_billing_customer_customers"."C_CURRENT_ADDR_SK" = "cr_billing_customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "cr_billing_customer_demographics_customer_demographics" on "cr_billing_customer_customers"."C_CURRENT_CDEMO_SK" = "cr_billing_customer_demographics_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."household_demographics" as "cr_billing_customer_household_demographic_household_demographics" on "cr_billing_customer_customers"."C_CURRENT_HDEMO_SK" = "cr_billing_customer_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "cr_date_date"."D_YEAR" = 1998 and "cr_date_date"."D_MOY" = 11 and ( ( "cr_billing_customer_demographics_customer_demographics"."CD_MARITAL_STATUS" = 'M' and "cr_billing_customer_demographics_customer_demographics"."CD_EDUCATION_STATUS" = 'Unknown' ) or ( "cr_billing_customer_demographics_customer_demographics"."CD_MARITAL_STATUS" = 'W' and "cr_billing_customer_demographics_customer_demographics"."CD_EDUCATION_STATUS" = 'Advanced Degree' ) ) and "cr_billing_customer_household_demographic_household_demographics"."HD_BUY_POTENTIAL" like 'Unknown%' and "cr_billing_customer_address_customer_address"."CA_GMT_OFFSET" = -7

GROUP BY
    1,
    2,
    3,
    "cr_billing_customer_demographics_customer_demographics"."CD_EDUCATION_STATUS",
    "cr_billing_customer_demographics_customer_demographics"."CD_MARITAL_STATUS"
ORDER BY 
    "returns_loss" desc nulls first
```
