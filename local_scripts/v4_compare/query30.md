# Query 30

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | YES |

## Result comparison

v4 rows: 100 (100 distinct)
ref rows: 100 (100 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 6767 | 104 | 75.09 ms |
| reference | 15268 | 229 | 99.67 ms |
| v4 / ref | 0.44x | 0.45x | 0.75x |

## Preql

```
import web_returns as web_returns;

# Non-rowset equivalent of query30 (rowset form): the key-join shape achieved by
# grouping the aggregate to (return_address.state, customer.id). The year/state
# filter is bound *inside* the conditional sum because a plain sum + a row-grain
# WHERE year=2002 loses the filter from the customer-grain aggregate.
auto customer_state_returns_2002 <- sum(
    web_returns.return_amount
        ? web_returns.return_date.year = 2002 and web_returns.return_address.state is not null
)
    by web_returns.return_address.state, web_returns.billing_customer.id;
auto scaled_state_returns_2002 <- 1.2 * avg(customer_state_returns_2002) by web_returns.return_address.state;

where
    customer_state_returns_2002 > scaled_state_returns_2002
    and web_returns.billing_customer.address.state = 'GA'
    and web_returns.return_address.state is not null
select
    --web_returns.billing_customer.id,
    web_returns.billing_customer.text_id,
    web_returns.billing_customer.salutation,
    web_returns.billing_customer.first_name,
    web_returns.billing_customer.last_name,
    web_returns.billing_customer.preferred_cust_flag,
    web_returns.billing_customer.birth_day,
    web_returns.billing_customer.birth_month,
    web_returns.billing_customer.birth_year,
    web_returns.billing_customer.birth_country,
    web_returns.billing_customer.login,
    web_returns.billing_customer.email_address,
    web_returns.billing_customer.last_review_date,
    customer_state_returns_2002,
order by
    web_returns.billing_customer.text_id asc nulls first,
    web_returns.billing_customer.salutation asc nulls first,
    web_returns.billing_customer.first_name asc nulls first,
    web_returns.billing_customer.last_name asc nulls first,
    web_returns.billing_customer.preferred_cust_flag asc nulls first,
    web_returns.billing_customer.birth_day asc nulls first,
    web_returns.billing_customer.birth_month asc nulls first,
    web_returns.billing_customer.birth_year asc nulls first,
    web_returns.billing_customer.birth_country asc nulls first,
    web_returns.billing_customer.login asc nulls first,
    web_returns.billing_customer.email_address asc nulls first,
    web_returns.billing_customer.last_review_date asc nulls first,
    customer_state_returns_2002 asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
uneven as (
SELECT
    "web_returns_return_address_customer_address"."CA_STATE" as "web_returns_return_address_state",
    "web_returns_web_returns"."WR_RETURNING_CUSTOMER_SK" as "web_returns_billing_customer_id",
    sum(CASE WHEN "web_returns_return_date_date"."D_YEAR" = 2002 and "web_returns_return_address_customer_address"."CA_STATE" is not null THEN "web_returns_web_returns"."WR_RETURN_AMT" ELSE NULL END) as "customer_state_returns_2002"
FROM
    "memory"."web_returns" as "web_returns_web_returns"
    INNER JOIN "memory"."date_dim" as "web_returns_return_date_date" on "web_returns_web_returns"."WR_RETURNED_DATE_SK" = "web_returns_return_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "web_returns_return_address_customer_address" on "web_returns_web_returns"."WR_RETURNING_ADDR_SK" = "web_returns_return_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "web_returns_return_address_customer_address"."CA_STATE" is not null

GROUP BY
    1,
    2),
cooperative as (
SELECT
    "web_returns_billing_customer_customers"."C_BIRTH_COUNTRY" as "web_returns_billing_customer_birth_country",
    "web_returns_billing_customer_customers"."C_BIRTH_DAY" as "web_returns_billing_customer_birth_day",
    "web_returns_billing_customer_customers"."C_BIRTH_MONTH" as "web_returns_billing_customer_birth_month",
    "web_returns_billing_customer_customers"."C_BIRTH_YEAR" as "web_returns_billing_customer_birth_year",
    "web_returns_billing_customer_customers"."C_CUSTOMER_ID" as "web_returns_billing_customer_text_id",
    "web_returns_billing_customer_customers"."C_CUSTOMER_SK" as "web_returns_billing_customer_id",
    "web_returns_billing_customer_customers"."C_EMAIL_ADDRESS" as "web_returns_billing_customer_email_address",
    "web_returns_billing_customer_customers"."C_FIRST_NAME" as "web_returns_billing_customer_first_name",
    "web_returns_billing_customer_customers"."C_LAST_NAME" as "web_returns_billing_customer_last_name",
    "web_returns_billing_customer_customers"."C_LAST_REVIEW_DATE_SK" as "web_returns_billing_customer_last_review_date",
    "web_returns_billing_customer_customers"."C_LOGIN" as "web_returns_billing_customer_login",
    "web_returns_billing_customer_customers"."C_PREFERRED_CUST_FLAG" as "web_returns_billing_customer_preferred_cust_flag",
    "web_returns_billing_customer_customers"."C_SALUTATION" as "web_returns_billing_customer_salutation"
FROM
    "memory"."web_returns" as "web_returns_web_returns"
    INNER JOIN "memory"."date_dim" as "web_returns_return_date_date" on "web_returns_web_returns"."WR_RETURNED_DATE_SK" = "web_returns_return_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "web_returns_return_address_customer_address" on "web_returns_web_returns"."WR_RETURNING_ADDR_SK" = "web_returns_return_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer" as "web_returns_billing_customer_customers" on "web_returns_web_returns"."WR_RETURNING_CUSTOMER_SK" = "web_returns_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "web_returns_billing_customer_address_customer_address" on "web_returns_billing_customer_customers"."C_CURRENT_ADDR_SK" = "web_returns_billing_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "web_returns_billing_customer_address_customer_address"."CA_STATE" = 'GA' and "web_returns_return_address_customer_address"."CA_STATE" is not null

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
    10,
    11,
    12,
    13),
concerned as (
SELECT
    "uneven"."web_returns_return_address_state" as "web_returns_return_address_state",
    avg("uneven"."customer_state_returns_2002") as "_virt_agg_avg_3885168128306444"
FROM
    "uneven"
GROUP BY
    1),
sparkling as (
SELECT
    "concerned"."web_returns_return_address_state" as "web_returns_return_address_state",
    1.2 * "concerned"."_virt_agg_avg_3885168128306444" as "scaled_state_returns_2002"
FROM
    "concerned")
SELECT
    "cooperative"."web_returns_billing_customer_text_id" as "web_returns_billing_customer_text_id",
    "cooperative"."web_returns_billing_customer_salutation" as "web_returns_billing_customer_salutation",
    "cooperative"."web_returns_billing_customer_first_name" as "web_returns_billing_customer_first_name",
    "cooperative"."web_returns_billing_customer_last_name" as "web_returns_billing_customer_last_name",
    "cooperative"."web_returns_billing_customer_preferred_cust_flag" as "web_returns_billing_customer_preferred_cust_flag",
    "cooperative"."web_returns_billing_customer_birth_day" as "web_returns_billing_customer_birth_day",
    "cooperative"."web_returns_billing_customer_birth_month" as "web_returns_billing_customer_birth_month",
    "cooperative"."web_returns_billing_customer_birth_year" as "web_returns_billing_customer_birth_year",
    "cooperative"."web_returns_billing_customer_birth_country" as "web_returns_billing_customer_birth_country",
    "cooperative"."web_returns_billing_customer_login" as "web_returns_billing_customer_login",
    "cooperative"."web_returns_billing_customer_email_address" as "web_returns_billing_customer_email_address",
    "cooperative"."web_returns_billing_customer_last_review_date" as "web_returns_billing_customer_last_review_date",
    "uneven"."customer_state_returns_2002" as "customer_state_returns_2002"
FROM
    "uneven"
    INNER JOIN "cooperative" on "uneven"."web_returns_billing_customer_id" = "cooperative"."web_returns_billing_customer_id"
    INNER JOIN "sparkling" on "uneven"."web_returns_return_address_state" is not distinct from "sparkling"."web_returns_return_address_state"
WHERE
    "uneven"."customer_state_returns_2002" > "sparkling"."scaled_state_returns_2002"

ORDER BY 
    "cooperative"."web_returns_billing_customer_text_id" asc nulls first,
    "cooperative"."web_returns_billing_customer_salutation" asc nulls first,
    "cooperative"."web_returns_billing_customer_first_name" asc nulls first,
    "cooperative"."web_returns_billing_customer_last_name" asc nulls first,
    "cooperative"."web_returns_billing_customer_preferred_cust_flag" asc nulls first,
    "cooperative"."web_returns_billing_customer_birth_day" asc nulls first,
    "cooperative"."web_returns_billing_customer_birth_month" asc nulls first,
    "cooperative"."web_returns_billing_customer_birth_year" asc nulls first,
    "cooperative"."web_returns_billing_customer_birth_country" asc nulls first,
    "cooperative"."web_returns_billing_customer_login" asc nulls first,
    "cooperative"."web_returns_billing_customer_email_address" asc nulls first,
    "cooperative"."web_returns_billing_customer_last_review_date" asc nulls first,
    "uneven"."customer_state_returns_2002" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cooperative as (
SELECT
    "web_returns_billing_customer_customers"."C_BIRTH_COUNTRY" as "web_returns_billing_customer_birth_country",
    "web_returns_billing_customer_customers"."C_BIRTH_DAY" as "web_returns_billing_customer_birth_day",
    "web_returns_billing_customer_customers"."C_BIRTH_MONTH" as "web_returns_billing_customer_birth_month",
    "web_returns_billing_customer_customers"."C_BIRTH_YEAR" as "web_returns_billing_customer_birth_year",
    "web_returns_billing_customer_customers"."C_CUSTOMER_ID" as "web_returns_billing_customer_text_id",
    "web_returns_billing_customer_customers"."C_CUSTOMER_SK" as "web_returns_billing_customer_id",
    "web_returns_billing_customer_customers"."C_EMAIL_ADDRESS" as "web_returns_billing_customer_email_address",
    "web_returns_billing_customer_customers"."C_FIRST_NAME" as "web_returns_billing_customer_first_name",
    "web_returns_billing_customer_customers"."C_LAST_NAME" as "web_returns_billing_customer_last_name",
    "web_returns_billing_customer_customers"."C_LAST_REVIEW_DATE_SK" as "web_returns_billing_customer_last_review_date",
    "web_returns_billing_customer_customers"."C_LOGIN" as "web_returns_billing_customer_login",
    "web_returns_billing_customer_customers"."C_PREFERRED_CUST_FLAG" as "web_returns_billing_customer_preferred_cust_flag",
    "web_returns_billing_customer_customers"."C_SALUTATION" as "web_returns_billing_customer_salutation"
FROM
    "memory"."web_returns" as "web_returns_web_returns"
    INNER JOIN "memory"."date_dim" as "web_returns_return_date_date" on "web_returns_web_returns"."WR_RETURNED_DATE_SK" = "web_returns_return_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "web_returns_return_address_customer_address" on "web_returns_web_returns"."WR_RETURNING_ADDR_SK" = "web_returns_return_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer" as "web_returns_billing_customer_customers" on "web_returns_web_returns"."WR_RETURNING_CUSTOMER_SK" = "web_returns_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "web_returns_billing_customer_address_customer_address" on "web_returns_billing_customer_customers"."C_CURRENT_ADDR_SK" = "web_returns_billing_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "web_returns_billing_customer_address_customer_address"."CA_STATE" = 'GA' and "web_returns_return_address_customer_address"."CA_STATE" is not null
),
uneven as (
SELECT
    "web_returns_return_address_customer_address"."CA_STATE" as "web_returns_return_address_state",
    "web_returns_web_returns"."WR_RETURNING_CUSTOMER_SK" as "web_returns_billing_customer_id",
    sum(CASE WHEN "web_returns_return_date_date"."D_YEAR" = 2002 and "web_returns_return_address_customer_address"."CA_STATE" is not null THEN "web_returns_web_returns"."WR_RETURN_AMT" ELSE NULL END) as "customer_state_returns_2002"
FROM
    "memory"."web_returns" as "web_returns_web_returns"
    INNER JOIN "memory"."date_dim" as "web_returns_return_date_date" on "web_returns_web_returns"."WR_RETURNED_DATE_SK" = "web_returns_return_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "web_returns_return_address_customer_address" on "web_returns_web_returns"."WR_RETURNING_ADDR_SK" = "web_returns_return_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "web_returns_return_address_customer_address"."CA_STATE" is not null

GROUP BY
    1,
    2),
scrawny as (
SELECT
    "cooperative"."web_returns_billing_customer_id" as "web_returns_billing_customer_id",
    "cooperative"."web_returns_billing_customer_text_id" as "web_returns_billing_customer_text_id"
FROM
    "cooperative"
GROUP BY
    1,
    2,
    "cooperative"."web_returns_billing_customer_birth_country",
    "cooperative"."web_returns_billing_customer_birth_day",
    "cooperative"."web_returns_billing_customer_birth_month",
    "cooperative"."web_returns_billing_customer_birth_year",
    "cooperative"."web_returns_billing_customer_email_address",
    "cooperative"."web_returns_billing_customer_first_name",
    "cooperative"."web_returns_billing_customer_last_name",
    "cooperative"."web_returns_billing_customer_last_review_date",
    "cooperative"."web_returns_billing_customer_login",
    "cooperative"."web_returns_billing_customer_preferred_cust_flag",
    "cooperative"."web_returns_billing_customer_salutation"),
questionable as (
SELECT
    "cooperative"."web_returns_billing_customer_birth_country" as "web_returns_billing_customer_birth_country",
    "cooperative"."web_returns_billing_customer_birth_day" as "web_returns_billing_customer_birth_day",
    "cooperative"."web_returns_billing_customer_birth_month" as "web_returns_billing_customer_birth_month",
    "cooperative"."web_returns_billing_customer_birth_year" as "web_returns_billing_customer_birth_year",
    "cooperative"."web_returns_billing_customer_email_address" as "web_returns_billing_customer_email_address",
    "cooperative"."web_returns_billing_customer_first_name" as "web_returns_billing_customer_first_name",
    "cooperative"."web_returns_billing_customer_id" as "web_returns_billing_customer_id",
    "cooperative"."web_returns_billing_customer_last_name" as "web_returns_billing_customer_last_name",
    "cooperative"."web_returns_billing_customer_last_review_date" as "web_returns_billing_customer_last_review_date",
    "cooperative"."web_returns_billing_customer_login" as "web_returns_billing_customer_login",
    "cooperative"."web_returns_billing_customer_preferred_cust_flag" as "web_returns_billing_customer_preferred_cust_flag",
    "cooperative"."web_returns_billing_customer_salutation" as "web_returns_billing_customer_salutation",
    "cooperative"."web_returns_billing_customer_text_id" as "web_returns_billing_customer_text_id"
FROM
    "cooperative"
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
    10,
    11,
    12,
    13),
concerned as (
SELECT
    "uneven"."web_returns_return_address_state" as "web_returns_return_address_state",
    avg("uneven"."customer_state_returns_2002") as "_virt_agg_avg_3885168128306444"
FROM
    "uneven"
GROUP BY
    1),
sparkling as (
SELECT
    "concerned"."web_returns_return_address_state" as "web_returns_return_address_state",
    1.2 * "concerned"."_virt_agg_avg_3885168128306444" as "scaled_state_returns_2002"
FROM
    "concerned"),
abhorrent as (
SELECT
    "questionable"."web_returns_billing_customer_birth_country" as "web_returns_billing_customer_birth_country",
    "questionable"."web_returns_billing_customer_birth_day" as "web_returns_billing_customer_birth_day",
    "questionable"."web_returns_billing_customer_birth_month" as "web_returns_billing_customer_birth_month",
    "questionable"."web_returns_billing_customer_birth_year" as "web_returns_billing_customer_birth_year",
    "questionable"."web_returns_billing_customer_email_address" as "web_returns_billing_customer_email_address",
    "questionable"."web_returns_billing_customer_first_name" as "web_returns_billing_customer_first_name",
    "questionable"."web_returns_billing_customer_id" as "web_returns_billing_customer_id",
    "questionable"."web_returns_billing_customer_last_name" as "web_returns_billing_customer_last_name",
    "questionable"."web_returns_billing_customer_last_review_date" as "web_returns_billing_customer_last_review_date",
    "questionable"."web_returns_billing_customer_login" as "web_returns_billing_customer_login",
    "questionable"."web_returns_billing_customer_preferred_cust_flag" as "web_returns_billing_customer_preferred_cust_flag",
    "questionable"."web_returns_billing_customer_salutation" as "web_returns_billing_customer_salutation",
    "questionable"."web_returns_billing_customer_text_id" as "web_returns_billing_customer_text_id",
    "uneven"."customer_state_returns_2002" as "customer_state_returns_2002"
FROM
    "uneven"
    INNER JOIN "questionable" on "uneven"."web_returns_billing_customer_id" = "questionable"."web_returns_billing_customer_id"
    INNER JOIN "sparkling" on "uneven"."web_returns_return_address_state" is not distinct from "sparkling"."web_returns_return_address_state"
WHERE
    "uneven"."customer_state_returns_2002" > "sparkling"."scaled_state_returns_2002"
),
sweltering as (
SELECT
    "abhorrent"."customer_state_returns_2002" as "customer_state_returns_2002",
    "abhorrent"."web_returns_billing_customer_birth_country" as "web_returns_billing_customer_birth_country",
    "abhorrent"."web_returns_billing_customer_birth_day" as "web_returns_billing_customer_birth_day",
    "abhorrent"."web_returns_billing_customer_birth_month" as "web_returns_billing_customer_birth_month",
    "abhorrent"."web_returns_billing_customer_birth_year" as "web_returns_billing_customer_birth_year",
    "abhorrent"."web_returns_billing_customer_email_address" as "web_returns_billing_customer_email_address",
    "abhorrent"."web_returns_billing_customer_first_name" as "web_returns_billing_customer_first_name",
    "abhorrent"."web_returns_billing_customer_id" as "web_returns_billing_customer_id",
    "abhorrent"."web_returns_billing_customer_last_name" as "web_returns_billing_customer_last_name",
    "abhorrent"."web_returns_billing_customer_last_review_date" as "web_returns_billing_customer_last_review_date",
    "abhorrent"."web_returns_billing_customer_login" as "web_returns_billing_customer_login",
    "abhorrent"."web_returns_billing_customer_preferred_cust_flag" as "web_returns_billing_customer_preferred_cust_flag",
    "abhorrent"."web_returns_billing_customer_salutation" as "web_returns_billing_customer_salutation",
    "abhorrent"."web_returns_billing_customer_text_id" as "web_returns_billing_customer_text_id"
FROM
    "abhorrent"),
late as (
SELECT
    "sweltering"."customer_state_returns_2002" as "customer_state_returns_2002",
    "sweltering"."web_returns_billing_customer_birth_country" as "web_returns_billing_customer_birth_country",
    "sweltering"."web_returns_billing_customer_birth_day" as "web_returns_billing_customer_birth_day",
    "sweltering"."web_returns_billing_customer_birth_month" as "web_returns_billing_customer_birth_month",
    "sweltering"."web_returns_billing_customer_birth_year" as "web_returns_billing_customer_birth_year",
    "sweltering"."web_returns_billing_customer_email_address" as "web_returns_billing_customer_email_address",
    "sweltering"."web_returns_billing_customer_first_name" as "web_returns_billing_customer_first_name",
    "sweltering"."web_returns_billing_customer_id" as "web_returns_billing_customer_id",
    "sweltering"."web_returns_billing_customer_last_name" as "web_returns_billing_customer_last_name",
    "sweltering"."web_returns_billing_customer_last_review_date" as "web_returns_billing_customer_last_review_date",
    "sweltering"."web_returns_billing_customer_login" as "web_returns_billing_customer_login",
    "sweltering"."web_returns_billing_customer_preferred_cust_flag" as "web_returns_billing_customer_preferred_cust_flag",
    "sweltering"."web_returns_billing_customer_salutation" as "web_returns_billing_customer_salutation",
    "sweltering"."web_returns_billing_customer_text_id" as "web_returns_billing_customer_text_id"
FROM
    "sweltering"),
macho as (
SELECT
    "late"."customer_state_returns_2002" as "customer_state_returns_2002",
    "late"."web_returns_billing_customer_birth_country" as "web_returns_billing_customer_birth_country",
    "late"."web_returns_billing_customer_birth_day" as "web_returns_billing_customer_birth_day",
    "late"."web_returns_billing_customer_birth_month" as "web_returns_billing_customer_birth_month",
    "late"."web_returns_billing_customer_birth_year" as "web_returns_billing_customer_birth_year",
    "late"."web_returns_billing_customer_email_address" as "web_returns_billing_customer_email_address",
    "late"."web_returns_billing_customer_first_name" as "web_returns_billing_customer_first_name",
    "late"."web_returns_billing_customer_id" as "web_returns_billing_customer_id",
    "late"."web_returns_billing_customer_last_name" as "web_returns_billing_customer_last_name",
    "late"."web_returns_billing_customer_last_review_date" as "web_returns_billing_customer_last_review_date",
    "late"."web_returns_billing_customer_login" as "web_returns_billing_customer_login",
    "late"."web_returns_billing_customer_preferred_cust_flag" as "web_returns_billing_customer_preferred_cust_flag",
    "late"."web_returns_billing_customer_salutation" as "web_returns_billing_customer_salutation",
    "late"."web_returns_billing_customer_text_id" as "web_returns_billing_customer_text_id"
FROM
    "late")
SELECT
    "macho"."web_returns_billing_customer_text_id" as "web_returns_billing_customer_text_id",
    "macho"."web_returns_billing_customer_salutation" as "web_returns_billing_customer_salutation",
    "macho"."web_returns_billing_customer_first_name" as "web_returns_billing_customer_first_name",
    "macho"."web_returns_billing_customer_last_name" as "web_returns_billing_customer_last_name",
    "macho"."web_returns_billing_customer_preferred_cust_flag" as "web_returns_billing_customer_preferred_cust_flag",
    "macho"."web_returns_billing_customer_birth_day" as "web_returns_billing_customer_birth_day",
    "macho"."web_returns_billing_customer_birth_month" as "web_returns_billing_customer_birth_month",
    "macho"."web_returns_billing_customer_birth_year" as "web_returns_billing_customer_birth_year",
    "macho"."web_returns_billing_customer_birth_country" as "web_returns_billing_customer_birth_country",
    "macho"."web_returns_billing_customer_login" as "web_returns_billing_customer_login",
    "macho"."web_returns_billing_customer_email_address" as "web_returns_billing_customer_email_address",
    "macho"."web_returns_billing_customer_last_review_date" as "web_returns_billing_customer_last_review_date",
    "macho"."customer_state_returns_2002" as "customer_state_returns_2002"
FROM
    "macho"
    INNER JOIN "scrawny" on "macho"."web_returns_billing_customer_id" = "scrawny"."web_returns_billing_customer_id" AND "macho"."web_returns_billing_customer_text_id" = "scrawny"."web_returns_billing_customer_text_id"
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
    10,
    11,
    12,
    13,
    "macho"."web_returns_billing_customer_id"
ORDER BY 
    "macho"."web_returns_billing_customer_text_id" asc nulls first,
    "macho"."web_returns_billing_customer_salutation" asc nulls first,
    "macho"."web_returns_billing_customer_first_name" asc nulls first,
    "macho"."web_returns_billing_customer_last_name" asc nulls first,
    "macho"."web_returns_billing_customer_preferred_cust_flag" asc nulls first,
    "macho"."web_returns_billing_customer_birth_day" asc nulls first,
    "macho"."web_returns_billing_customer_birth_month" asc nulls first,
    "macho"."web_returns_billing_customer_birth_year" asc nulls first,
    "macho"."web_returns_billing_customer_birth_country" asc nulls first,
    "macho"."web_returns_billing_customer_login" asc nulls first,
    "macho"."web_returns_billing_customer_email_address" asc nulls first,
    "macho"."web_returns_billing_customer_last_review_date" asc nulls first,
    "macho"."customer_state_returns_2002" asc nulls first
LIMIT (100)
```
