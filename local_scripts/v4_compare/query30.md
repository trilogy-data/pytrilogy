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
| v4 | 7940 | 122 | 84.46 ms |
| reference | 6399 | 99 | 68.35 ms |
| v4 / ref | 1.24x | 1.23x | 1.24x |

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
    by web_returns.return_address.state, web_returns.customer.id;
auto scaled_state_returns_2002 <- 1.2 * avg(customer_state_returns_2002) by web_returns.return_address.state;

where
    customer_state_returns_2002 > scaled_state_returns_2002
    and web_returns.customer.address.state = 'GA'
    and web_returns.return_address.state is not null
select
    --web_returns.customer.id,
    web_returns.customer.text_id,
    web_returns.customer.salutation,
    web_returns.customer.first_name,
    web_returns.customer.last_name,
    web_returns.customer.preferred_cust_flag,
    web_returns.customer.birth_day,
    web_returns.customer.birth_month,
    web_returns.customer.birth_year,
    web_returns.customer.birth_country,
    web_returns.customer.login,
    web_returns.customer.email_address,
    web_returns.customer.last_review_date,
    customer_state_returns_2002,
order by
    web_returns.customer.text_id asc nulls first,
    web_returns.customer.salutation asc nulls first,
    web_returns.customer.first_name asc nulls first,
    web_returns.customer.last_name asc nulls first,
    web_returns.customer.preferred_cust_flag asc nulls first,
    web_returns.customer.birth_day asc nulls first,
    web_returns.customer.birth_month asc nulls first,
    web_returns.customer.birth_year asc nulls first,
    web_returns.customer.birth_country asc nulls first,
    web_returns.customer.login asc nulls first,
    web_returns.customer.email_address asc nulls first,
    web_returns.customer.last_review_date asc nulls first,
    customer_state_returns_2002 asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
abundant as (
SELECT
    "web_returns_return_address_customer_address"."CA_STATE" as "web_returns_return_address_state",
    "web_returns_web_returns"."WR_RETURNING_CUSTOMER_SK" as "web_returns_customer_id",
    sum(CASE WHEN "web_returns_return_date_date"."D_YEAR" = 2002 and "web_returns_return_address_customer_address"."CA_STATE" is not null THEN "web_returns_web_returns"."WR_RETURN_AMT" ELSE NULL END) as "customer_state_returns_2002"
FROM
    "memory"."web_returns" as "web_returns_web_returns"
    INNER JOIN "memory"."date_dim" as "web_returns_return_date_date" on "web_returns_web_returns"."WR_RETURNED_DATE_SK" = "web_returns_return_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "web_returns_return_address_customer_address" on "web_returns_web_returns"."WR_RETURNING_ADDR_SK" = "web_returns_return_address_customer_address"."CA_ADDRESS_SK"
GROUP BY
    1,
    2),
cooperative as (
SELECT
    "web_returns_customer_customers"."C_BIRTH_COUNTRY" as "web_returns_customer_birth_country",
    "web_returns_customer_customers"."C_BIRTH_DAY" as "web_returns_customer_birth_day",
    "web_returns_customer_customers"."C_BIRTH_MONTH" as "web_returns_customer_birth_month",
    "web_returns_customer_customers"."C_BIRTH_YEAR" as "web_returns_customer_birth_year",
    "web_returns_customer_customers"."C_CUSTOMER_ID" as "web_returns_customer_text_id",
    "web_returns_customer_customers"."C_CUSTOMER_SK" as "web_returns_customer_id",
    "web_returns_customer_customers"."C_EMAIL_ADDRESS" as "web_returns_customer_email_address",
    "web_returns_customer_customers"."C_FIRST_NAME" as "web_returns_customer_first_name",
    "web_returns_customer_customers"."C_LAST_NAME" as "web_returns_customer_last_name",
    "web_returns_customer_customers"."C_LAST_REVIEW_DATE_SK" as "web_returns_customer_last_review_date",
    "web_returns_customer_customers"."C_LOGIN" as "web_returns_customer_login",
    "web_returns_customer_customers"."C_PREFERRED_CUST_FLAG" as "web_returns_customer_preferred_cust_flag",
    "web_returns_customer_customers"."C_SALUTATION" as "web_returns_customer_salutation",
    "web_returns_return_address_customer_address"."CA_STATE" as "web_returns_return_address_state"
FROM
    "memory"."web_returns" as "web_returns_web_returns"
    INNER JOIN "memory"."customer" as "web_returns_customer_customers" on "web_returns_web_returns"."WR_RETURNING_CUSTOMER_SK" = "web_returns_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "web_returns_return_date_date" on "web_returns_web_returns"."WR_RETURNED_DATE_SK" = "web_returns_return_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "web_returns_return_address_customer_address" on "web_returns_web_returns"."WR_RETURNING_ADDR_SK" = "web_returns_return_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_address" as "web_returns_customer_address_customer_address" on "web_returns_customer_customers"."C_CURRENT_ADDR_SK" = "web_returns_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "web_returns_customer_address_customer_address"."CA_STATE" = 'GA' and "web_returns_return_address_customer_address"."CA_STATE" is not null
),
juicy as (
SELECT
    "abundant"."web_returns_return_address_state" as "web_returns_return_address_state",
    avg("abundant"."customer_state_returns_2002") as "_virt_agg_avg_3885168128306444"
FROM
    "abundant"
GROUP BY
    1),
questionable as (
SELECT
    "cooperative"."web_returns_customer_birth_country" as "web_returns_customer_birth_country",
    "cooperative"."web_returns_customer_birth_day" as "web_returns_customer_birth_day",
    "cooperative"."web_returns_customer_birth_month" as "web_returns_customer_birth_month",
    "cooperative"."web_returns_customer_birth_year" as "web_returns_customer_birth_year",
    "cooperative"."web_returns_customer_email_address" as "web_returns_customer_email_address",
    "cooperative"."web_returns_customer_first_name" as "web_returns_customer_first_name",
    "cooperative"."web_returns_customer_id" as "web_returns_customer_id",
    "cooperative"."web_returns_customer_last_name" as "web_returns_customer_last_name",
    "cooperative"."web_returns_customer_last_review_date" as "web_returns_customer_last_review_date",
    "cooperative"."web_returns_customer_login" as "web_returns_customer_login",
    "cooperative"."web_returns_customer_preferred_cust_flag" as "web_returns_customer_preferred_cust_flag",
    "cooperative"."web_returns_customer_salutation" as "web_returns_customer_salutation",
    "cooperative"."web_returns_customer_text_id" as "web_returns_customer_text_id",
    "cooperative"."web_returns_return_address_state" as "web_returns_return_address_state"
FROM
    "cooperative"),
vacuous as (
SELECT
    "juicy"."web_returns_return_address_state" as "web_returns_return_address_state",
    1.2 * "juicy"."_virt_agg_avg_3885168128306444" as "scaled_state_returns_2002"
FROM
    "juicy")
SELECT
    "abundant"."customer_state_returns_2002" as "customer_state_returns_2002",
    "questionable"."web_returns_customer_birth_country" as "web_returns_customer_birth_country",
    "questionable"."web_returns_customer_birth_day" as "web_returns_customer_birth_day",
    "questionable"."web_returns_customer_birth_month" as "web_returns_customer_birth_month",
    "questionable"."web_returns_customer_birth_year" as "web_returns_customer_birth_year",
    "questionable"."web_returns_customer_email_address" as "web_returns_customer_email_address",
    "questionable"."web_returns_customer_first_name" as "web_returns_customer_first_name",
    "questionable"."web_returns_customer_last_name" as "web_returns_customer_last_name",
    "questionable"."web_returns_customer_last_review_date" as "web_returns_customer_last_review_date",
    "questionable"."web_returns_customer_login" as "web_returns_customer_login",
    "questionable"."web_returns_customer_preferred_cust_flag" as "web_returns_customer_preferred_cust_flag",
    "questionable"."web_returns_customer_salutation" as "web_returns_customer_salutation",
    "questionable"."web_returns_customer_text_id" as "web_returns_customer_text_id"
FROM
    "questionable"
    RIGHT OUTER JOIN "abundant" on "questionable"."web_returns_customer_id" = "abundant"."web_returns_customer_id" AND "questionable"."web_returns_return_address_state" is not distinct from "abundant"."web_returns_return_address_state"
    INNER JOIN "vacuous" on "questionable"."web_returns_return_address_state" is not distinct from "vacuous"."web_returns_return_address_state"
WHERE
    "abundant"."customer_state_returns_2002" > "vacuous"."scaled_state_returns_2002"

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
    coalesce("abundant"."web_returns_customer_id","questionable"."web_returns_customer_id"),
    coalesce("abundant"."web_returns_return_address_state","questionable"."web_returns_return_address_state","vacuous"."web_returns_return_address_state")
ORDER BY 
    "questionable"."web_returns_customer_text_id" asc nulls first,
    "questionable"."web_returns_customer_salutation" asc nulls first,
    "questionable"."web_returns_customer_first_name" asc nulls first,
    "questionable"."web_returns_customer_last_name" asc nulls first,
    "questionable"."web_returns_customer_preferred_cust_flag" asc nulls first,
    "questionable"."web_returns_customer_birth_day" asc nulls first,
    "questionable"."web_returns_customer_birth_month" asc nulls first,
    "questionable"."web_returns_customer_birth_year" asc nulls first,
    "questionable"."web_returns_customer_birth_country" asc nulls first,
    "questionable"."web_returns_customer_login" asc nulls first,
    "questionable"."web_returns_customer_email_address" asc nulls first,
    "questionable"."web_returns_customer_last_review_date" asc nulls first,
    "abundant"."customer_state_returns_2002" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
questionable as (
SELECT
    "web_returns_return_address_customer_address"."CA_STATE" as "web_returns_return_address_state",
    "web_returns_web_returns"."WR_RETURNING_CUSTOMER_SK" as "web_returns_customer_id",
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
wakeful as (
SELECT
    "web_returns_customer_customers"."C_BIRTH_COUNTRY" as "web_returns_customer_birth_country",
    "web_returns_customer_customers"."C_BIRTH_DAY" as "web_returns_customer_birth_day",
    "web_returns_customer_customers"."C_BIRTH_MONTH" as "web_returns_customer_birth_month",
    "web_returns_customer_customers"."C_BIRTH_YEAR" as "web_returns_customer_birth_year",
    "web_returns_customer_customers"."C_CUSTOMER_ID" as "web_returns_customer_text_id",
    "web_returns_customer_customers"."C_CUSTOMER_SK" as "web_returns_customer_id",
    "web_returns_customer_customers"."C_EMAIL_ADDRESS" as "web_returns_customer_email_address",
    "web_returns_customer_customers"."C_FIRST_NAME" as "web_returns_customer_first_name",
    "web_returns_customer_customers"."C_LAST_NAME" as "web_returns_customer_last_name",
    "web_returns_customer_customers"."C_LAST_REVIEW_DATE_SK" as "web_returns_customer_last_review_date",
    "web_returns_customer_customers"."C_LOGIN" as "web_returns_customer_login",
    "web_returns_customer_customers"."C_PREFERRED_CUST_FLAG" as "web_returns_customer_preferred_cust_flag",
    "web_returns_customer_customers"."C_SALUTATION" as "web_returns_customer_salutation"
FROM
    "memory"."customer" as "web_returns_customer_customers"
    INNER JOIN "memory"."customer_address" as "web_returns_customer_address_customer_address" on "web_returns_customer_customers"."C_CURRENT_ADDR_SK" = "web_returns_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "web_returns_customer_address_customer_address"."CA_STATE" = 'GA'
),
juicy as (
SELECT
    "questionable"."web_returns_return_address_state" as "web_returns_return_address_state",
    avg("questionable"."customer_state_returns_2002") as "_virt_agg_avg_3885168128306444"
FROM
    "questionable"
GROUP BY
    1),
yummy as (
SELECT
    "questionable"."customer_state_returns_2002" as "customer_state_returns_2002",
    "questionable"."web_returns_return_address_state" as "web_returns_return_address_state",
    "wakeful"."web_returns_customer_birth_country" as "web_returns_customer_birth_country",
    "wakeful"."web_returns_customer_birth_day" as "web_returns_customer_birth_day",
    "wakeful"."web_returns_customer_birth_month" as "web_returns_customer_birth_month",
    "wakeful"."web_returns_customer_birth_year" as "web_returns_customer_birth_year",
    "wakeful"."web_returns_customer_email_address" as "web_returns_customer_email_address",
    "wakeful"."web_returns_customer_first_name" as "web_returns_customer_first_name",
    "wakeful"."web_returns_customer_last_name" as "web_returns_customer_last_name",
    "wakeful"."web_returns_customer_last_review_date" as "web_returns_customer_last_review_date",
    "wakeful"."web_returns_customer_login" as "web_returns_customer_login",
    "wakeful"."web_returns_customer_preferred_cust_flag" as "web_returns_customer_preferred_cust_flag",
    "wakeful"."web_returns_customer_salutation" as "web_returns_customer_salutation",
    "wakeful"."web_returns_customer_text_id" as "web_returns_customer_text_id"
FROM
    "questionable"
    INNER JOIN "wakeful" on "questionable"."web_returns_customer_id" = "wakeful"."web_returns_customer_id")
SELECT
    "yummy"."web_returns_customer_text_id" as "web_returns_customer_text_id",
    "yummy"."web_returns_customer_salutation" as "web_returns_customer_salutation",
    "yummy"."web_returns_customer_first_name" as "web_returns_customer_first_name",
    "yummy"."web_returns_customer_last_name" as "web_returns_customer_last_name",
    "yummy"."web_returns_customer_preferred_cust_flag" as "web_returns_customer_preferred_cust_flag",
    "yummy"."web_returns_customer_birth_day" as "web_returns_customer_birth_day",
    "yummy"."web_returns_customer_birth_month" as "web_returns_customer_birth_month",
    "yummy"."web_returns_customer_birth_year" as "web_returns_customer_birth_year",
    "yummy"."web_returns_customer_birth_country" as "web_returns_customer_birth_country",
    "yummy"."web_returns_customer_login" as "web_returns_customer_login",
    "yummy"."web_returns_customer_email_address" as "web_returns_customer_email_address",
    "yummy"."web_returns_customer_last_review_date" as "web_returns_customer_last_review_date",
    "yummy"."customer_state_returns_2002" as "customer_state_returns_2002"
FROM
    "yummy"
    LEFT OUTER JOIN "juicy" on "yummy"."web_returns_return_address_state" is not distinct from "juicy"."web_returns_return_address_state"
WHERE
    "yummy"."customer_state_returns_2002" > 1.2 * "juicy"."_virt_agg_avg_3885168128306444"

ORDER BY 
    "yummy"."web_returns_customer_text_id" asc nulls first,
    "yummy"."web_returns_customer_salutation" asc nulls first,
    "yummy"."web_returns_customer_first_name" asc nulls first,
    "yummy"."web_returns_customer_last_name" asc nulls first,
    "yummy"."web_returns_customer_preferred_cust_flag" asc nulls first,
    "yummy"."web_returns_customer_birth_day" asc nulls first,
    "yummy"."web_returns_customer_birth_month" asc nulls first,
    "yummy"."web_returns_customer_birth_year" asc nulls first,
    "yummy"."web_returns_customer_birth_country" asc nulls first,
    "yummy"."web_returns_customer_login" asc nulls first,
    "yummy"."web_returns_customer_email_address" asc nulls first,
    "yummy"."web_returns_customer_last_review_date" asc nulls first,
    "yummy"."customer_state_returns_2002" asc nulls first
LIMIT (100)
```
