# Query 68

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
| v4 | 4731 | 80 | 35.67 ms |
| reference | 4731 | 80 | 35.09 ms |
| v4 / ref | 1.00x | 1.00x | 1.02x |

## Preql

```
import physical_sales as physical_sales;
import customer as customer;

rowset dn <- where
    physical_sales.date.day_of_month between 1 and 2
    and (
        physical_sales.household_demographic.dependent_count = 4
        or physical_sales.household_demographic.vehicle_count = 3
    )
    and physical_sales.date.year in (1999, 2000, 2001)
    and physical_sales.store.city in ('Fairview', 'Midway')
    and physical_sales.billing_customer.id is not null
select
    physical_sales.ticket_number,
    physical_sales.billing_customer.id,
    physical_sales.sale_address.city,
    sum(physical_sales.ext_sales_price) as extended_price,
    sum(physical_sales.ext_tax) as extended_tax,
    sum(physical_sales.ext_list_price) as list_price,
merge
select
    customer.id,
    customer.last_name,
    customer.first_name,
    customer.address.city,
align
    customer_id: physical_sales.billing_customer.id, customer.id
where
customer.address.city != physical_sales.sale_address.city
;

select
    dn.customer.last_name,
    dn.customer.first_name,
    dn.customer.address.city,
    dn.physical_sales.sale_address.city,
    dn.physical_sales.ticket_number,
    dn.extended_price,
    dn.extended_tax,
    dn.list_price,
order by
    dn.customer.last_name asc nulls first,
    dn.physical_sales.ticket_number asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
wakeful as (
SELECT
    "customer_address_customer_address"."CA_CITY" as "customer_address_city",
    "customer_customers"."C_CUSTOMER_SK" as "customer_id",
    "customer_customers"."C_CUSTOMER_SK" as "customer_id",
    "customer_customers"."C_FIRST_NAME" as "customer_first_name",
    "customer_customers"."C_LAST_NAME" as "customer_last_name"
FROM
    "memory"."customer" as "customer_customers"
    INNER JOIN "memory"."customer_address" as "customer_address_customer_address" on "customer_customers"."C_CURRENT_ADDR_SK" = "customer_address_customer_address"."CA_ADDRESS_SK"),
uneven as (
SELECT
    "physical_sales_sale_address_customer_address"."CA_CITY" as "physical_sales_sale_address_city",
    "physical_sales_store_sales"."SS_CUSTOMER_SK" as "customer_id",
    "physical_sales_store_sales"."SS_CUSTOMER_SK" as "physical_sales_billing_customer_id",
    "physical_sales_store_sales"."SS_TICKET_NUMBER" as "physical_sales_ticket_number",
    sum("physical_sales_store_sales"."SS_EXT_LIST_PRICE") as "_dn_list_price",
    sum("physical_sales_store_sales"."SS_EXT_SALES_PRICE") as "_dn_extended_price",
    sum("physical_sales_store_sales"."SS_EXT_TAX") as "_dn_extended_tax"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    LEFT OUTER JOIN "memory"."customer_address" as "physical_sales_sale_address_customer_address" on "physical_sales_store_sales"."SS_ADDR_SK" = "physical_sales_sale_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."household_demographics" as "physical_sales_household_demographic_household_demographics" on "physical_sales_store_sales"."SS_HDEMO_SK" = "physical_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "physical_sales_date_date"."D_DOM" BETWEEN 1 AND 2 and ( "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 4 or "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" = 3 ) and "physical_sales_date_date"."D_YEAR" in (1999,2000,2001) and "physical_sales_store_store"."S_CITY" in ('Fairview','Midway') and "physical_sales_store_sales"."SS_CUSTOMER_SK" is not null

GROUP BY
    1,
    3,
    4),
vacuous as (
SELECT
    "uneven"."_dn_extended_price" as "_dn_extended_price",
    "uneven"."_dn_extended_tax" as "_dn_extended_tax",
    "uneven"."_dn_list_price" as "_dn_list_price",
    "uneven"."physical_sales_billing_customer_id" as "physical_sales_billing_customer_id",
    "uneven"."physical_sales_sale_address_city" as "physical_sales_sale_address_city",
    "uneven"."physical_sales_ticket_number" as "physical_sales_ticket_number",
    "wakeful"."customer_address_city" as "customer_address_city",
    "wakeful"."customer_first_name" as "customer_first_name",
    "wakeful"."customer_id" as "customer_id",
    "wakeful"."customer_last_name" as "customer_last_name"
FROM
    "uneven"
    INNER JOIN "wakeful" on "uneven"."customer_id" = "wakeful"."customer_id"
WHERE
    "wakeful"."customer_address_city" != "uneven"."physical_sales_sale_address_city"
),
concerned as (
SELECT
    "vacuous"."_dn_extended_price" as "_dn_extended_price",
    "vacuous"."_dn_extended_tax" as "_dn_extended_tax",
    "vacuous"."_dn_list_price" as "_dn_list_price",
    "vacuous"."customer_address_city" as "customer_address_city",
    "vacuous"."customer_first_name" as "customer_first_name",
    "vacuous"."customer_id" as "customer_id",
    "vacuous"."customer_last_name" as "customer_last_name",
    "vacuous"."physical_sales_billing_customer_id" as "physical_sales_billing_customer_id",
    "vacuous"."physical_sales_sale_address_city" as "physical_sales_sale_address_city",
    "vacuous"."physical_sales_ticket_number" as "physical_sales_ticket_number"
FROM
    "vacuous")
SELECT
    "concerned"."customer_last_name" as "dn_customer_last_name",
    "concerned"."customer_first_name" as "dn_customer_first_name",
    "concerned"."customer_address_city" as "dn_customer_address_city",
    "concerned"."physical_sales_sale_address_city" as "dn_physical_sales_sale_address_city",
    "concerned"."physical_sales_ticket_number" as "dn_physical_sales_ticket_number",
    "concerned"."_dn_extended_price" as "dn_extended_price",
    "concerned"."_dn_extended_tax" as "dn_extended_tax",
    "concerned"."_dn_list_price" as "dn_list_price"
FROM
    "concerned"
ORDER BY 
    "dn_customer_last_name" asc nulls first,
    "dn_physical_sales_ticket_number" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
wakeful as (
SELECT
    "customer_address_customer_address"."CA_CITY" as "customer_address_city",
    "customer_customers"."C_CUSTOMER_SK" as "customer_id",
    "customer_customers"."C_CUSTOMER_SK" as "customer_id",
    "customer_customers"."C_FIRST_NAME" as "customer_first_name",
    "customer_customers"."C_LAST_NAME" as "customer_last_name"
FROM
    "memory"."customer" as "customer_customers"
    INNER JOIN "memory"."customer_address" as "customer_address_customer_address" on "customer_customers"."C_CURRENT_ADDR_SK" = "customer_address_customer_address"."CA_ADDRESS_SK"),
uneven as (
SELECT
    "physical_sales_sale_address_customer_address"."CA_CITY" as "physical_sales_sale_address_city",
    "physical_sales_store_sales"."SS_CUSTOMER_SK" as "customer_id",
    "physical_sales_store_sales"."SS_CUSTOMER_SK" as "physical_sales_billing_customer_id",
    "physical_sales_store_sales"."SS_TICKET_NUMBER" as "physical_sales_ticket_number",
    sum("physical_sales_store_sales"."SS_EXT_LIST_PRICE") as "_dn_list_price",
    sum("physical_sales_store_sales"."SS_EXT_SALES_PRICE") as "_dn_extended_price",
    sum("physical_sales_store_sales"."SS_EXT_TAX") as "_dn_extended_tax"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    LEFT OUTER JOIN "memory"."customer_address" as "physical_sales_sale_address_customer_address" on "physical_sales_store_sales"."SS_ADDR_SK" = "physical_sales_sale_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."household_demographics" as "physical_sales_household_demographic_household_demographics" on "physical_sales_store_sales"."SS_HDEMO_SK" = "physical_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "physical_sales_date_date"."D_DOM" BETWEEN 1 AND 2 and ( "physical_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 4 or "physical_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" = 3 ) and "physical_sales_date_date"."D_YEAR" in (1999,2000,2001) and "physical_sales_store_store"."S_CITY" in ('Fairview','Midway') and "physical_sales_store_sales"."SS_CUSTOMER_SK" is not null

GROUP BY
    1,
    3,
    4),
vacuous as (
SELECT
    "uneven"."_dn_extended_price" as "_dn_extended_price",
    "uneven"."_dn_extended_tax" as "_dn_extended_tax",
    "uneven"."_dn_list_price" as "_dn_list_price",
    "uneven"."physical_sales_billing_customer_id" as "physical_sales_billing_customer_id",
    "uneven"."physical_sales_sale_address_city" as "physical_sales_sale_address_city",
    "uneven"."physical_sales_ticket_number" as "physical_sales_ticket_number",
    "wakeful"."customer_address_city" as "customer_address_city",
    "wakeful"."customer_first_name" as "customer_first_name",
    "wakeful"."customer_id" as "customer_id",
    "wakeful"."customer_last_name" as "customer_last_name"
FROM
    "uneven"
    INNER JOIN "wakeful" on "uneven"."customer_id" = "wakeful"."customer_id"
WHERE
    "wakeful"."customer_address_city" != "uneven"."physical_sales_sale_address_city"
),
concerned as (
SELECT
    "vacuous"."_dn_extended_price" as "_dn_extended_price",
    "vacuous"."_dn_extended_tax" as "_dn_extended_tax",
    "vacuous"."_dn_list_price" as "_dn_list_price",
    "vacuous"."customer_address_city" as "customer_address_city",
    "vacuous"."customer_first_name" as "customer_first_name",
    "vacuous"."customer_id" as "customer_id",
    "vacuous"."customer_last_name" as "customer_last_name",
    "vacuous"."physical_sales_billing_customer_id" as "physical_sales_billing_customer_id",
    "vacuous"."physical_sales_sale_address_city" as "physical_sales_sale_address_city",
    "vacuous"."physical_sales_ticket_number" as "physical_sales_ticket_number"
FROM
    "vacuous")
SELECT
    "concerned"."customer_last_name" as "dn_customer_last_name",
    "concerned"."customer_first_name" as "dn_customer_first_name",
    "concerned"."customer_address_city" as "dn_customer_address_city",
    "concerned"."physical_sales_sale_address_city" as "dn_physical_sales_sale_address_city",
    "concerned"."physical_sales_ticket_number" as "dn_physical_sales_ticket_number",
    "concerned"."_dn_extended_price" as "dn_extended_price",
    "concerned"."_dn_extended_tax" as "dn_extended_tax",
    "concerned"."_dn_list_price" as "dn_list_price"
FROM
    "concerned"
ORDER BY 
    "dn_customer_last_name" asc nulls first,
    "dn_physical_sales_ticket_number" asc nulls first
LIMIT (100)
```
