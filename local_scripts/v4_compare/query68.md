# Query 68

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | NO |

## Result comparison

v4 rows: 100 (100 distinct)
ref rows: 100 (100 distinct)
only in v4 (showing up to 5 of 100):
  1x  ('Unionville', None, 4344, 4344, None, Decimal('15617.02'), Decimal('1059.20'), Decimal('35200.10'), 'Woodlawn', 70)
  1x  ('Midway', None, 67575, 67575, None, Decimal('16050.99'), Decimal('361.53'), Decimal('25084.90'), 'Oakwood', 11488)
  1x  ('Caledonia', 'Rene', 88496, 88496, None, Decimal('18982.21'), Decimal('851.66'), Decimal('61572.90'), 'Stringtown', 16638)
  1x  ('Parkwood', 'John', 95304, 95304, None, Decimal('27211.89'), Decimal('1604.38'), Decimal('43810.22'), 'Edgewood', 24159)
  1x  ('Deerfield', 'Ana', 55591, 55591, None, Decimal('29588.22'), Decimal('1896.98'), Decimal('44933.70'), 'Jamestown', 26329)
only in ref (showing up to 5 of 100):
  1x  ('Unionville', None, None, Decimal('15617.02'), Decimal('1059.20'), Decimal('35200.10'), 'Woodlawn', 70)
  1x  ('Midway', None, None, Decimal('16050.99'), Decimal('361.53'), Decimal('25084.90'), 'Oakwood', 11488)
  1x  ('Caledonia', 'Rene', None, Decimal('18982.21'), Decimal('851.66'), Decimal('61572.90'), 'Stringtown', 16638)
  1x  ('Parkwood', 'John', None, Decimal('27211.89'), Decimal('1604.38'), Decimal('43810.22'), 'Edgewood', 24159)
  1x  ('Deerfield', 'Ana', None, Decimal('29588.22'), Decimal('1896.98'), Decimal('44933.70'), 'Jamestown', 26329)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 3293 | 54 | 63.53 ms |
| reference | 3059 | 50 | 63.20 ms |
| v4 / ref | 1.08x | 1.08x | 1.01x |

## Preql

```
import store_sales as store_sales;
import customer as customer;

rowset dn <- where
    store_sales.date.day_of_month between 1 and 2
    and (
        store_sales.household_demographic.dependent_count = 4
        or store_sales.household_demographic.vehicle_count = 3
    )
    and store_sales.date.year in (1999, 2000, 2001)
    and store_sales.store.city in ('Fairview', 'Midway')
    and store_sales.customer.id is not null
select
    store_sales.ticket_number,
    store_sales.customer.id,
    store_sales.sale_address.city,
    sum(store_sales.ext_sales_price) as extended_price,
    sum(store_sales.ext_tax) as extended_tax,
    sum(store_sales.ext_list_price) as list_price,
merge
select
    customer.id,
    customer.last_name,
    customer.first_name,
    customer.address.city,
align
    customer_id: store_sales.customer.id, customer.id
where
customer.address.city != store_sales.sale_address.city
;

select
    dn.customer.last_name,
    dn.customer.first_name,
    dn.customer.address.city,
    dn.store_sales.sale_address.city,
    dn.store_sales.ticket_number,
    dn.extended_price,
    dn.extended_tax,
    dn.list_price,
order by
    dn.customer.last_name asc nulls first,
    dn.store_sales.ticket_number asc nulls first
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
    "memory"."customer_address" as "customer_address_customer_address"
    INNER JOIN "memory"."customer" as "customer_customers" on "customer_address_customer_address"."CA_ADDRESS_SK" = "customer_customers"."C_CURRENT_ADDR_SK"),
uneven as (
SELECT
    "store_sales_sale_address_customer_address"."CA_CITY" as "store_sales_sale_address_city",
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "customer_id",
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number",
    sum("store_sales_store_sales"."SS_EXT_LIST_PRICE") as "_dn_list_price",
    sum("store_sales_store_sales"."SS_EXT_SALES_PRICE") as "_dn_extended_price",
    sum("store_sales_store_sales"."SS_EXT_TAX") as "_dn_extended_tax"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    LEFT OUTER JOIN "memory"."customer_address" as "store_sales_sale_address_customer_address" on "store_sales_store_sales"."SS_ADDR_SK" = "store_sales_sale_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."household_demographics" as "store_sales_household_demographic_household_demographics" on "store_sales_store_sales"."SS_HDEMO_SK" = "store_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "store_sales_date_date"."D_DOM" BETWEEN 1 AND 2 and ( "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 4 or "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" = 3 ) and "store_sales_date_date"."D_YEAR" in (1999,2000,2001) and "store_sales_store_store"."S_CITY" in ('Fairview','Midway') and "store_sales_store_sales"."SS_CUSTOMER_SK" is not null

GROUP BY
    1,
    3,
    4)
SELECT
    "wakeful"."customer_id" as "dn_customer_id",
    "uneven"."store_sales_ticket_number" as "dn_store_sales_ticket_number",
    "uneven"."store_sales_sale_address_city" as "dn_store_sales_sale_address_city",
    "uneven"."_dn_extended_price" as "dn_extended_price",
    "uneven"."_dn_extended_tax" as "dn_extended_tax",
    "uneven"."_dn_list_price" as "dn_list_price",
    "wakeful"."customer_id" as "dn_customer_id",
    "wakeful"."customer_last_name" as "dn_customer_last_name",
    "wakeful"."customer_first_name" as "dn_customer_first_name",
    "wakeful"."customer_address_city" as "dn_customer_address_city"
FROM
    "uneven"
    INNER JOIN "wakeful" on "uneven"."customer_id" = "wakeful"."customer_id"
WHERE
    "wakeful"."customer_address_city" != "uneven"."store_sales_sale_address_city"

ORDER BY 
    "dn_customer_last_name" asc nulls first,
    "dn_store_sales_ticket_number" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
wakeful as (
SELECT
    "customer_address_customer_address"."CA_CITY" as "customer_address_city",
    "customer_customers"."C_CUSTOMER_SK" as "customer_id",
    "customer_customers"."C_FIRST_NAME" as "customer_first_name",
    "customer_customers"."C_LAST_NAME" as "customer_last_name"
FROM
    "memory"."customer_address" as "customer_address_customer_address"
    INNER JOIN "memory"."customer" as "customer_customers" on "customer_address_customer_address"."CA_ADDRESS_SK" = "customer_customers"."C_CURRENT_ADDR_SK"),
uneven as (
SELECT
    "store_sales_sale_address_customer_address"."CA_CITY" as "store_sales_sale_address_city",
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "customer_id",
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number",
    sum("store_sales_store_sales"."SS_EXT_LIST_PRICE") as "_dn_list_price",
    sum("store_sales_store_sales"."SS_EXT_SALES_PRICE") as "_dn_extended_price",
    sum("store_sales_store_sales"."SS_EXT_TAX") as "_dn_extended_tax"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    LEFT OUTER JOIN "memory"."customer_address" as "store_sales_sale_address_customer_address" on "store_sales_store_sales"."SS_ADDR_SK" = "store_sales_sale_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."household_demographics" as "store_sales_household_demographic_household_demographics" on "store_sales_store_sales"."SS_HDEMO_SK" = "store_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "store_sales_date_date"."D_DOM" BETWEEN 1 AND 2 and ( "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" = 4 or "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" = 3 ) and "store_sales_date_date"."D_YEAR" in (1999,2000,2001) and "store_sales_store_store"."S_CITY" in ('Fairview','Midway') and "store_sales_store_sales"."SS_CUSTOMER_SK" is not null

GROUP BY
    1,
    2,
    3)
SELECT
    "wakeful"."customer_last_name" as "dn_customer_last_name",
    "wakeful"."customer_first_name" as "dn_customer_first_name",
    "wakeful"."customer_address_city" as "dn_customer_address_city",
    "uneven"."store_sales_sale_address_city" as "dn_store_sales_sale_address_city",
    "uneven"."store_sales_ticket_number" as "dn_store_sales_ticket_number",
    "uneven"."_dn_extended_price" as "dn_extended_price",
    "uneven"."_dn_extended_tax" as "dn_extended_tax",
    "uneven"."_dn_list_price" as "dn_list_price"
FROM
    "uneven"
    INNER JOIN "wakeful" on "uneven"."customer_id" = "wakeful"."customer_id"
WHERE
    "wakeful"."customer_address_city" != "uneven"."store_sales_sale_address_city"

ORDER BY 
    "dn_customer_last_name" asc nulls first,
    "dn_store_sales_ticket_number" asc nulls first
LIMIT (100)
```
