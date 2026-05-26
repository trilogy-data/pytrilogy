# Query 81

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | NO |

## Result comparison

v4 rows: 100 (71 distinct)
ref rows: 100 (100 distinct)
only in v4 (showing up to 5 of 66):
  2x  ('Woodlawn', 'United States', 'Morgan County', Decimal('-5.00'), 'single family', 'GA', 'Walnut ', '329', 'Boulevard', 'Suite 460', '34098', 'Ruth', 'Parker', 'Mrs.', 'AAAAAAAAAAAGAAAA', None)
  1x  ('Edgewood', 'United States', 'Meriwether County', Decimal('-5.00'), 'single family', 'GA', 'Tenth 3rd', '300', 'Road', 'Suite 130', '30069', 'Eduardo', 'Goodwin', 'Mr.', 'AAAAAAAAAAAIBAAA', None)
  1x  ('Oakwood', 'United States', 'Wheeler County', Decimal('-5.00'), 'condo', 'GA', 'Sycamore ', '12', 'Way', 'Suite 440', '30169', 'Jayme', 'Mcfarland', 'Ms.', 'AAAAAAAAAABBAAAA', None)
  1x  ('Bunker Hill', 'United States', 'Tattnall County', Decimal('-5.00'), 'apartment', 'GA', 'Third Cedar', '968', 'RD', 'Suite N', '30150', 'Wendy', 'Jones', 'Mrs.', 'AAAAAAAAAABFBAAA', None)
  1x  ('Riceville', 'United States', 'Wheeler County', Decimal('-5.00'), 'apartment', 'GA', 'Cedar ', '566', 'Wy', 'Suite G', '35867', 'Kristopher', 'Stone', 'Mr.', 'AAAAAAAAAABJAAAA', None)
only in ref (showing up to 5 of 95):
  1x  ('Shiloh', 'United States', 'Hart County', Decimal('-5.00'), 'apartment', 'GA', 'Hickory Broadway', '272', 'Circle', 'Suite A', '39275', 'Kevin', 'Chalmers', 'Sir', 'AAAAAAAAAGCEBAAA', Decimal('6973.39'))
  1x  ('Oneida', 'United States', 'Dougherty County', Decimal('-5.00'), 'apartment', 'GA', '14th ', '904', 'Lane', 'Suite 490', '34027', 'Amy', 'Sullivan', 'Mrs.', 'AAAAAAAAAGKEBAAA', Decimal('5039.40'))
  1x  ('Woodville', 'United States', 'Montgomery County', Decimal('-5.00'), 'single family', 'GA', 'Meadow ', '142', 'Road', 'Suite 460', '34289', 'Eugene', 'Morris', 'Mr.', 'AAAAAAAAAHJAAAAA', Decimal('2182.75'))
  1x  ('Post Oak', 'United States', 'Oglethorpe County', Decimal('-5.00'), 'single family', 'GA', 'River ', '896', 'Ave', 'Suite 310', '38567', None, 'Kirby', None, 'AAAAAAAAAIPLAAAA', Decimal('1811.05'))
  1x  ('Summit', 'United States', 'Murray County', Decimal('-5.00'), 'single family', 'GA', '2nd Ridge', '63', 'Ave', 'Suite S', '30499', 'Garrett', 'King', 'Mr.', 'AAAAAAAAAJDEBAAA', Decimal('5694.48'))

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 6941 | 101 | 54.25 ms |
| reference | 6504 | 111 | 123.80 ms |
| v4 / ref | 1.07x | 0.91x | 0.44x |

## Preql

```
import catalog_returns as cr;

# Non-rowset equivalent of the rowset form: the key-join shape achieved by
# grouping the aggregate to (return_address.state, customer.id). The year/state
# filter is bound *inside* the conditional sum because a plain sum + a row-grain
# WHERE year=2000 loses the filter from the customer-grain aggregate.
auto customer_state <- sum(cr.return_amt_inc_tax ? cr.date.year = 2000 and cr.return_address.state is not null)
    by cr.return_address.state, cr.customer.id;
auto scaled_state <- 1.2 * avg(customer_state) by cr.return_address.state;

where
    customer_state > scaled_state
    and cr.customer.address.state = 'GA'
    and cr.return_address.state is not null
select
    --cr.customer.id,
    cr.customer.text_id,
    cr.customer.salutation,
    cr.customer.first_name,
    cr.customer.last_name,
    cr.customer.address.street_number,
    cr.customer.address.street_name,
    cr.customer.address.street_type,
    cr.customer.address.suite_number,
    cr.customer.address.city,
    cr.customer.address.county,
    cr.customer.address.state,
    cr.customer.address.zip,
    cr.customer.address.country,
    cr.customer.address.gmt_offset,
    cr.customer.address.location_type,
    customer_state,
order by
    cr.customer.text_id asc nulls first,
    cr.customer.salutation asc nulls first,
    cr.customer.first_name asc nulls first,
    cr.customer.last_name asc nulls first,
    cr.customer.address.street_number asc nulls first,
    cr.customer.address.street_name asc nulls first,
    cr.customer.address.street_type asc nulls first,
    cr.customer.address.suite_number asc nulls first,
    cr.customer.address.city asc nulls first,
    cr.customer.address.county asc nulls first,
    cr.customer.address.state asc nulls first,
    cr.customer.address.zip asc nulls first,
    cr.customer.address.country asc nulls first,
    cr.customer.address.gmt_offset asc nulls first,
    cr.customer.address.location_type asc nulls first,
    customer_state asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cooperative as (
SELECT
    "cr_catalog_returns"."CR_RETURNING_CUSTOMER_SK" as "cr_customer_id",
    "cr_catalog_returns"."CR_RETURN_AMT_INC_TAX" as "cr_return_amt_inc_tax",
    "cr_customer_address_customer_address"."CA_CITY" as "cr_customer_address_city",
    "cr_customer_address_customer_address"."CA_COUNTRY" as "cr_customer_address_country",
    "cr_customer_address_customer_address"."CA_COUNTY" as "cr_customer_address_county",
    "cr_customer_address_customer_address"."CA_GMT_OFFSET" as "cr_customer_address_gmt_offset",
    "cr_customer_address_customer_address"."CA_LOCATION_TYPE" as "cr_customer_address_location_type",
    "cr_customer_address_customer_address"."CA_STATE" as "cr_customer_address_state",
    "cr_customer_address_customer_address"."CA_STREET_NAME" as "cr_customer_address_street_name",
    "cr_customer_address_customer_address"."CA_STREET_NUMBER" as "cr_customer_address_street_number",
    "cr_customer_address_customer_address"."CA_STREET_TYPE" as "cr_customer_address_street_type",
    "cr_customer_address_customer_address"."CA_SUITE_NUMBER" as "cr_customer_address_suite_number",
    "cr_customer_address_customer_address"."CA_ZIP" as "cr_customer_address_zip",
    "cr_customer_customers"."C_CUSTOMER_ID" as "cr_customer_text_id",
    "cr_customer_customers"."C_FIRST_NAME" as "cr_customer_first_name",
    "cr_customer_customers"."C_LAST_NAME" as "cr_customer_last_name",
    "cr_customer_customers"."C_SALUTATION" as "cr_customer_salutation",
    "cr_date_date"."D_YEAR" as "cr_date_year",
    "cr_return_address_customer_address"."CA_STATE" as "cr_return_address_state"
FROM
    "memory"."catalog_returns" as "cr_catalog_returns"
    INNER JOIN "memory"."date_dim" as "cr_date_date" on "cr_catalog_returns"."CR_RETURNED_DATE_SK" = "cr_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer" as "cr_customer_customers" on "cr_catalog_returns"."CR_RETURNING_CUSTOMER_SK" = "cr_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "cr_return_address_customer_address" on "cr_catalog_returns"."CR_RETURNING_ADDR_SK" = "cr_return_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_address" as "cr_customer_address_customer_address" on "cr_customer_customers"."C_CURRENT_ADDR_SK" = "cr_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "cr_customer_address_customer_address"."CA_STATE" = 'GA' and "cr_return_address_customer_address"."CA_STATE" is not null
),
questionable as (
SELECT
    "cooperative"."cr_customer_address_city" as "cr_customer_address_city",
    "cooperative"."cr_customer_address_country" as "cr_customer_address_country",
    "cooperative"."cr_customer_address_county" as "cr_customer_address_county",
    "cooperative"."cr_customer_address_gmt_offset" as "cr_customer_address_gmt_offset",
    "cooperative"."cr_customer_address_location_type" as "cr_customer_address_location_type",
    "cooperative"."cr_customer_address_state" as "cr_customer_address_state",
    "cooperative"."cr_customer_address_street_name" as "cr_customer_address_street_name",
    "cooperative"."cr_customer_address_street_number" as "cr_customer_address_street_number",
    "cooperative"."cr_customer_address_street_type" as "cr_customer_address_street_type",
    "cooperative"."cr_customer_address_suite_number" as "cr_customer_address_suite_number",
    "cooperative"."cr_customer_address_zip" as "cr_customer_address_zip",
    "cooperative"."cr_customer_first_name" as "cr_customer_first_name",
    "cooperative"."cr_customer_id" as "cr_customer_id",
    "cooperative"."cr_customer_last_name" as "cr_customer_last_name",
    "cooperative"."cr_customer_salutation" as "cr_customer_salutation",
    "cooperative"."cr_customer_text_id" as "cr_customer_text_id",
    "cooperative"."cr_return_address_state" as "cr_return_address_state",
    CASE WHEN "cooperative"."cr_date_year" = 2000 and "cooperative"."cr_return_address_state" is not null THEN "cooperative"."cr_return_amt_inc_tax" ELSE NULL END as "_virt_filter_return_amt_inc_tax_2184255153361204"
FROM
    "cooperative"),
abundant as (
SELECT
    "questionable"."cr_customer_id" as "cr_customer_id",
    "questionable"."cr_return_address_state" as "cr_return_address_state",
    sum("questionable"."_virt_filter_return_amt_inc_tax_2184255153361204") as "customer_state"
FROM
    "questionable"
GROUP BY
    1,
    2)
SELECT
    "questionable"."cr_customer_text_id" as "cr_customer_text_id",
    "questionable"."cr_customer_salutation" as "cr_customer_salutation",
    "questionable"."cr_customer_first_name" as "cr_customer_first_name",
    "questionable"."cr_customer_last_name" as "cr_customer_last_name",
    "questionable"."cr_customer_address_street_number" as "cr_customer_address_street_number",
    "questionable"."cr_customer_address_street_name" as "cr_customer_address_street_name",
    "questionable"."cr_customer_address_street_type" as "cr_customer_address_street_type",
    "questionable"."cr_customer_address_suite_number" as "cr_customer_address_suite_number",
    "questionable"."cr_customer_address_city" as "cr_customer_address_city",
    "questionable"."cr_customer_address_county" as "cr_customer_address_county",
    "questionable"."cr_customer_address_state" as "cr_customer_address_state",
    "questionable"."cr_customer_address_zip" as "cr_customer_address_zip",
    "questionable"."cr_customer_address_country" as "cr_customer_address_country",
    "questionable"."cr_customer_address_gmt_offset" as "cr_customer_address_gmt_offset",
    "questionable"."cr_customer_address_location_type" as "cr_customer_address_location_type",
    "abundant"."customer_state" as "customer_state"
FROM
    "abundant"
    FULL JOIN "questionable" on "abundant"."cr_customer_id" = "questionable"."cr_customer_id" AND "abundant"."cr_return_address_state" is not distinct from "questionable"."cr_return_address_state"
ORDER BY 
    "questionable"."cr_customer_text_id" asc nulls first,
    "questionable"."cr_customer_salutation" asc nulls first,
    "questionable"."cr_customer_first_name" asc nulls first,
    "questionable"."cr_customer_last_name" asc nulls first,
    "questionable"."cr_customer_address_street_number" asc nulls first,
    "questionable"."cr_customer_address_street_name" asc nulls first,
    "questionable"."cr_customer_address_street_type" asc nulls first,
    "questionable"."cr_customer_address_suite_number" asc nulls first,
    "questionable"."cr_customer_address_city" asc nulls first,
    "questionable"."cr_customer_address_county" asc nulls first,
    "questionable"."cr_customer_address_state" asc nulls first,
    "questionable"."cr_customer_address_zip" asc nulls first,
    "questionable"."cr_customer_address_country" asc nulls first,
    "questionable"."cr_customer_address_gmt_offset" asc nulls first,
    "questionable"."cr_customer_address_location_type" asc nulls first,
    "abundant"."customer_state" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "cr_catalog_returns"."CR_RETURNING_CUSTOMER_SK" as "cr_customer_id",
    "cr_return_address_customer_address"."CA_STATE" as "cr_return_address_state",
    sum(CASE WHEN "cr_date_date"."D_YEAR" = 2000 and "cr_return_address_customer_address"."CA_STATE" is not null THEN "cr_catalog_returns"."CR_RETURN_AMT_INC_TAX" ELSE NULL END) as "customer_state"
FROM
    "memory"."catalog_returns" as "cr_catalog_returns"
    INNER JOIN "memory"."date_dim" as "cr_date_date" on "cr_catalog_returns"."CR_RETURNED_DATE_SK" = "cr_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "cr_return_address_customer_address" on "cr_catalog_returns"."CR_RETURNING_ADDR_SK" = "cr_return_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "cr_return_address_customer_address"."CA_STATE" is not null

GROUP BY
    1,
    2),
yummy as (
SELECT
    "cr_customer_address_customer_address"."CA_CITY" as "cr_customer_address_city",
    "cr_customer_address_customer_address"."CA_COUNTRY" as "cr_customer_address_country",
    "cr_customer_address_customer_address"."CA_COUNTY" as "cr_customer_address_county",
    "cr_customer_address_customer_address"."CA_GMT_OFFSET" as "cr_customer_address_gmt_offset",
    "cr_customer_address_customer_address"."CA_LOCATION_TYPE" as "cr_customer_address_location_type",
    "cr_customer_address_customer_address"."CA_STATE" as "cr_customer_address_state",
    "cr_customer_address_customer_address"."CA_STREET_NAME" as "cr_customer_address_street_name",
    "cr_customer_address_customer_address"."CA_STREET_NUMBER" as "cr_customer_address_street_number",
    "cr_customer_address_customer_address"."CA_STREET_TYPE" as "cr_customer_address_street_type",
    "cr_customer_address_customer_address"."CA_SUITE_NUMBER" as "cr_customer_address_suite_number",
    "cr_customer_address_customer_address"."CA_ZIP" as "cr_customer_address_zip",
    "cr_customer_customers"."C_CUSTOMER_ID" as "cr_customer_text_id",
    "cr_customer_customers"."C_CUSTOMER_SK" as "cr_customer_id",
    "cr_customer_customers"."C_FIRST_NAME" as "cr_customer_first_name",
    "cr_customer_customers"."C_LAST_NAME" as "cr_customer_last_name",
    "cr_customer_customers"."C_SALUTATION" as "cr_customer_salutation"
FROM
    "memory"."customer_address" as "cr_customer_address_customer_address"
    INNER JOIN "memory"."customer" as "cr_customer_customers" on "cr_customer_address_customer_address"."CA_ADDRESS_SK" = "cr_customer_customers"."C_CURRENT_ADDR_SK"
WHERE
    "cr_customer_address_customer_address"."CA_STATE" = 'GA'
),
questionable as (
SELECT
    "cheerful"."cr_return_address_state" as "cr_return_address_state",
    avg("cheerful"."customer_state") as "_virt_agg_avg_7052944147524274"
FROM
    "cheerful"
GROUP BY
    1),
juicy as (
SELECT
    "cheerful"."cr_return_address_state" as "cr_return_address_state",
    "cheerful"."customer_state" as "customer_state",
    "yummy"."cr_customer_address_city" as "cr_customer_address_city",
    "yummy"."cr_customer_address_country" as "cr_customer_address_country",
    "yummy"."cr_customer_address_county" as "cr_customer_address_county",
    "yummy"."cr_customer_address_gmt_offset" as "cr_customer_address_gmt_offset",
    "yummy"."cr_customer_address_location_type" as "cr_customer_address_location_type",
    "yummy"."cr_customer_address_state" as "cr_customer_address_state",
    "yummy"."cr_customer_address_street_name" as "cr_customer_address_street_name",
    "yummy"."cr_customer_address_street_number" as "cr_customer_address_street_number",
    "yummy"."cr_customer_address_street_type" as "cr_customer_address_street_type",
    "yummy"."cr_customer_address_suite_number" as "cr_customer_address_suite_number",
    "yummy"."cr_customer_address_zip" as "cr_customer_address_zip",
    "yummy"."cr_customer_first_name" as "cr_customer_first_name",
    "yummy"."cr_customer_last_name" as "cr_customer_last_name",
    "yummy"."cr_customer_salutation" as "cr_customer_salutation",
    "yummy"."cr_customer_text_id" as "cr_customer_text_id"
FROM
    "cheerful"
    INNER JOIN "yummy" on "cheerful"."cr_customer_id" = "yummy"."cr_customer_id")
SELECT
    "juicy"."cr_customer_text_id" as "cr_customer_text_id",
    "juicy"."cr_customer_salutation" as "cr_customer_salutation",
    "juicy"."cr_customer_first_name" as "cr_customer_first_name",
    "juicy"."cr_customer_last_name" as "cr_customer_last_name",
    "juicy"."cr_customer_address_street_number" as "cr_customer_address_street_number",
    "juicy"."cr_customer_address_street_name" as "cr_customer_address_street_name",
    "juicy"."cr_customer_address_street_type" as "cr_customer_address_street_type",
    "juicy"."cr_customer_address_suite_number" as "cr_customer_address_suite_number",
    "juicy"."cr_customer_address_city" as "cr_customer_address_city",
    "juicy"."cr_customer_address_county" as "cr_customer_address_county",
    "juicy"."cr_customer_address_state" as "cr_customer_address_state",
    "juicy"."cr_customer_address_zip" as "cr_customer_address_zip",
    "juicy"."cr_customer_address_country" as "cr_customer_address_country",
    "juicy"."cr_customer_address_gmt_offset" as "cr_customer_address_gmt_offset",
    "juicy"."cr_customer_address_location_type" as "cr_customer_address_location_type",
    "juicy"."customer_state" as "customer_state"
FROM
    "juicy"
    LEFT OUTER JOIN "questionable" on "juicy"."cr_return_address_state" is not distinct from "questionable"."cr_return_address_state"
WHERE
    "juicy"."customer_state" > 1.2 * "questionable"."_virt_agg_avg_7052944147524274"

ORDER BY 
    "juicy"."cr_customer_text_id" asc nulls first,
    "juicy"."cr_customer_salutation" asc nulls first,
    "juicy"."cr_customer_first_name" asc nulls first,
    "juicy"."cr_customer_last_name" asc nulls first,
    "juicy"."cr_customer_address_street_number" asc nulls first,
    "juicy"."cr_customer_address_street_name" asc nulls first,
    "juicy"."cr_customer_address_street_type" asc nulls first,
    "juicy"."cr_customer_address_suite_number" asc nulls first,
    "juicy"."cr_customer_address_city" asc nulls first,
    "juicy"."cr_customer_address_county" asc nulls first,
    "juicy"."cr_customer_address_state" asc nulls first,
    "juicy"."cr_customer_address_zip" asc nulls first,
    "juicy"."cr_customer_address_country" asc nulls first,
    "juicy"."cr_customer_address_gmt_offset" asc nulls first,
    "juicy"."cr_customer_address_location_type" asc nulls first,
    "juicy"."customer_state" asc nulls first
LIMIT (100)
```
