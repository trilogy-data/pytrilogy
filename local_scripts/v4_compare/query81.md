# Query 81

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | NO |

## Result comparison

v4 rows: 100 (58 distinct)
ref rows: 100 (100 distinct)
only in v4 (showing up to 5 of 53):
  2x  ('Woodlawn', 'United States', 'Morgan County', Decimal('-5.00000000'), 'single family', 'GA', 'Walnut ', '329', 'Boulevard', 'Suite 460', '34098', 'Ruth', 'Parker', 'Mrs.', 'AAAAAAAAAAAGAAAA', None)
  1x  ('Edgewood', 'United States', 'Meriwether County', Decimal('-5.00000000'), 'single family', 'GA', 'Tenth 3rd', '300', 'Road', 'Suite 130', '30069', 'Eduardo', 'Goodwin', 'Mr.', 'AAAAAAAAAAAIBAAA', None)
  1x  ('Oakwood', 'United States', 'Wheeler County', Decimal('-5.00000000'), 'condo', 'GA', 'Sycamore ', '12', 'Way', 'Suite 440', '30169', 'Jayme', 'Mcfarland', 'Ms.', 'AAAAAAAAAABBAAAA', None)
  1x  ('Bunker Hill', 'United States', 'Tattnall County', Decimal('-5.00000000'), 'apartment', 'GA', 'Third Cedar', '968', 'RD', 'Suite N', '30150', 'Wendy', 'Jones', 'Mrs.', 'AAAAAAAAAABFBAAA', None)
  2x  ('Riceville', 'United States', 'Wheeler County', Decimal('-5.00000000'), 'apartment', 'GA', 'Cedar ', '566', 'Wy', 'Suite G', '35867', 'Kristopher', 'Stone', 'Mr.', 'AAAAAAAAAABJAAAA', None)
only in ref (showing up to 5 of 95):
  1x  ('Shiloh', 'United States', 'Hart County', Decimal('-5.00000000'), 'apartment', 'GA', 'Hickory Broadway', '272', 'Circle', 'Suite A', '39275', 'Kevin', 'Chalmers', 'Sir', 'AAAAAAAAAGCEBAAA', Decimal('6973.39000000'))
  1x  ('Oneida', 'United States', 'Dougherty County', Decimal('-5.00000000'), 'apartment', 'GA', '14th ', '904', 'Lane', 'Suite 490', '34027', 'Amy', 'Sullivan', 'Mrs.', 'AAAAAAAAAGKEBAAA', Decimal('5039.40000000'))
  1x  ('Woodville', 'United States', 'Montgomery County', Decimal('-5.00000000'), 'single family', 'GA', 'Meadow ', '142', 'Road', 'Suite 460', '34289', 'Eugene', 'Morris', 'Mr.', 'AAAAAAAAAHJAAAAA', Decimal('2182.75000000'))
  1x  ('Post Oak', 'United States', 'Oglethorpe County', Decimal('-5.00000000'), 'single family', 'GA', 'River ', '896', 'Ave', 'Suite 310', '38567', None, 'Kirby', None, 'AAAAAAAAAIPLAAAA', Decimal('1811.05000000'))
  1x  ('Summit', 'United States', 'Murray County', Decimal('-5.00000000'), 'single family', 'GA', '2nd Ridge', '63', 'Ave', 'Suite S', '30499', 'Garrett', 'King', 'Mr.', 'AAAAAAAAAJDEBAAA', Decimal('5694.48000000'))

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 9649 | 135 | 125.98 ms |
| reference | 7464 | 111 | 99.92 ms |
| v4 / ref | 1.29x | 1.22x | 1.26x |

## Preql

```
import catalog_returns as cr;

# Non-rowset equivalent of the rowset form: the key-join shape achieved by
# grouping the aggregate to (return_address.state, customer.id). The year/state
# filter is bound *inside* the conditional sum because a plain sum + a row-grain
# WHERE year=2000 loses the filter from the customer-grain aggregate.
auto customer_state <- sum(cr.return_amt_inc_tax ? cr.date.year = 2000 and cr.return_address.state is not null)
    by cr.return_address.state, cr.billing_customer.id;
auto scaled_state <- 1.2 * avg(customer_state) by cr.return_address.state;

where
    customer_state > scaled_state
    and cr.billing_customer.address.state = 'GA'
    and cr.return_address.state is not null
select
    --cr.billing_customer.id,
    cr.billing_customer.text_id,
    cr.billing_customer.salutation,
    cr.billing_customer.first_name,
    cr.billing_customer.last_name,
    cr.billing_customer.address.street_number,
    cr.billing_customer.address.street_name,
    cr.billing_customer.address.street_type,
    cr.billing_customer.address.suite_number,
    cr.billing_customer.address.city,
    cr.billing_customer.address.county,
    cr.billing_customer.address.state,
    cr.billing_customer.address.zip,
    cr.billing_customer.address.country,
    cr.billing_customer.address.gmt_offset,
    cr.billing_customer.address.location_type,
    customer_state,
order by
    cr.billing_customer.text_id asc nulls first,
    cr.billing_customer.salutation asc nulls first,
    cr.billing_customer.first_name asc nulls first,
    cr.billing_customer.last_name asc nulls first,
    cr.billing_customer.address.street_number asc nulls first,
    cr.billing_customer.address.street_name asc nulls first,
    cr.billing_customer.address.street_type asc nulls first,
    cr.billing_customer.address.suite_number asc nulls first,
    cr.billing_customer.address.city asc nulls first,
    cr.billing_customer.address.county asc nulls first,
    cr.billing_customer.address.state asc nulls first,
    cr.billing_customer.address.zip asc nulls first,
    cr.billing_customer.address.country asc nulls first,
    cr.billing_customer.address.gmt_offset asc nulls first,
    cr.billing_customer.address.location_type asc nulls first,
    customer_state asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
abundant as (
SELECT
    "cr_catalog_returns"."CR_RETURNING_CUSTOMER_SK" as "cr_billing_customer_id",
    "cr_return_address_customer_address"."CA_STATE" as "cr_return_address_state",
    sum(CASE WHEN "cr_date_date"."D_YEAR" = 2000 and "cr_return_address_customer_address"."CA_STATE" is not null THEN "cr_catalog_returns"."CR_RETURN_AMT_INC_TAX" ELSE NULL END) as "customer_state"
FROM
    "memory"."catalog_returns" as "cr_catalog_returns"
    INNER JOIN "memory"."date_dim" as "cr_date_date" on "cr_catalog_returns"."CR_RETURNED_DATE_SK" = "cr_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "cr_return_address_customer_address" on "cr_catalog_returns"."CR_RETURNING_ADDR_SK" = "cr_return_address_customer_address"."CA_ADDRESS_SK"
GROUP BY
    1,
    2),
cooperative as (
SELECT
    "cr_billing_customer_address_customer_address"."CA_CITY" as "cr_billing_customer_address_city",
    "cr_billing_customer_address_customer_address"."CA_COUNTRY" as "cr_billing_customer_address_country",
    "cr_billing_customer_address_customer_address"."CA_COUNTY" as "cr_billing_customer_address_county",
    "cr_billing_customer_address_customer_address"."CA_GMT_OFFSET" as "cr_billing_customer_address_gmt_offset",
    "cr_billing_customer_address_customer_address"."CA_LOCATION_TYPE" as "cr_billing_customer_address_location_type",
    "cr_billing_customer_address_customer_address"."CA_STATE" as "cr_billing_customer_address_state",
    "cr_billing_customer_address_customer_address"."CA_STREET_NAME" as "cr_billing_customer_address_street_name",
    "cr_billing_customer_address_customer_address"."CA_STREET_NUMBER" as "cr_billing_customer_address_street_number",
    "cr_billing_customer_address_customer_address"."CA_STREET_TYPE" as "cr_billing_customer_address_street_type",
    "cr_billing_customer_address_customer_address"."CA_SUITE_NUMBER" as "cr_billing_customer_address_suite_number",
    "cr_billing_customer_address_customer_address"."CA_ZIP" as "cr_billing_customer_address_zip",
    "cr_billing_customer_customers"."C_CUSTOMER_ID" as "cr_billing_customer_text_id",
    "cr_billing_customer_customers"."C_CUSTOMER_SK" as "cr_billing_customer_id",
    "cr_billing_customer_customers"."C_FIRST_NAME" as "cr_billing_customer_first_name",
    "cr_billing_customer_customers"."C_LAST_NAME" as "cr_billing_customer_last_name",
    "cr_billing_customer_customers"."C_SALUTATION" as "cr_billing_customer_salutation",
    "cr_return_address_customer_address"."CA_STATE" as "cr_return_address_state"
FROM
    "memory"."catalog_returns" as "cr_catalog_returns"
    INNER JOIN "memory"."date_dim" as "cr_date_date" on "cr_catalog_returns"."CR_RETURNED_DATE_SK" = "cr_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "cr_return_address_customer_address" on "cr_catalog_returns"."CR_RETURNING_ADDR_SK" = "cr_return_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer" as "cr_billing_customer_customers" on "cr_catalog_returns"."CR_RETURNING_CUSTOMER_SK" = "cr_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "cr_billing_customer_address_customer_address" on "cr_billing_customer_customers"."C_CURRENT_ADDR_SK" = "cr_billing_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "cr_billing_customer_address_customer_address"."CA_STATE" = 'GA' and "cr_return_address_customer_address"."CA_STATE" is not null
),
juicy as (
SELECT
    "abundant"."cr_return_address_state" as "cr_return_address_state",
    avg("abundant"."customer_state") as "_virt_agg_avg_7052944147524274"
FROM
    "abundant"
GROUP BY
    1),
questionable as (
SELECT
    "cooperative"."cr_billing_customer_address_city" as "cr_billing_customer_address_city",
    "cooperative"."cr_billing_customer_address_country" as "cr_billing_customer_address_country",
    "cooperative"."cr_billing_customer_address_county" as "cr_billing_customer_address_county",
    "cooperative"."cr_billing_customer_address_gmt_offset" as "cr_billing_customer_address_gmt_offset",
    "cooperative"."cr_billing_customer_address_location_type" as "cr_billing_customer_address_location_type",
    "cooperative"."cr_billing_customer_address_state" as "cr_billing_customer_address_state",
    "cooperative"."cr_billing_customer_address_street_name" as "cr_billing_customer_address_street_name",
    "cooperative"."cr_billing_customer_address_street_number" as "cr_billing_customer_address_street_number",
    "cooperative"."cr_billing_customer_address_street_type" as "cr_billing_customer_address_street_type",
    "cooperative"."cr_billing_customer_address_suite_number" as "cr_billing_customer_address_suite_number",
    "cooperative"."cr_billing_customer_address_zip" as "cr_billing_customer_address_zip",
    "cooperative"."cr_billing_customer_first_name" as "cr_billing_customer_first_name",
    "cooperative"."cr_billing_customer_id" as "cr_billing_customer_id",
    "cooperative"."cr_billing_customer_last_name" as "cr_billing_customer_last_name",
    "cooperative"."cr_billing_customer_salutation" as "cr_billing_customer_salutation",
    "cooperative"."cr_billing_customer_text_id" as "cr_billing_customer_text_id",
    "cooperative"."cr_return_address_state" as "cr_return_address_state"
FROM
    "cooperative"),
vacuous as (
SELECT
    "juicy"."cr_return_address_state" as "cr_return_address_state",
    1.2 * "juicy"."_virt_agg_avg_7052944147524274" as "scaled_state"
FROM
    "juicy"),
concerned as (
SELECT
    "abundant"."customer_state" as "customer_state",
    "questionable"."cr_billing_customer_text_id" as "cr_billing_customer_text_id",
    coalesce("abundant"."cr_billing_customer_id","questionable"."cr_billing_customer_id") as "cr_billing_customer_id",
    coalesce("abundant"."cr_return_address_state","questionable"."cr_return_address_state","vacuous"."cr_return_address_state") as "cr_return_address_state"
FROM
    "questionable"
    RIGHT OUTER JOIN "abundant" on "questionable"."cr_billing_customer_id" = "abundant"."cr_billing_customer_id" AND "questionable"."cr_return_address_state" is not distinct from "abundant"."cr_return_address_state"
    INNER JOIN "vacuous" on "questionable"."cr_return_address_state" is not distinct from "vacuous"."cr_return_address_state"
WHERE
    "abundant"."customer_state" > "vacuous"."scaled_state"

GROUP BY
    1,
    2,
    3,
    4,
    "questionable"."cr_billing_customer_first_name",
    "questionable"."cr_billing_customer_last_name",
    "questionable"."cr_billing_customer_salutation")
SELECT
    coalesce("concerned"."cr_billing_customer_text_id","questionable"."cr_billing_customer_text_id") as "cr_billing_customer_text_id",
    "questionable"."cr_billing_customer_salutation" as "cr_billing_customer_salutation",
    "questionable"."cr_billing_customer_first_name" as "cr_billing_customer_first_name",
    "questionable"."cr_billing_customer_last_name" as "cr_billing_customer_last_name",
    "questionable"."cr_billing_customer_address_street_number" as "cr_billing_customer_address_street_number",
    "questionable"."cr_billing_customer_address_street_name" as "cr_billing_customer_address_street_name",
    "questionable"."cr_billing_customer_address_street_type" as "cr_billing_customer_address_street_type",
    "questionable"."cr_billing_customer_address_suite_number" as "cr_billing_customer_address_suite_number",
    "questionable"."cr_billing_customer_address_city" as "cr_billing_customer_address_city",
    "questionable"."cr_billing_customer_address_county" as "cr_billing_customer_address_county",
    "questionable"."cr_billing_customer_address_state" as "cr_billing_customer_address_state",
    "questionable"."cr_billing_customer_address_zip" as "cr_billing_customer_address_zip",
    "questionable"."cr_billing_customer_address_country" as "cr_billing_customer_address_country",
    "questionable"."cr_billing_customer_address_gmt_offset" as "cr_billing_customer_address_gmt_offset",
    "questionable"."cr_billing_customer_address_location_type" as "cr_billing_customer_address_location_type",
    "concerned"."customer_state" as "customer_state"
FROM
    "questionable"
    FULL JOIN "concerned" on "questionable"."cr_billing_customer_id" = "concerned"."cr_billing_customer_id" AND "questionable"."cr_billing_customer_text_id" = "concerned"."cr_billing_customer_text_id" AND "questionable"."cr_return_address_state" is not distinct from "concerned"."cr_return_address_state"
ORDER BY 
    coalesce("concerned"."cr_billing_customer_text_id","questionable"."cr_billing_customer_text_id") asc nulls first,
    "questionable"."cr_billing_customer_salutation" asc nulls first,
    "questionable"."cr_billing_customer_first_name" asc nulls first,
    "questionable"."cr_billing_customer_last_name" asc nulls first,
    "questionable"."cr_billing_customer_address_street_number" asc nulls first,
    "questionable"."cr_billing_customer_address_street_name" asc nulls first,
    "questionable"."cr_billing_customer_address_street_type" asc nulls first,
    "questionable"."cr_billing_customer_address_suite_number" asc nulls first,
    "questionable"."cr_billing_customer_address_city" asc nulls first,
    "questionable"."cr_billing_customer_address_county" asc nulls first,
    "questionable"."cr_billing_customer_address_state" asc nulls first,
    "questionable"."cr_billing_customer_address_zip" asc nulls first,
    "questionable"."cr_billing_customer_address_country" asc nulls first,
    "questionable"."cr_billing_customer_address_gmt_offset" asc nulls first,
    "questionable"."cr_billing_customer_address_location_type" asc nulls first,
    "concerned"."customer_state" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
questionable as (
SELECT
    "cr_catalog_returns"."CR_RETURNING_CUSTOMER_SK" as "cr_billing_customer_id",
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
wakeful as (
SELECT
    "cr_billing_customer_address_customer_address"."CA_CITY" as "cr_billing_customer_address_city",
    "cr_billing_customer_address_customer_address"."CA_COUNTRY" as "cr_billing_customer_address_country",
    "cr_billing_customer_address_customer_address"."CA_COUNTY" as "cr_billing_customer_address_county",
    "cr_billing_customer_address_customer_address"."CA_GMT_OFFSET" as "cr_billing_customer_address_gmt_offset",
    "cr_billing_customer_address_customer_address"."CA_LOCATION_TYPE" as "cr_billing_customer_address_location_type",
    "cr_billing_customer_address_customer_address"."CA_STATE" as "cr_billing_customer_address_state",
    "cr_billing_customer_address_customer_address"."CA_STREET_NAME" as "cr_billing_customer_address_street_name",
    "cr_billing_customer_address_customer_address"."CA_STREET_NUMBER" as "cr_billing_customer_address_street_number",
    "cr_billing_customer_address_customer_address"."CA_STREET_TYPE" as "cr_billing_customer_address_street_type",
    "cr_billing_customer_address_customer_address"."CA_SUITE_NUMBER" as "cr_billing_customer_address_suite_number",
    "cr_billing_customer_address_customer_address"."CA_ZIP" as "cr_billing_customer_address_zip",
    "cr_billing_customer_customers"."C_CUSTOMER_ID" as "cr_billing_customer_text_id",
    "cr_billing_customer_customers"."C_CUSTOMER_SK" as "cr_billing_customer_id",
    "cr_billing_customer_customers"."C_FIRST_NAME" as "cr_billing_customer_first_name",
    "cr_billing_customer_customers"."C_LAST_NAME" as "cr_billing_customer_last_name",
    "cr_billing_customer_customers"."C_SALUTATION" as "cr_billing_customer_salutation"
FROM
    "memory"."customer" as "cr_billing_customer_customers"
    INNER JOIN "memory"."customer_address" as "cr_billing_customer_address_customer_address" on "cr_billing_customer_customers"."C_CURRENT_ADDR_SK" = "cr_billing_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "cr_billing_customer_address_customer_address"."CA_STATE" = 'GA'
),
juicy as (
SELECT
    "questionable"."cr_return_address_state" as "cr_return_address_state",
    avg("questionable"."customer_state") as "_virt_agg_avg_7052944147524274"
FROM
    "questionable"
GROUP BY
    1),
yummy as (
SELECT
    "questionable"."cr_return_address_state" as "cr_return_address_state",
    "questionable"."customer_state" as "customer_state",
    "wakeful"."cr_billing_customer_address_city" as "cr_billing_customer_address_city",
    "wakeful"."cr_billing_customer_address_country" as "cr_billing_customer_address_country",
    "wakeful"."cr_billing_customer_address_county" as "cr_billing_customer_address_county",
    "wakeful"."cr_billing_customer_address_gmt_offset" as "cr_billing_customer_address_gmt_offset",
    "wakeful"."cr_billing_customer_address_location_type" as "cr_billing_customer_address_location_type",
    "wakeful"."cr_billing_customer_address_state" as "cr_billing_customer_address_state",
    "wakeful"."cr_billing_customer_address_street_name" as "cr_billing_customer_address_street_name",
    "wakeful"."cr_billing_customer_address_street_number" as "cr_billing_customer_address_street_number",
    "wakeful"."cr_billing_customer_address_street_type" as "cr_billing_customer_address_street_type",
    "wakeful"."cr_billing_customer_address_suite_number" as "cr_billing_customer_address_suite_number",
    "wakeful"."cr_billing_customer_address_zip" as "cr_billing_customer_address_zip",
    "wakeful"."cr_billing_customer_first_name" as "cr_billing_customer_first_name",
    "wakeful"."cr_billing_customer_last_name" as "cr_billing_customer_last_name",
    "wakeful"."cr_billing_customer_salutation" as "cr_billing_customer_salutation",
    "wakeful"."cr_billing_customer_text_id" as "cr_billing_customer_text_id"
FROM
    "questionable"
    INNER JOIN "wakeful" on "questionable"."cr_billing_customer_id" = "wakeful"."cr_billing_customer_id")
SELECT
    "yummy"."cr_billing_customer_text_id" as "cr_billing_customer_text_id",
    "yummy"."cr_billing_customer_salutation" as "cr_billing_customer_salutation",
    "yummy"."cr_billing_customer_first_name" as "cr_billing_customer_first_name",
    "yummy"."cr_billing_customer_last_name" as "cr_billing_customer_last_name",
    "yummy"."cr_billing_customer_address_street_number" as "cr_billing_customer_address_street_number",
    "yummy"."cr_billing_customer_address_street_name" as "cr_billing_customer_address_street_name",
    "yummy"."cr_billing_customer_address_street_type" as "cr_billing_customer_address_street_type",
    "yummy"."cr_billing_customer_address_suite_number" as "cr_billing_customer_address_suite_number",
    "yummy"."cr_billing_customer_address_city" as "cr_billing_customer_address_city",
    "yummy"."cr_billing_customer_address_county" as "cr_billing_customer_address_county",
    "yummy"."cr_billing_customer_address_state" as "cr_billing_customer_address_state",
    "yummy"."cr_billing_customer_address_zip" as "cr_billing_customer_address_zip",
    "yummy"."cr_billing_customer_address_country" as "cr_billing_customer_address_country",
    "yummy"."cr_billing_customer_address_gmt_offset" as "cr_billing_customer_address_gmt_offset",
    "yummy"."cr_billing_customer_address_location_type" as "cr_billing_customer_address_location_type",
    "yummy"."customer_state" as "customer_state"
FROM
    "yummy"
    LEFT OUTER JOIN "juicy" on "yummy"."cr_return_address_state" is not distinct from "juicy"."cr_return_address_state"
WHERE
    "yummy"."customer_state" > 1.2 * "juicy"."_virt_agg_avg_7052944147524274"

ORDER BY 
    "yummy"."cr_billing_customer_text_id" asc nulls first,
    "yummy"."cr_billing_customer_salutation" asc nulls first,
    "yummy"."cr_billing_customer_first_name" asc nulls first,
    "yummy"."cr_billing_customer_last_name" asc nulls first,
    "yummy"."cr_billing_customer_address_street_number" asc nulls first,
    "yummy"."cr_billing_customer_address_street_name" asc nulls first,
    "yummy"."cr_billing_customer_address_street_type" asc nulls first,
    "yummy"."cr_billing_customer_address_suite_number" asc nulls first,
    "yummy"."cr_billing_customer_address_city" asc nulls first,
    "yummy"."cr_billing_customer_address_county" asc nulls first,
    "yummy"."cr_billing_customer_address_state" asc nulls first,
    "yummy"."cr_billing_customer_address_zip" asc nulls first,
    "yummy"."cr_billing_customer_address_country" asc nulls first,
    "yummy"."cr_billing_customer_address_gmt_offset" asc nulls first,
    "yummy"."cr_billing_customer_address_location_type" asc nulls first,
    "yummy"."customer_state" asc nulls first
LIMIT (100)
```
