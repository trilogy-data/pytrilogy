# Query 81

**Status:** `gen_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | FAILED |
| reference execution | OK (100 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 0 | 0 |
| reference | 6504 | 111 |

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

_v4 did not produce SQL._

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

## v4 generation error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 114, in generate_v4_sql
    info, build_env, _, build_stmt = run_tpcds_query(query_id)
                                     ~~~~~~~~~~~~~~~^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4.py", line 470, in run_tpcds_query
    info = search_concepts(
        mandatory_list=list(build_stmt.output_components),
    ...<4 lines>...
        conditions=[conditions] if conditions else [],
    )
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\concept_strategies_v4.py", line 92, in search_concepts
    result = _search_concepts(
        mandatory_list,
    ...<5 lines>...
        conditions=conditions,
    )
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\concept_strategies_v4.py", line 58, in _search_concepts
    strategy_node = build_strategy_node(
        group_graph, mandatory_list, environment, g, history
    )
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\strategy_builder.py", line 321, in build_strategy_node
    return _assemble_final_node(group_graph, built, mandatory_list, environment)
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\strategy_builder.py", line 246, in _assemble_final_node
    parents.extend(_wrap_root_for_grain(node, per_group[gid], environment))
                   ~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\strategy_builder.py", line 205, in _wrap_root_for_grain
    GroupNode(
    ~~~~~~~~~^
        output_concepts=outputs,
        ^^^^^^^^^^^^^^^^^^^^^^^^
    ...<2 lines>...
        parents=[root_node],
        ^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\nodes\group_node.py", line 52, in __init__
    super().__init__(
    ~~~~~~~~~~~~~~~~^
        input_concepts=input_concepts,
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<13 lines>...
        ordering=ordering,
        ^^^^^^^^^^^^^^^^^^
    )
    ^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\nodes\base_node.py", line 188, in __init__
    self.validate_inputs()
    ~~~~~~~~~~~~~~~~~~~~^^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\nodes\base_node.py", line 221, in validate_inputs
    raise ValueError(
        f"Invalid input concepts to node! {missing} are missing non-hidden parent nodes; have {non_hidden} and hidden {hidden} from root {usable_outputs}"
    )
ValueError: Invalid input concepts to node! ['cr.customer.address.id'] are missing non-hidden parent nodes; have {'cr.customer.address.street_number', 'cr.customer.address.gmt_offset', 'cr.customer.text_id', 'cr.customer.address.street_type', 'cr.return_address.state', 'cr.customer.address.city', 'cr.date.year', 'cr.return_amt_inc_tax', 'cr.customer.address.suite_number', 'cr.customer.address.street_name', 'cr.customer.address.zip', 'cr.customer.first_name', 'cr.customer.last_name', 'cr.customer.address.country', 'cr.customer.id', 'cr.customer.address.state', 'cr.customer.address.county', 'cr.customer.salutation', 'cr.customer.address.location_type'} and hidden set() from root {'cr.customer.address.street_number', 'cr.customer.address.gmt_offset', 'cr.customer.text_id', 'cr.customer.address.street_type', 'cr.return_address.state', 'cr.customer.address.city', 'cr.date.year', 'cr.return_amt_inc_tax', 'cr.customer.address.suite_number', 'cr.customer.address.street_name', 'cr.customer.address.zip', 'cr.customer.first_name', 'cr.customer.last_name', 'cr.customer.address.country', 'cr.customer.id', 'cr.customer.address.state', 'cr.customer.address.county', 'cr.customer.salutation', 'cr.customer.address.location_type'}
```
