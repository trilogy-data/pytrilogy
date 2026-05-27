# Query 84

**Status:** `gen_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | FAILED |
| reference execution | OK (16 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 0 | 0 | — |
| reference | 1772 | 20 | 27.60 ms |

## Preql

```
import customer as customer;
import store_returns as returns;

# sr_cdemo_sk = cd_demo_sk in the reference; cross-table value join.
merge customer.demographics.id into returns.customer_demographic.id;

where
    customer.address.city = 'Edgewood'
    and returns.customer_demographic.id is not null
    and customer.household_demographic.id is not null
    and customer.household_demographic.income_band.lower_bound >= 38128
    and customer.household_demographic.income_band.upper_bound <= 38128 + 50000
select
    customer.text_id,
    concat(coalesce(customer.last_name, ''), ', ', coalesce(customer.first_name, '')) as customername,
    --returns.store_sales.ticket_number,
    --returns.item.id,
order by
    customer.text_id asc nulls first
limit 100
;
```

## v4 generated SQL

_v4 did not produce SQL._

## Reference SQL (zquery log)

```sql
SELECT
    "customer_customers"."C_CUSTOMER_ID" as "customer_text_id",
    (coalesce("customer_customers"."C_LAST_NAME",'') || ', ' || coalesce("customer_customers"."C_FIRST_NAME",'')) as "customername"
FROM
    "memory"."customer_address" as "customer_address_customer_address"
    INNER JOIN "memory"."customer" as "customer_customers" on "customer_address_customer_address"."CA_ADDRESS_SK" = "customer_customers"."C_CURRENT_ADDR_SK"
    INNER JOIN "memory"."store_returns" as "returns_store_returns" on "customer_customers"."C_CURRENT_CDEMO_SK" is not distinct from "returns_store_returns"."SR_CDEMO_SK"
    INNER JOIN "memory"."household_demographics" as "customer_household_demographic_household_demographics" on "customer_customers"."C_CURRENT_HDEMO_SK" = "customer_household_demographic_household_demographics"."HD_DEMO_SK"
    INNER JOIN "memory"."income_band" as "customer_household_demographic_income_band_income_band" on "customer_household_demographic_household_demographics"."HD_INCOME_BAND_SK" = "customer_household_demographic_income_band_income_band"."IB_INCOME_BAND_SK"
WHERE
    "customer_address_customer_address"."CA_CITY" = 'Edgewood' and coalesce("customer_customers"."C_CURRENT_CDEMO_SK","returns_store_returns"."SR_CDEMO_SK") is not null and coalesce("customer_customers"."C_CURRENT_HDEMO_SK","customer_household_demographic_household_demographics"."HD_DEMO_SK") is not null and "customer_household_demographic_income_band_income_band"."IB_LOWER_BOUND" >= 38128 and "customer_household_demographic_income_band_income_band"."IB_UPPER_BOUND" <= 38128 + 50000

GROUP BY
    1,
    2,
    "returns_store_returns"."SR_ITEM_SK",
    "returns_store_returns"."SR_TICKET_NUMBER"
ORDER BY 
    "customer_customers"."C_CUSTOMER_ID" asc nulls first
LIMIT (100)
```

## v4 generation error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 132, in generate_v4_sql
    info, build_env, _, build_stmt = run_tpcds_query(query_id)
                                     ~~~~~~~~~~~~~~~^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4.py", line 469, in run_tpcds_query
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
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\strategy_builder.py", line 514, in build_strategy_node
    return _assemble_final_node(group_graph, built, mandatory_list, environment)
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\strategy_builder.py", line 383, in _assemble_final_node
    for o in p.output_concepts:
                       ^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\strategy_builder.py", line 308, in _wrap_for_grain
    GroupNode(
    ~~~~~~~~~^
        output_concepts=outputs,
        ^^^^^^^^^^^^^^^^^^^^^^^^
    ...<2 lines>...
        parents=[parent_node],
        ^^^^^^^^^^^^^^^^^^^^^^
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
ValueError: Invalid input concepts to node! ['customer.id'] are missing non-hidden parent nodes; have {'customer.last_name', 'returns.customer_demographic.id', 'customer.text_id', 'returns.store_sales.ticket_number', 'returns.item.id', 'customer.household_demographic.income_band.upper_bound', 'customer.demographics.id', 'customer.household_demographic.income_band.lower_bound', 'customer.household_demographic.id', 'customer.address.city', 'customer.first_name'} and hidden set() from root {'customer.last_name', 'returns.customer_demographic.id', 'customer.text_id', 'returns.store_sales.ticket_number', 'returns.item.id', 'customer.household_demographic.income_band.upper_bound', 'customer.household_demographic.income_band.lower_bound', 'customer.household_demographic.id', 'customer.address.city', 'customer.first_name'}
```
