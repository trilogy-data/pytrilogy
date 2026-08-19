# Trilogy failure analysis — 20260818-022329

- Run `20260818-022328_ingest_openai_gpt-5.6-luna_effort-max` | `openai/gpt-5.6-luna` | sf=1
- `trilogy` calls: 1400 | failed: 174 (12%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `cli-misuse` | 129 | 74% |
| `other` | 23 | 13% |
| `undefined-concept` | 8 | 5% |
| `join-resolution` | 7 | 4% |
| `syntax-parse` | 4 | 2% |
| `type-error` | 1 | 1% |
| `no-output` | 1 | 1% |
| `planner-recursion` | 1 | 1% |

## Detail

### `cli-misuse`

- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root.store_sales`

  ```text
  Invalid value for 'PATH': File 'root.store_sales' does not exist.
  ```
- `trilogy explore root.catalog_sales`

  ```text
  Invalid value for 'PATH': File 'root.catalog_sales' does not exist.
  ```
- `trilogy explore root.web_sales`

  ```text
  Invalid value for 'PATH': File 'root.web_sales' does not exist.
  ```
- `trilogy explore root.store_returns`

  ```text
  Invalid value for 'PATH': File 'root.store_returns' does not exist.
  ```
- `trilogy explore root.catalog_returns`

  ```text
  Invalid value for 'PATH': File 'root.catalog_returns' does not exist.
  ```
- `trilogy explore root.web_returns`

  ```text
  Invalid value for 'PATH': File 'root.web_returns' does not exist.
  ```
- `trilogy explore root.orders`

  ```text
  Invalid value for 'PATH': File 'root.orders' does not exist.
  ```
- `trilogy explore root.store_sales`

  ```text
  Invalid value for 'PATH': File 'root.store_sales' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root.orders`

  ```text
  Invalid value for 'PATH': File 'root.orders' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql --reshow`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root.catalog_sales`

  ```text
  Invalid value for 'PATH': File 'root.catalog_sales' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root.store_sales`

  ```text
  Invalid value for 'PATH': File 'root.store_sales' does not exist.
  ```
- `trilogy explore root.store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'root.store_sales.preql' does not exist.
  ```
- `trilogy explore root.store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'root.store_returns.preql' does not exist.
  ```
- `trilogy explore root.catalog_sales.preql`

  ```text
  Invalid value for 'PATH': File 'root.catalog_sales.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root.catalog_sales`

  ```text
  Invalid value for 'PATH': File 'root.catalog_sales' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy agent-info syntax examples`

  ```text
  No such command 'examples'.
  ```
- `trilogy explore root.store_sales`

  ```text
  Invalid value for 'PATH': File 'root.store_sales' does not exist.
  ```
- `trilogy explore root.web_sales`

  ```text
  Invalid value for 'PATH': File 'root.web_sales' does not exist.
  ```
- `trilogy explore root.catalog_sales`

  ```text
  Invalid value for 'PATH': File 'root.catalog_sales' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root.inventory`

  ```text
  Invalid value for 'PATH': File 'root.inventory' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders`

  ```text
  Invalid value for 'PATH': File 'root/orders' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root.store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'root.store_sales.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root.store_sales`

  ```text
  Invalid value for 'PATH': File 'root.store_sales' does not exist.
  ```
- `trilogy explore root.catalog_sales`

  ```text
  Invalid value for 'PATH': File 'root.catalog_sales' does not exist.
  ```
- `trilogy explore root.web_sales`

  ```text
  Invalid value for 'PATH': File 'root.web_sales' does not exist.
  ```
- `trilogy explore root.store_returns`

  ```text
  Invalid value for 'PATH': File 'root.store_returns' does not exist.
  ```
- `trilogy explore root.catalog_returns`

  ```text
  Invalid value for 'PATH': File 'root.catalog_returns' does not exist.
  ```
- `trilogy explore root.web_returns`

  ```text
  Invalid value for 'PATH': File 'root.web_returns' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root.orders`

  ```text
  Invalid value for 'PATH': File 'root.orders' does not exist.
  ```
- `trilogy explore root.store_sales`

  ```text
  Invalid value for 'PATH': File 'root.store_sales' does not exist.
  ```
- `trilogy explore root.catalog_sales`

  ```text
  Invalid value for 'PATH': File 'root.catalog_sales' does not exist.
  ```
- `trilogy explore root.web_sales`

  ```text
  Invalid value for 'PATH': File 'root.web_sales' does not exist.
  ```
- `trilogy explore root.item`

  ```text
  Invalid value for 'PATH': File 'root.item' does not exist.
  ```
- `trilogy explore root.customer_address`

  ```text
  Invalid value for 'PATH': File 'root.customer_address' does not exist.
  ```
- `trilogy explore root.date_dim`

  ```text
  Invalid value for 'PATH': File 'root.date_dim' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders`

  ```text
  Invalid value for 'PATH': File 'root/orders' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root.store_sales`

  ```text
  Invalid value for 'PATH': File 'root.store_sales' does not exist.
  ```
- `trilogy explore root.catalog_sales`

  ```text
  Invalid value for 'PATH': File 'root.catalog_sales' does not exist.
  ```
- `trilogy explore root.web_sales`

  ```text
  Invalid value for 'PATH': File 'root.web_sales' does not exist.
  ```
- `trilogy explore root.orders`

  ```text
  Invalid value for 'PATH': File 'root.orders' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root.store_sales`

  ```text
  Invalid value for 'PATH': File 'root.store_sales' does not exist.
  ```
- `trilogy explore root.store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'root.store_sales.preql' does not exist.
  ```
- `trilogy explore root.catalog_sales.preql`

  ```text
  Invalid value for 'PATH': File 'root.catalog_sales.preql' does not exist.
  ```
- `trilogy explore root.catalog_returns.preql`

  ```text
  Invalid value for 'PATH': File 'root.catalog_returns.preql' does not exist.
  ```
- `trilogy explore root.store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'root.store_returns.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root.store_sales`

  ```text
  Invalid value for 'PATH': File 'root.store_sales' does not exist.
  ```
- `trilogy explore root.web_sales`

  ```text
  Invalid value for 'PATH': File 'root.web_sales' does not exist.
  ```
- `trilogy explore root.catalog_sales`

  ```text
  Invalid value for 'PATH': File 'root.catalog_sales' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root.web_sales.preql`

  ```text
  Invalid value for 'PATH': File 'root.web_sales.preql' does not exist.
  ```
- `trilogy explore root.catalog_sales.preql`

  ```text
  Invalid value for 'PATH': File 'root.catalog_sales.preql' does not exist.
  ```
- `trilogy explore root.store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'root.store_sales.preql' does not exist.
  ```
- `trilogy explore root.orders`

  ```text
  Invalid value for 'PATH': File 'root.orders' does not exist.
  ```
- `trilogy explore root.catalog_sales`

  ```text
  Invalid value for 'PATH': File 'root.catalog_sales' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root.orders`

  ```text
  Invalid value for 'PATH': File 'root.orders' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root.orders`

  ```text
  Invalid value for 'PATH': File 'root.orders' does not exist.
  ```
- `trilogy explore root.item`

  ```text
  Invalid value for 'PATH': File 'root.item' does not exist.
  ```
- `trilogy explore root.inventory`

  ```text
  Invalid value for 'PATH': File 'root.inventory' does not exist.
  ```
- `trilogy explore root.store_sales`

  ```text
  Invalid value for 'PATH': File 'root.store_sales' does not exist.
  ```
- `trilogy explore root.store_returns`

  ```text
  Invalid value for 'PATH': File 'root.store_returns' does not exist.
  ```
- `trilogy explore root.customer`

  ```text
  Invalid value for 'PATH': File 'root.customer' does not exist.
  ```
- `trilogy explore root.customer_address`

  ```text
  Invalid value for 'PATH': File 'root.customer_address' does not exist.
  ```
- `trilogy explore root.customer_demographics`

  ```text
  Invalid value for 'PATH': File 'root.customer_demographics' does not exist.
  ```
- `trilogy explore root.household_demographics`

  ```text
  Invalid value for 'PATH': File 'root.household_demographics' does not exist.
  ```
- `trilogy explore root.income_band`

  ```text
  Invalid value for 'PATH': File 'root.income_band' does not exist.
  ```
- `trilogy explore root.store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'root.store_returns.preql' does not exist.
  ```
- `trilogy explore root.customer_demographics.preql --show all`

  ```text
  Invalid value for 'PATH': File 'root.customer_demographics.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root.web_sales`

  ```text
  Invalid value for 'PATH': File 'root.web_sales' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```

### `other`

- `trilogy file write probe_lines.preql --run-and-delete`

  ```text
  Resolution error in probe_lines.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.item.item_at_ss_item_item_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_grouped_by_ss.date_dim.date_sk_ss.item.item_sk_at_ss_date_dim_date_sk_ss_item_item_sk_at_ss_date_dim_date_sk_ss_item_item_sk_filtered_by_9498251537808529_at_ss_item_item_sk_grouped_by_local.___tvf_arm_0_brand_id_local.___tvf_arm_0_category_id_local.___tvf_arm_0_class_id_at_local____tvf_arm_0_brand_id_local____tvf_arm_0_category_id_local____tvf_arm_0_class_id_at_local____tvf_arm_0_brand_id_local____tvf_arm_0_category_id_local____tvf_arm_0_class_id_local__common_tuples_brand_id_local__common_tuples_category_id_local__common_tuples_class_id_union_cs.catalog_sales_at_cs_item_item_sk_cs_order_number_grouped_by_cs.item.item_sk_cs.sold_date.date_sk_at_cs_item_item_sk_cs_sold_date_date_sk_join_cs.item.item_at_cs_item_item_sk_join_cs.sold_date.date_dim_at_cs_sold_date_date_sk_at_cs_item_item_sk_cs_sold_date_date_sk_filtered_by_2740678123651161_at_cs_item_item_sk_grouped_by_local.___tvf_arm_1_brand_id_local.___tvf_arm_1_category_id_local.___tvf_arm_1_class_id_at_local____tvf_arm_1_brand_id_local____tvf_arm_1_category_id_local____tvf_arm_1_class_id_at_local____tvf_arm_1_brand_id_local____tvf_arm_1_category_id_local____tvf_arm_1_class_id_local__common_tuples_brand_id_local__common_tuples_category_id_local__common_tuples_class_id_union_ws.item.item_at_ws_item_item_sk_join_ws.sold_date.date_dim_at_ws_sold_date_date_sk_join_ws.web_sales_at_ws_item_item_sk_ws_order_number_grouped_by_ws.item.item_sk_ws.sold_date.date_sk_at_ws_item_item_sk_ws_sold_date_date_sk_at_ws_item_item_sk_ws_sold_date_date_sk_filtered_by_7524390031337973_at_ws_item_item_sk_grouped_by_local.___tvf_arm_2_brand_id_local.___tvf_arm_2_category_id_local.___tvf_arm_2_class_id_at_local____tvf_arm_2_brand_id_local____tvf_arm_2_category_id_local____tvf_arm_2_class_id_at_local____tvf_arm_2_brand_id_local____tvf_arm_2_category_id_local____tvf_arm_2_class_id_local__common_tuples_brand_id_local__common_tuples_category_id_local__common_tuples_class_id_intersected_at_common_tuples_brand_id_common_tuples_category_id_common_tuples_class_id_join_ss.item.item_at_ss_item_item_sk_at_ss_item_item_sk onto ss.store_sales_at_ss_item_item_sk_ss_ticket_number_grouped_by_ss.list_price_ss.quantity_ss.ticket_number_at_ss_list_price_ss_quantity_ss_ticket_number_at_local____tvf_arm_0_amount_ss_ticket_number. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_complete.preql --run-and-delete`

  ```text
  Resolution error in probe_complete.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.item.item_at_ss_item_item_sk_at_ss_item_item_sk onto ss.store_sales_at_ss_item_item_sk_ss_ticket_number_grouped_by_ss.list_price_ss.quantity_ss.ticket_number_at_ss_list_price_ss_quantity_ss_ticket_number_at_local____tvf_arm_0_amount_ss_ticket_number. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe7661.preql --run-and-delete`

  ```text
  Unexpected error in probe7661.preql: Could not render the query: Missing source reference to cs.order_number; Missing source reference to cs.quantity. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  macho as (
  SELECT
      "sr_store_returns"."sr_item_sk" as "___tvf_arm_1_item_sk",
      "sr_store_returns"."sr_ticket_number" as "___tvf_arm_1_ticket_number",
      coalesce("sr_customer_customer"."c_customer_sk","sr_store_returns"."sr_customer_sk") as "___tvf_arm_1_customer_sk"
  FROM
      "store_returns" as "sr_store_returns"
      INNER JOIN "date_dim" as "sr_date_dim_date_dim" on "sr_store_returns"."sr_returned_date_sk" = "sr_date_dim_date_dim"."d_date_sk"
      INNER JOIN "customer" as "sr_customer_customer" on "sr_store_returns"."sr_customer_sk" = "sr_customer_customer"."c_customer_sk"
  WHERE
      ("sr_date_dim_date_dim"."d_year" is not null and "sr_date_dim_date_dim"."d_year" in (2001,2002)) and coalesce("sr_customer_customer"."c_customer_sk","sr_store_returns"."sr_customer_sk") is not null
  ),
  abhorrent as (
  SELECT
      "ss_store_sales"."ss_item_sk" as "___tvf_arm_0_item_sk",
      "ss_store_sales"."ss_ticket_number" as "___tvf_arm_0_ticket_number",
      coalesce("ss_customer_customer"."c_customer_sk","ss_store_sales"."ss_customer_sk") as "___tvf_arm_0_customer_sk"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_dim_date_dim" on "ss_store_sales"."ss_sold_date_sk" = "ss_date_dim_date_dim"."d_date_sk"
      INNER JOIN "customer" as "ss_customer_customer" on "ss_store_sales"."ss_customer_sk" = "ss_customer_customer"."c_customer_sk"
  WHERE
      "ss_date_dim_date_dim"."d_year" = 2001 and coalesce("ss_customer_customer"."c_customer_sk","ss_store_sales"."ss_customer_sk") is not null
  ),
  kaput as (
  SELECT
      "abhorrent"."___tvf_arm_0_item_sk" as "_return_keys_item_sk",
      "abhorrent"."___tvf_arm_0_ticket_number" as "_return_keys_ticket_number",
      "abhorrent"."___tvf_arm_0_customer_sk" as "_return_keys_customer_sk"
  FROM
      "abhorrent"
  INTERSECT
  SELECT
      "macho"."___tvf_arm_1_item_sk" as "_return_keys_item_sk",
      "macho"."___tvf_arm_1_ticket_number" as "_return_keys_ticket_number",
      "macho"."___tvf_arm_1_customer_sk" as "_return_keys_customer_sk"
  FROM
      "macho"),
  divergent as (
  SELECT
      "kaput"."_return_keys_customer_sk" as "return_keys_customer_sk",
      "kaput"."_return_keys_item_sk" as "return_keys_item_sk",
      "kaput"."_return_keys_ticket_number" as "return_keys_ticket_number"
  FROM
      "kaput"),
  highfalutin as (
  SELECT
      "cs_catalog_sales"."cs_bill_customer_sk" as "cs_bill_customer_customer_sk",
      "cs_catalog_sales"."cs_item_sk" as "cs_item_item_sk",
      "cs_catalog_sales"."cs_sold_date_sk" as "cs_sold_date_date_sk",
      coalesce("cs_catalog_sales"."cs_bill_customer_sk") as "_virt_presence_1406534026135109"
  FROM
      "catalog_sales" as "cs_catalog_sales"),
  wakeful as (
  SELECT
      "cs_catalog_sales"."cs_bill_customer_sk" as "cs_bill_customer_customer_sk",
      "cs_catalog_sales"."cs_item_sk" as "cs_item_item_sk",
      "cs_catalog_sales"."cs_sold_date_sk" as "cs_sold_date_date_sk"
  FROM
      "catalog_sales" as "cs_catalog_sales"
  WHERE
      "cs_catalog_sales"."cs_bill_customer_sk" is not null

  GROUP BY
      1,
      2,
      3),
  thoughtful as (
  SELECT
      "wakeful"."cs_item_item_sk" as "catalog_pairs_item_sk",
      coalesce("cs_bill_customer_customer"."c_customer_sk","wakeful"."cs_bill_customer_customer_sk") as "catalog_pairs_customer_sk"
  FROM
      "wakeful"
      INNER JOIN "date_dim" as "cs_sold_date_date_dim" on "wakeful"."cs_sold_date_date_sk" = "cs_sold_date_date_dim"."d_date_sk"
      INNER JOIN "customer" as "cs_bill_customer_customer" on "wakeful"."cs_bill_customer_customer_sk" = "cs_bill_customer_customer"."c_customer_sk"
  WHERE
      ("cs_sold_date_date_dim"."d_year" is not null and "cs_sold_date_date_dim"."d_year" in (2001,2002)) and coalesce("cs_bill_customer_customer"."c_customer_sk","wakeful"."cs_bill_customer_customer_sk") is not null

  GROUP BY
      1,
      2),
  rambunctious as (
  SELECT
      "ss_customer_customer"."c_customer_sk" as "_store_pairs_customer_sk",
      "ss_customer_customer"."c_customer_sk" as "store_pairs_customer_sk",
      "ss_item_item"."i_item_desc" as "store_pairs_item_desc",
      "ss_item_item"."i_item_id" as "store_pairs_item_id",
      "ss_item_item"."i_item_sk" as "store_pairs_item_sk",
      "ss_store_sales"."ss_ticket_number" as "_eligible_store_ticket_number",
      "ss_store_store"."s_state" as "store_pairs_store_state"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "item" as "ss_item_item" on "ss_store_sales"."ss_item_sk" = "ss_item_item"."i_item_sk"
      INNER JOIN "date_dim" as "ss_date_dim_date_dim" on "ss_store_sales"."ss_sold_date_sk" = "ss_date_dim_date_dim"."d_date_sk"
      INNER JOIN "customer" as "ss_customer_customer" on "ss_store_sales"."ss_customer_sk" = "ss_customer_customer"."c_customer_sk"
      LEFT OUTER JOIN "store" as "ss_store_store" on "ss_store_sales"."ss_store_sk" = "ss_store_store"."s_store_sk"
  WHERE
      "ss_date_dim_date_dim"."d_year" = 2001 and "ss_customer_customer"."c_customer_sk" is not null and exists (select 1 from divergent where divergent."return_keys_item_sk" is not distinct from "ss_item_item"."i_item_sk" and divergent."return_keys_ticket_number" is not distinct from "ss_store_sales"."ss_ticket_number" and divergent."return_keys_customer_sk" is not distinct from "ss_customer_customer"."c_customer_sk") and exists (select 1 from thoughtful where thoughtful."catalog_pairs_item_sk" is not distinct from "ss_item_item"."i_item_sk" and thoughtful."catalog_pairs_customer_sk" is not distinct from "ss_customer_customer"."c_customer_sk")

  GROUP BY
      2,
      3,
      4,
      5,
      6,
      7,
      "ss_date_dim_date_dim"."d_date_sk",
      "ss_date_dim_date_dim"."d_year",
      "ss_store_sales"."ss_quantity",
      "ss_store_store"."s_store_sk"),
  ceaseless as (
  SELECT
      count("rambunctious"."_eligible_store_ticket_number") as "store_n"
  FROM
      "rambunctious"),
  vast as (
  SELECT
      "highfalutin"."cs_item_item_sk" as "cs_item_item_sk",
      coalesce("cs_bill_customer_customer"."c_customer_sk","highfalutin"."cs_bill_customer_customer_sk") as "cs_bill_customer_customer_sk"
  FROM
      "highfalutin"
      INNER JOIN "date_dim" as "cs_sold_date_date_dim" on "highfalutin"."cs_sold_date_date_sk" = "cs_sold_date_date_dim"."d_date_sk"
      LEFT OUTER JOIN "customer" as "cs_bill_customer_customer" on "highfalutin"."cs_bill_customer_customer_sk" = "cs_bill_customer_customer"."c_customer_sk"
  WHERE
      ("cs_sold_date_date_dim"."d_year" is not null and "cs_sold_date_date_dim"."d_year" in (2001,2002)) and "highfalutin"."_virt_presence_1406534026135109" is not null
  ),
  cool as (
  SELECT
      "vast"."cs_bill_customer_customer_sk" as "cs_bill_customer_customer_sk",
      "vast"."cs_item_item_sk" as "cs_item_item_sk",
      "vast"."cs_item_item_sk" as "store_pairs_item_sk"
  FROM
      "vast"
  GROUP BY
      1,
      2),
  courageous as (
  SELECT
      "rambunctious"."store_pairs_customer_sk" as "cs_bill_customer_customer_sk",
      "rambunctious"."store_pairs_customer_sk" as "store_pairs_customer_sk",
      "rambunctious"."store_pairs_item_desc" as "_catalog_state_rows_item_desc",
      "rambunctious"."store_pairs_item_id" as "_catalog_state_rows_item_id",
      "rambunctious"."store_pairs_item_sk" as "_catalog_state_rows_item_sk",
      "rambunctious"."store_pairs_item_sk" as "store_pairs_item_sk",
      "rambunctious"."store_pairs_store_state" as "_catalog_state_rows_store_state"
  FROM
      "rambunctious"),
  wary as (
  SELECT
      INVALID_REFERENCE_BUG<Missing source reference to cs.order_number> as "_catalog_state_rows_order_number"
  FROM
      "courageous"
      LEFT OUTER JOIN "cool" on "courageous"."_catalog_state_rows_item_sk" = "cool"."store_pairs_item_sk" AND "courageous"."cs_bill_customer_customer_sk" is not distinct from "cool"."cs_bill_customer_customer_sk"
  WHERE
      coalesce("cool"."store_pairs_item_sk","courageous"."_catalog_state_rows_item_sk","courageous"."store_pairs_item_sk") is not null

  GROUP BY
      1,
      "courageous"."_catalog_state_rows_item_desc",
      "courageous"."_catalog_state_rows_item_id",
      "courageous"."_catalog_state_rows_store_state",
      INVALID_REFERENCE_BUG<Missing source reference to cs.quantity>,
      coalesce("cool"."cs_bill_customer_customer_sk","courageous"."cs_bill_customer_customer_sk"),
      coalesce("cool"."store_pairs_item_sk","courageous"."_catalog_state_rows_item_sk","courageous"."store_pairs_item_sk")),
  level as (
  SELECT
      count("wary"."_catalog_state_rows_order_number") as "catalog_n"
  FROM
      "wary"),
  yellow as (
  SELECT
      count("rambunctious"."_store_pairs_customer_sk") as "pair_n"
  FROM
      "rambunctious"),
  busy as (
  SELECT
      "sr_store_returns"."sr_ticket_number" as "_eligible_return_ticket_number"
  FROM
      "store_returns" as "sr_store_returns"
      INNER JOIN "date_dim" as "sr_date_dim_date_dim" on "sr_store_returns"."sr_returned_date_sk" = "sr_date_dim_date_dim"."d_date_sk"
      INNER JOIN "customer" as "sr_customer_customer" on "sr_store_returns"."sr_customer_sk" = "sr_customer_customer"."c_customer_sk"
  WHERE
      ("sr_date_dim_date_dim"."d_year" is not null and "sr_date_dim_date_dim"."d_year" in (2001,2002)) and "sr_customer_customer"."c_customer_sk" is not null and exists (select 1 from divergent where divergent."return_keys_item_sk" is not distinct from "sr_store_returns"."sr_item_sk" and divergent."return_keys_ticket_number" is not distinct from "sr_store_returns"."sr_ticket_number" and divergent."return_keys_customer_sk" is not distinct from "sr_customer_customer"."c_customer_sk") and exists (select 1 from thoughtful where thoughtful."catalog_pairs_item_sk" is not distinct from "sr_store_returns"."sr_item_sk" and thoughtful."catalog_pairs_customer_sk" is not distinct from "sr_customer_customer"."c_customer_sk")

  GROUP BY
      1,
      "sr_customer_customer"."c_customer_sk",
      "sr_date_dim_date_dim"."d_date_sk",
      "sr_date_dim_date_dim"."d_year",
      "sr_store_returns"."sr_item_sk",
      "sr_store_returns"."sr_return_quantity"),
  protective as (
  SELECT
      count("busy"."_eligible_return_ticket_number") as "return_n"
  FROM
      "busy")
  SELECT
      coalesce("ceaseless"."store_n",0) as "store_n",
      coalesce("protective"."return_n",0) as "return_n",
      coalesce("yellow"."pair_n",0) as "pair_n",
      coalesce("level"."catalog_n",0) as "catalog_n"
  FROM
      "protective"
      INNER JOIN "yellow" on 1=1
      INNER JOIN "level" on 1=1
      INNER JOIN "ceaseless" on 1=1
  ```
- `trilogy file write probe7666.preql --run-and-delete`

  ```text
  {
    "event": "write",
    "path": "probe7666.preql",
    "bytes": 1618
  }
  ```
- `trilogy file write probe_cust.preql --run-and-delete`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file write probe_candidate.preql --run-and-delete`

  ```text
  Resolution error in probe_candidate.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.date_dim.date_dim_at_sr_date_dim_date_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_at_sr_item_item_sk_sr_ticket_number_filtered_by_569209252849533_at_sr_item_item_sk_sr_ticket_number onto sr.customer.customer_at_sr_customer_customer_sk_at_local__sr_window_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_sr.preql --run-and-delete`

  ```text
  Resolution error in probe_sr.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.date_dim.date_dim_at_sr_date_dim_date_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_at_sr_item_item_sk_sr_ticket_number_filtered_by_569209252849533_at_sr_item_item_sk_sr_ticket_number onto sr.customer.customer_at_sr_customer_customer_sk_at_local__sr_window_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_ssagg.preql --run-and-delete`

  ```text
  Resolution error in probe_ssagg.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_filtered_by_9320673287308412_at_ss_item_item_sk_ss_ticket_number onto ss.customer.customer_at_ss_customer_customer_sk_join_ss.store.store_at_ss_store_store_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_grouped_by_ss.customer.customer_sk_ss.store.store_sk_at_ss_customer_customer_sk_ss_store_store_sk_at_ss_customer_customer_sk_ss_store_store_sk_at_local__ss_apr_customer_sk_local__ss_apr_store_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_sragg.preql --run-and-delete`

  ```text
  Resolution error in probe_sragg.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.date_dim.date_dim_at_sr_date_dim_date_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_at_sr_item_item_sk_sr_ticket_number_filtered_by_569209252849533_at_sr_item_item_sk_sr_ticket_number onto sr.customer.customer_at_sr_customer_customer_sk_at_local__sr_window_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_match_counts.preql --run-and-delete`

  ```text
  Syntax error in probe_match_counts.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 31). Move the filter to WHERE to filter before aggregation.
  ```
- `trilogy file write answer_4140546834.preql --run`

  ```text
  Syntax error in answer_4140546834.preql: ORDER BY references 'local.subgroup', which is not in the SELECT projection (line 10). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.subgroup order by local.subgroup asc`.
  ```
- `trilogy file write answer_2118989494.preql --run`

  ```text
  Syntax error in answer_2118989494.preql: Output column 'average_monthly_sales' renames 'local.average_monthly_sales' back to the name of an existing concept 'average_monthly_sales' (defined at line 4) that 'local.average_monthly_sales' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'average_monthly_sales_out').
  ```
- `trilogy file write answer_1809796058.preql --run`

  ```text
  Resolution error in answer_1809796058.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.item.item_at_ss_item_item_sk_at_local__sale_lines_item_sk onto ss.customer.customer_at_ss_customer_customer_sk_at_local__sale_lines_customer_sk, ss.store.store_at_ss_store_store_sk_at_ss_store_store_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write answer_71623752.preql --run`

  ```text
  Syntax error in answer_71623752.preql: Output column 'quarterly_total' renames 'local.quarterly_total' back to the name of an existing concept 'quarterly_total' (defined at line 3) that 'local.quarterly_total' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'quarterly_total_out').
  ```
- `trilogy file write answer_2986518257.preql --run`

  ```text
  Syntax error in answer_2986518257.preql: ORDER BY references 'local.signed_difference', which is not in the SELECT projection (line 24). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.signed_difference order by local.signed_difference asc`.
  ```
- `trilogy file write answer_1484301313.preql --run`

  ```text
  Syntax error in answer_1484301313.preql: Impossible comparison in ref:ss.promotion.channel_email = Y: 'Y' can never match a declared value of enum<'N'> — fix the constant, or update the enum declaration if the domain is stale
  ```
- `trilogy file write probe_catalog.preql --run-and-delete`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file write probe_store.preql --run-and-delete`

  ```text
  Resolution error in probe_store.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.item.item_at_ss_item_item_sk_at_local_item_sk onto ss.customer.customer_demographics.customer_demographics_at_ss_customer_customer_demographics_demo_sk_grouped_by_ss.customer.customer_demographics.marital_status_at_ss_customer_customer_demographics_marital_status_at_local_cur_marital, ss.customer.first_sales_date.date_dim_at_ss_customer_first_sales_date_date_sk_grouped_by_ss.customer.first_sales_date.year_at_ss_customer_first_sales_date_year_at_local_first_sales_year, ss.customer.first_shipto_date.date_dim_at_ss_customer_first_shipto_date_date_sk_grouped_by_ss.customer.first_shipto_date.year_at_ss_customer_first_shipto_date_year_at_local_first_shipto_year, ss.customer_address.customer_address_at_ss_customer_address_address_sk_grouped_by_ss.customer_address.city_ss.customer_address.street_name_ss.customer_address.street_number_ss.customer_address.zip_at_ss_customer_address_city_ss_customer_address_street_name_ss_customer_address_street_number_ss_customer_address_zip_at_local_cur_city_local_cur_street_name_local_cur_street_number_local_cur_zip. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_store_min.preql --run-and-delete`

  ```text
  Resolution error in probe_store_min.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.item.item_at_ss_item_item_sk_at_local_item_sk onto ss.customer.customer_demographics.customer_demographics_at_ss_customer_customer_demographics_demo_sk_grouped_by_ss.customer.customer_demographics.marital_status_at_ss_customer_customer_demographics_marital_status_at_local_cur_marital, ss.customer.first_sales_date.date_dim_at_ss_customer_first_sales_date_date_sk_grouped_by_ss.customer.first_sales_date.year_at_ss_customer_first_sales_date_year_at_local_first_sales_year, ss.customer.first_shipto_date.date_dim_at_ss_customer_first_shipto_date_date_sk_grouped_by_ss.customer.first_shipto_date.year_at_ss_customer_first_shipto_date_year_at_local_first_shipto_year. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write answer_1772060640.preql --run`

  ```text
  Syntax error in answer_1772060640.preql: Impossible comparison in SubselectComparison(left=ref:o.store.county, right=('Orange County', 'Bronx County', 'Franklin Parish', 'Williamson County'), operator=<ComparisonOperator.IN: 'in'>): 'Orange County' can never match a declared value of enum<'Williamson County'> — fix the constant, or update the enum declaration if the domain is stale
  ```
- `trilogy file write answer_1772060640.preql --run`

  ```text
  Syntax error in answer_1772060640.preql: ORDER BY references 'o.customer.customer_sk', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --o.customer.customer_sk order by o.customer.customer_sk asc`.
  ```
- `trilogy file write answer_1226264875.preql --run`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file write answer_840315271.preql --run`

  ```text
  Syntax error in answer_840315271.preql: Output column 'monthly_total' renames 'local.monthly_total' back to the name of an existing concept 'monthly_total' (defined at line 3) that 'local.monthly_total' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'monthly_total_out').
  ```

### `undefined-concept`

- `trilogy file write probe3863442186_fixed.preql --run-and-delete`

  ```text
  Syntax error in probe3863442186_fixed.preql: Undefined concept: c.ext.sales_price. Suggestions: ['c.ext_sales_price', 'c.sales_price', 'c.ext_list_price', 'o.sales_price', 'w.sales_price', 'w.ext_sales_price']
  ```
- `trilogy file write answer_3863442186.preql --run`

  ```text
  Syntax error in answer_3863442186.preql: Undefined concept: o.ext.sales_price. Suggestions: ['o.ext_sales_price', 'o.sales_price', 'o.ext_list_price', 'c.sales_price', 'w.sales_price', 'w.ext_sales_price']
  ```
- `trilogy file write probe_freq.preql --run-and-delete`

  ```text
  Syntax error in probe_freq.preql: Undefined concept: ss.sold_date.year. Suggestions: ['ss.date_dim.year', 'ss.store.date_dim.year', 'ss.promotion.end_date.year', 'ss.customer.first_sales_date.year', 'ss.customer.last_review_date.year', 'ss.promotion.start_date.year']
  ```
- `trilogy file write probe_freq2.preql --run-and-delete`

  ```text
  Syntax error in probe_freq2.preql: Undefined concept: ss.sold_date.date. Suggestions: ['ss.promotion.end_date.date', 'ss.promotion.end_date.date_sk', 'ss.promotion.end_date.date_id', 'ss.customer.first_sales_date.date', 'ss.customer.last_review_date.date', 'ss.promotion.start_date.date']
  ```
- `trilogy file write probe_date.preql --run-and-delete`

  ```text
  Syntax error in probe_date.preql: 5 undefined concept references; fix all before re-running:
    - d.date_dim.date_sk (line 4, col 5, in SELECT); did you mean: d.date_dim.date, d.date_dim.month_seq, d.date_dim.year, d.date_sk?
    - d.date_dim.date (line 5, col 5, in SELECT); did you mean: d.date_dim.date_sk, d.date_dim.year, d.date_dim.moy, d.date?
    - d.date_dim.year (line 6, col 5, in SELECT); did you mean: d.date_dim.moy, d.date_dim.date, d.date_dim.date_sk, d.year?
    - d.date_dim.moy (line 7, col 5, in SELECT); did you mean: d.date_dim.year, d.date_dim.month_seq, d.date_dim.date, d.moy?
    - d.date_dim.month_seq (line 8, col 5, in SELECT); did you mean: d.date_dim.moy, d.date_dim.date, d.date_dim.date_sk, d.month_seq?
  ```
- `trilogy file write probe_music.preql --run-and-delete`

  ```text
  Syntax error in probe_music.preql: 2 undefined concept references; fix all before re-running:
    - ss.sold_date.date (line 3, col 7, in WHERE); did you mean: ss.promotion.end_date.date, ss.promotion.end_date.date_sk, ss.promotion.end_date.date_id, ss.customer.first_sales_date.date, ss.customer.last_review_date.date, ss.promotion.start_date.date?
    - ss.sold_date.date (line 4, col 7, in WHERE); did you mean: ss.promotion.end_date.date, ss.promotion.end_date.date_sk, ss.promotion.end_date.date_id, ss.customer.first_sales_date.date, ss.customer.last_review_date.date, ss.promotion.start_date.date?
  ```
- `trilogy file write answer_2852230229.preql --run`

  ```text
  Syntax error in answer_2852230229.preql: Undefined concept: _virt_agg_grouping_164218295708188.
  ```
- `trilogy file write probe_rank.preql --run-and-delete`

  ```text
  Syntax error in probe_rank.preql: Undefined concept: _virt_agg_grouping_164218295708188.
  ```

### `join-resolution`

- `trilogy file write probe1858999935.preql --run-and-delete`

  ```text
  Resolution error in probe1858999935.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {catalog_sales}; {dow, web_sales, week_seq}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_store.preql --run-and-delete`

  ```text
  Resolution error in probe_store.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {losses, return_store, returns, sr.date_dim.date, sr.store.store_id}; {profit, sale_store, sales, ss.date_dim.date, ss.store.store_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_catalog.preql --run-and-delete`

  ```text
  Resolution error in probe_catalog.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {cr.catalog_page.catalog_page_id, cr.date_dim.date, losses, return_page, returns}; {cs.catalog_page.catalog_page_id, cs.sold_date.date, profit, sale_page, sales}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_web.preql --run-and-delete`

  ```text
  Resolution error in probe_web.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {losses, return_page_sk, returns, wr.date_dim.date, wr.web_page.web_page_sk}; {profit, sale_site, sales, ws.sold_date.date, ws.web_site.site_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_channels.preql --run-and-delete`

  ```text
  Resolution error in probe_channels.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 36). The requested concepts split into 2 disconnected subgraphs: {catalog_returns_by.returns, catalog_sales_by.outlet, catalog_sales_by.sales}; {web_outlet, web_returns, web_sales}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_2869182220.preql --run-and-delete`

  ```text
  Resolution error in probe_2869182220.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {c.customer_address.city, c.customer_demographics.demo_sk, c.customer_demographics.demo_sk, c.customer_sk, c.household_demographics.income_band.lower_bound, c.household_demographics.income_band.upper_bound, customer_code, full_name}; {sr.customer_demographics.demo_sk, sr.item.item_sk, sr.ticket_number}. Are you missing a join or merge statement to relate them?
  Note: the membership predicate(s) `(c.customer_demographics.demo_sk) in (sr.customer_demographics.demo_sk)` span these subgraphs, but membership only filters rows on its left side — it does not join the two sides, so it cannot relate them for outputs or grouping. To combine values from both sides, author a query-scoped join or a merge on shared keys.
  ```
- `trilogy file write answer_3562094594.preql --run`

  ```text
  Resolution error in answer_3562094594.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {_catalog_only_customer_sk}; {_catalog_only_item_sk}. Are you missing a join or merge statement to relate them?
  ```

### `syntax-parse`

- `trilogy file write probe7659.preql --run-and-delete`

  ```text
  refused to write 'probe7659.preql': not syntactically valid Trilogy.

  Parse error:
    --> 19:3
     |
  19 |   (where cs.sold_date.year in (2001, 2002)
     |   ^---
     |
     = expected select_statement, tvf_union_invocation, tvf_except_invocation, or tvf_intersect_invocation
  Location:
  ...;

   with catalog_pairs as
     ??? (where cs.sold_date.year in (2...
  ```
- `trilogy file write probe7665.preql --run-and-delete`

  ```text
  refused to write 'probe7665.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...sk = sr.customer.customer_sk
   ??? union join ss.item.item_sk = c...
  ```
- `trilogy file write probe_matched_agg.preql --run-and-delete`

  ```text
  refused to write 'probe_matched_agg.preql': not syntactically valid Trilogy.

  Parse error:
    --> 58:1
     |
  58 | select matched_store.item_code, matched_store.store_code, matched_store.store_name, sum(matched_store.store_profit) as store_profit, sum(matched_store.return_loss) as return_loss
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...ed_store.item_sk is not null
   ??? select matched_store.item_code...
  ```
- `trilogy file write answer_1197120511.preql --run`

  ```text
  refused to write 'answer_1197120511.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:1
     |
  12 | order by total_ext_discount desc;
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ..._amt) as total_ext_discount;
   ??? order by total_ext_discount de...
  ```

### `type-error`

- `trilogy file write probe_union.preql --run-and-delete`

  ```text
  Type error in probe_union.preql: Invalid argument type 'BIGINT' passed into CONCAT function in position 2 from concept: wr.web_page.web_page_sk. Valid: 'STRING'.
  ```

### `no-output`

- `trilogy file write probe7656.preql --run-and-delete`

  ```text
  Nothing was executed: parsed 2 definition statement(s) (1 import, 1 rowset) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```

### `planner-recursion`

- `trilogy file write probe7658.preql --run-and-delete`

  ```text
  Resolution error in probe7658.preql: query could not be planned; this is a bug.
  ```
