# Trilogy failure analysis — 20260811-124039

- Run `20260811-124031_enriched_aggregates` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 213 | failed: 6 (3%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `disabled-tool` | 2 | 33% |
| `other` | 2 | 33% |
| `type-error` | 1 | 17% |
| `undefined-concept` | 1 | 17% |

## Detail

### `disabled-tool`

- `trilogy file read answer_883027685.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `other`

- `trilogy run answer_4077069387.preql`

  ```text
  Syntax error in answer_4077069387.preql: Cannot compare DATE (ref:ws.sale_date.date) and STRING (1999-02-22) of different types with operator >= in ref:ws.sale_date.date >= 1999-02-22
  ```
- `trilogy run answer_2524943990.preql`

  ```text
  Unexpected error in answer_2524943990.preql: (_duckdb.BinderException) Binder Error: aggregate function calls cannot be nested

  LINE 42:     count(max(CASE WHEN ("catalog_catalog_returns"."CR_ORDER_NUMBER...
                     ^
  [SQL:
  WITH
  juicy as (
  SELECT
      "catalog_catalog_sales"."CS_ORDER_NUMBER" as "catalog_order_number",
      "catalog_catalog_sales"."CS_WAREHOUSE_SK" as "catalog_warehouse_sk"
  FROM
      "fact_catalog_sales" as "catalog_catalog_sales"
  GROUP BY
      1,
      2),
  vacuous as (
  SELECT
      "juicy"."catalog_order_number" as "catalog_order_number"
  FROM
      "juicy"
  GROUP BY
      1
  HAVING
      count(distinct "juicy"."catalog_warehouse_sk") > 1
  ),
  concerned as (
  SELECT
      "vacuous"."catalog_order_number" as "multi_wh_catalog_order_number"
  FROM
      "vacuous"),
  abundant as (
  SELECT
      "catalog_catalog_sales"."CS_ORDER_NUMBER" as "catalog_order_number"
  FROM
      "fact_catalog_sales" as "catalog_catalog_sales"
  GROUP BY
      1),
  cooperative as (
  SELECT
      "catalog_catalog_returns"."CR_ORDER_NUMBER" as "catalog_order_number"
  FROM
      "fact_catalog_returns" as "catalog_catalog_returns"
  GROUP BY
      1
  HAVING
      count(max(CASE WHEN ("catalog_catalog_returns"."CR_ORDER_NUMBER" is not null) = True THEN "catalog_catalog_returns"."CR_ORDER_NUMBER" ELSE NULL END)) = 0
  ),
  uneven as (
  SELECT
      "abundant"."catalog_order_number" as "no_ret_catalog_order_number"
  FROM
      "abundant"
      INNER JOIN "cooperative" on "abundant"."catalog_order_number" = "cooperative"."catalog_order_number"),
  thoughtful as (
  SELECT
      "catalog_catalog_sales"."CS_EXT_SHIP_COST" as "catalog_ext_ship_cost",
      "catalog_catalog_sales"."CS_NET_PROFIT" as "catalog_net_profit",
      "catalog_catalog_sales"."CS_ORDER_NUMBER" as "catalog_order_number"
  FROM
      "fact_catalog_sales" as "catalog_catalog_sales"
      INNER JOIN "dim_date_dim" as "catalog_ship_date_date" on "catalog_catalog_sales"."CS_SHIP_DATE_SK" = "catalog_ship_date_date"."D_DATE_SK"
      INNER JOIN "dim_call_center" as "catalog_call_center_call_center" on "catalog_catalog_sales"."CS_CALL_CENTER_SK" = "catalog_call_center_call_center"."CC_CALL_CENTER_SK"
      INNER JOIN "dim_customer_address" as "catalog_pos_ship_address_customer_address" on "catalog_catalog_sales"."CS_SHIP_ADDR_SK" = "catalog_pos_ship_address_customer_address"."CA_ADDRESS_SK"
  WHERE
      cast("catalog_ship_date_date"."D_DATE" as date) BETWEEN date '2002-02-01' AND date '2002-04-02' and "catalog_pos_ship_address_customer_address"."CA_STATE" = 'GA' and "catalog_call_center_call_center"."CC_COUNTY" = 'Williamson County' and exists (select 1 from concerned where concerned."multi_wh_catalog_order_number" is not distinct from "catalog_catalog_sales"."CS_ORDER_NUMBER") and exists (select 1 from uneven where uneven."no_ret_catalog_order_number" is not distinct from "catalog_catalog_sales"."CS_ORDER_NUMBER")
  )
  SELECT
      count(distinct "thoughtful"."catalog_order_number") as "order_cnt",
      sum("thoughtful"."catalog_ext_ship_cost") as "total_ship_cost",
      sum("thoughtful"."catalog_net_profit") as "total_net_profit"
  FROM
      "thoughtful"
  WHERE
      exists (select 1 from concerned where concerned."multi_wh_catalog_order_number" is not distinct from "thoughtful"."catalog_order_number") and exists (select 1 from uneven where uneven."no_ret_catalog_order_number" is not distinct from "thoughtful"."catalog_order_number")

  ORDER BY
      "order_cnt" desc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```

### `type-error`

- `trilogy run answer_2524943990.preql`

  ```text
  Syntax error in answer_2524943990.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```

### `undefined-concept`

- `trilogy run answer_2133330107.preql`

  ```text
  Syntax error in answer_2133330107.preql: 2 undefined concept references; fix all before re-running:
    - local.brand_id (line 14, col 24, in ORDER BY); did you mean: ss.item.brand_id?
    - local.manufacturer_id (line 14, col 34, in ORDER BY); did you mean: ss.item.manufacturer_id, manufacturer_name?
  ```
