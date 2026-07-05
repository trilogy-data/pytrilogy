# Trilogy failure analysis — 20260705-002825

- Run `20260705-002825` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 325 | failed: 34 (10%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 18 | 53% |
| `syntax-parse` | 12 | 35% |
| `planner-recursion` | 3 | 9% |
| `cli-misuse` | 1 | 3% |

## Detail

### `other`

- `trilogy file read query11.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query31.preql`

  ```text
  Resolution error in query31.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {local.ss_q1_ext_sales, local.ss_q2_ext_sales, local.ss_q3_ext_sales, local.ws_q1_ext_sales, local.ws_q2_ext_sales, local.ws_q3_ext_sales, ss.sale_address.county, ws.bill_address.county}
  ```
- `trilogy run query31.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 2098 (char 2097). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {best_performers.items_above_threshold.ss.item.product_name, best_performers.rank, worst_performers.items_above_threshold.ss.item.product_name}
  ```
- `trilogy run query44.preql`

  ```text
  Unexpected error in query44.preql: (_duckdb.BinderException) Binder Error: column "ss_item_id" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(ss_item_id)" if the exact value of "ss_item_id" is not important.

  LINE 23: ..."."threshold" and "questionable"."ss_store_id" = 1 THEN "questionable"."ss_item_id" ELSE NULL END as "_virt_filter_...
                                                                      ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
      avg(CASE WHEN "ss_store_sales"."SS_ADDR_SK" is null THEN "ss_store_sales"."SS_NET_PROFIT" ELSE NULL END) as "null_addr_avg_net_profit"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1),
  questionable as (
  SELECT
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
      "ss_store_sales"."SS_NET_PROFIT" as "ss_net_profit",
      "ss_store_sales"."SS_STORE_SK" as "ss_store_id",
      "thoughtful"."null_addr_avg_net_profit" * 0.9 as "threshold"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "thoughtful" on "ss_store_sales"."SS_ITEM_SK" = "thoughtful"."ss_item_id"),
  abundant as (
  SELECT
      "questionable"."ss_store_id" as "ss_store_id",
      CASE WHEN avg("questionable"."ss_net_profit") > "questionable"."threshold" and "questionable"."ss_store_id" = 1 THEN "questionable"."ss_item_id" ELSE NULL END as "_virt_filter_id_7691558876961924"
  FROM
      "questionable"
  GROUP BY
      1,
      "questionable"."threshold"
  HAVING
      avg("questionable"."ss_net_profit") > "questionable"."threshold"
  ),
  uneven as (
  SELECT
      "abundant"."_virt_filter_id_7691558876961924" as "_virt_filter_id_7691558876961924"
  FROM
      "abundant"
  WHERE
      "abundant"."ss_store_id" = 1
  ),
  highfalutin as (
  SELECT
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
      avg("ss_store_sales"."SS_NET_PROFIT") as "avg_net_profit_per_item"
  FROM
      "store_sales" as "ss_store_sales"
  WHERE
      "ss_store_sales"."SS_STORE_SK" = 1 and "ss_store_sales"."SS_ITEM_SK" in (select uneven."_virt_filter_id_7691558876961924" from uneven where uneven."_virt_filter_id_7691558876961924" is not null)

  GROUP BY
      1),
  yummy as (
  SELECT
      "highfalutin"."avg_net_profit_per_item" as "avg_net_profit_per_item",
      "ss_item_items"."I_PRODUCT_NAME" as "ss_item_product_name"
  FROM
      "highfalutin"
      INNER JOIN "item" as "ss_item_items" on "highfalutin"."ss_item_id" = "ss_item_items"."I_ITEM_SK"
  WHERE
      "ss_item_items"."I_ITEM_SK" in (select uneven."_virt_filter_id_7691558876961924" from uneven where uneven."_virt_filter_id_7691558876961924" is not null)
  ),
  juicy as (
  SELECT
      "yummy"."ss_item_product_name" as "qualifying_items_ss_item_product_name",
      dense_rank() over (order by "yummy"."avg_net_profit_per_item" asc ) as "asc_rank",
      dense_rank() over (order by "yummy"."avg_net_profit_per_item" desc ) as "desc_rank"
  FROM
      "yummy"),
  concerned as (
  SELECT
      "juicy"."asc_rank" as "ranked_asc_rank",
      "juicy"."desc_rank" as "ranked_desc_rank",
      "juicy"."qualifying_items_ss_item_product_name" as "ranked_qualifying_items_ss_item_product_name"
  FROM
      "juicy"),
  abhorrent as (
  SELECT
      "concerned"."ranked_desc_rank" as "_worst_pair_rank",
      "concerned"."ranked_qualifying_items_ss_item_product_name" as "_worst_worst_product_name"
  FROM
      "concerned"
  WHERE
      "concerned"."ranked_desc_rank" <= 10

  GROUP BY
      1,
      2),
  young as (
  SELECT
      "concerned"."ranked_asc_rank" as "_best_pair_rank",
      "concerned"."ranked_qualifying_items_ss_item_product_name" as "_best_best_product_name"
  FROM
      "concerned"
  WHERE
      "concerned"."ranked_asc_rank" <= 10

  GROUP BY
      1,
      2),
  sweltering as (
  SELECT
      "abhorrent"."_worst_pair_rank" as "worst_pair_rank",
      "abhorrent"."_worst_worst_product_name" as "worst_worst_product_name"
  FROM
      "abhorrent"),
  sparkling as (
  SELECT
      "young"."_best_best_product_name" as "best_best_product_name",
      "young"."_best_pair_rank" as "best_pair_rank"
  FROM
      "young")
  SELECT
      coalesce("sparkling"."best_pair_rank","sweltering"."worst_pair_rank") as "rank",
      "sparkling"."best_best_product_name" as "best_performer_product_name",
      "sweltering"."worst_worst_product_name" as "worst_performer_product_name"
  FROM
      "sweltering"
      FULL JOIN "sparkling" on "sweltering"."worst_pair_rank" = "sparkling"."best_pair_rank"
  ORDER BY
      "rank" asc,
      "best_performer_product_name" desc,
      "worst_performer_product_name" desc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query49.preql`

  ```text
  Syntax error in query49.preql: Output column 'return_ratio' renames 'local.return_ratio' back to the name of an existing concept 'return_ratio' (defined at line 3) that 'local.return_ratio' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'return_ratio_out').
  ```
- `trilogy run query49.preql`

  ```text
  Unexpected error in query49.preql: (_duckdb.BinderException) Binder Error: column "sales_channel" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(sales_channel)" if the exact value of "sales_channel" is not important.

  LINE 120:     LOWER("vacuous"."sales_channel")  asc nulls first,
                      ^
  [SQL:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
      "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
      "sales_catalog_returns_unified"."CR_RETURN_AMOUNT" as "sales_return_amount",
      "sales_catalog_returns_unified"."CR_RETURN_QUANTITY" as "sales_return_quantity"
  FROM
      "catalog_returns" as "sales_catalog_returns_unified"
  WHERE
      "sales_catalog_returns_unified"."CR_RETURN_AMOUNT" > 10000

  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
      "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
      "sales_store_returns_unified"."SR_RETURN_AMT" as "sales_return_amount",
      "sales_store_returns_unified"."SR_RETURN_QUANTITY" as "sales_return_quantity"
  FROM
      "store_returns" as "sales_store_returns_unified"
  WHERE
      "sales_store_returns_unified"."SR_RETURN_AMT" > 10000

  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
      "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
      "sales_web_returns_unified"."WR_RETURN_AMT" as "sales_return_amount",
      "sales_web_returns_unified"."WR_RETURN_QUANTITY" as "sales_return_quantity"
  FROM
      "web_returns" as "sales_web_returns_unified"
  WHERE
      "sales_web_returns_unified"."WR_RETURN_AMT" > 10000
  ),
  abundant as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
      "sales_catalog_sales_unified"."CS_NET_PAID" as "sales_net_paid",
      "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
      "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity"
  FROM
      "catalog_sales" as "sales_catalog_sales_unified"
      INNER JOIN "date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
  WHERE
      "sales_catalog_sales_unified"."CS_NET_PROFIT" > 1 and "sales_catalog_sales_unified"."CS_NET_PAID" > 0 and "sales_catalog_sales_unified"."CS_QUANTITY" > 0 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 12

  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
      "sales_store_sales_unified"."SS_NET_PAID" as "sales_net_paid",
      "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
      "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity"
  FROM
      "store_sales" as "sales_store_sales_unified"
      INNER JOIN "date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
  WHERE
      "sales_store_sales_unified"."SS_NET_PROFIT" > 1 and "sales_store_sales_unified"."SS_NET_PAID" > 0 and "sales_store_sales_unified"."SS_QUANTITY" > 0 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 12

  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
      "sales_web_sales_unified"."WS_NET_PAID" as "sales_net_paid",
      "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
      "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity"
  FROM
      "web_sales" as "sales_web_sales_unified"
      INNER JOIN "date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
  WHERE
      "sales_web_sales_unified"."WS_NET_PROFIT" > 1 and "sales_web_sales_unified"."WS_NET_PAID" > 0 and "sales_web_sales_unified"."WS_QUANTITY" > 0 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 12
  ),
  yummy as (
  SELECT
      "abundant"."sales_item_id" as "sales_item_id",
      "cheerful"."sales_channel" as "sales_channel",
      sum("abundant"."sales_net_paid") as "_virt_agg_sum_2775739316613904",
      sum("abundant"."sales_quantity") as "_virt_agg_sum_6186806241713450",
      sum("cheerful"."sales_return_amount") as "_virt_agg_sum_8056058741317745",
      sum(coalesce("cheerful"."sales_return_quantity",0)) as "_virt_agg_sum_4057596625194597"
  FROM
      "abundant"
      INNER JOIN "cheerful" on "abundant"."sales_channel" = "cheerful"."sales_channel" AND "abundant"."sales_item_id" = "cheerful"."sales_item_id" AND "abundant"."sales_order_id" = "cheerful"."sales_order_id"
  GROUP BY
      1,
      2),
  vacuous as (
  SELECT
      "yummy"."_virt_agg_sum_4057596625194597" / "yummy"."_virt_agg_sum_6186806241713450" as "return_ratio",
      "yummy"."sales_channel" as "sales_channel",
      "yummy"."sales_item_id" as "item",
      LOWER("yummy"."sales_channel")  as "channel",
      rank() over (partition by "yummy"."sales_channel" order by "yummy"."_virt_agg_sum_4057596625194597" / "yummy"."_virt_agg_sum_6186806241713450" asc nulls first ) as "return_rank",
      rank() over (partition by "yummy"."sales_channel" order by "yummy"."_virt_agg_sum_8056058741317745" / "yummy"."_virt_agg_sum_2775739316613904" asc nulls first ) as "currency_rank"
  FROM
      "yummy")
  SELECT
      "vacuous"."channel" as "channel",
      "vacuous"."item" as "item",
      "vacuous"."return_ratio" as "return_ratio",
      "vacuous"."return_rank" as "return_rank",
      "vacuous"."currency_rank" as "currency_rank"
  FROM
      "vacuous"
  WHERE
      "vacuous"."return_rank" <= 10 or "vacuous"."currency_rank" <= 10

  GROUP BY
      1,
      2,
      3,
      4,
      5
  ORDER BY
      LOWER("vacuous"."sales_channel")  asc nulls first,
      "vacuous"."return_rank" asc nulls first,
      "vacuous"."currency_rank" asc nulls first,
      "vacuous"."item" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query57.preql`

  ```text
  Syntax error in query57.preql: Comparison `cs.sold_date.month_of_year between 1 and 12` matches every value of enum field 'cs.sold_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: ORDER BY references 'store_sales.store.id', which is not in the SELECT projection (line 9). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_sales.store.id order by store_sales.store.id asc`.
  ```
- `trilogy file read query65.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query83.preql`

  ```text
  Syntax error in query83.preql: Undefined concept: web.item.text_id. Suggestions: ['web.web_sales.item.text_id', 'store.store.date.text_id', 'store.store.text_id', 'store.item.text_id', 'store.return_date.text_id', 'store.time.text_id']
  ```
- `trilogy file read query83.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query97.preql`

  ```text
  Resolution error in query97.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {catalog_sales.bill_customer.id, catalog_sales.item.id, local.in_catalog, local.in_store, store_sales.customer.id, store_sales.item.id}
  ```

### `syntax-parse`

- `trilogy file write query06.preql --content import raw.store_sales as ss;

# average item current price by category (each item once, not weighted by volume)
a…s.customer.address.state
having line_item_count >= 10
order by line_item_count asc nulls first, ss.customer.address.state asc nulls first
limit 100;
`

  ```text
  refused to write 'query06.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...stomer.address.id is not null ??? group by ss.customer.address.s...

  Write stats: received 769 chars / 769 bytes; tail: …'first, ss.customer.address.state asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query27.preql -e -c import raw.store_sales as ss;

where
  ss.date.year = 2002
  and ss.store.state = 'TN'
  and ss.customer_demographic.m…avg_unit_price
by rollup (ss.item.text_id, ss.store.state)
order by grp_indicator, ss.item.text_id nulls first, ss.store.state nulls first
limit 100;`

  ```text
  refused to write 'query27.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:27
     |
  12 |   grouping(ss.item.text_id, ss.store.state) as grp_indicator,
     |                           ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...te,   grouping(ss.item.text_id ??? , ss.store.state) as grp_indic...

  Write stats: received 651 chars / 651 bytes; tail: …'m.text_id nulls first, ss.store.state nulls first\\nlimit 100;'.
  ```
- `trilogy file write query31.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;

# Compute per-county store sales by quarter in 2000
aut…ver_q2,
    ss_q3_q2 as store_q3_over_q2
union join ss_county = ws_county
having
    ws_q2_q1 > ss_q2_q1
    and ws_q3_q2 > ss_q3_q2
order by county;`

  ```text
  refused to write 'query31.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...0 and ws_q2 > 0 and ws_q3 > 0 ??? auto ws_q2_q1 <- ws_q2 / ws_q1...

  Write stats: received 1449 chars / 1449 bytes; tail: …'2_q1 > ss_q2_q1\\n    and ws_q3_q2 > ss_q3_q2\\norder by county;'.
  ```
- `trilogy file write query40.preql --content import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Filter to items with current_price between 0.…efunded_cash, 0) ? cs.sold_date.date >= '2000-03-11'::date) as after_20000311
order by
    cs.warehouse.state asc,
    cs.item.text_id asc
limit 100;`

  ```text
  refused to write 'query40.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:21
     |
  12 | subset join cs = cr on cs.order_number = cr.order_number, cs.item.id = cr.item.id
     |                     ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...not null  subset join cs = cr ??? on cs.order_number = cr.order_...

  Write stats: received 884 chars / 884 bytes; tail: …'  cs.warehouse.state asc,\\n    cs.item.text_id asc\\nlimit 100;'.
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Eight attribute profiles (category, color, units, size):
# 1. Books / tan / Oz / N/A
#…m.manufacturer_id between 1 and 500
  and item.manufact in profile_manufs
select distinct
  item.product_name
order by
  item.product_name
limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...fact in profile_manufs select ??? distinct   item.product_name o...

  Write stats: received 1716 chars / 1716 bytes; tail: …'\\n  item.product_name\\norder by\\n  item.product_name\\nlimit 100;'.
  ```
- `trilogy file write query70.preql -e -c import raw.store_sales as ss;

where ss.date.year = 2000
select
    ss.store.state,
    ss.store.county,
    sum(ss.ne…    hierarchy_level,
    rnk
order by
    hierarchy_level desc,
    case when hierarchy_level = 0 then ss.store.state end asc,
    rnk asc
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:37
    |
  9 |         when grouping(ss.store.state, ss.store.county) = 0 then 2
    |                                     ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
    when grouping(ss.store.state ??? , ss.store.county) = 0 then 2

  Write stats: received 885 chars / 885 bytes; tail: …'evel = 0 then ss.store.state end asc,\\n    rnk asc\\nlimit 100;'.
  ```
- `trilogy file write query83.preql --content import raw.store_returns as store;

# Find the week_seq containing 2000-06-30
where store.return_date.date = '2000-06-30'::date
select distinct store.return_date.week_seq as wk;`

  ```text
  refused to write 'query83.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...e = '2000-06-30'::date select ??? distinct store.return_date.wee...

  Write stats: received 177 chars / 177 bytes; tail: …"-30'::date\\nselect distinct store.return_date.week_seq as wk;".
  ```
- `trilogy file write query97.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

# Rowset 1: unique (customer, i…e 0 end) as both
union join store_set.store_cust_id = catalog_set.cat_cust_id
union join store_set.store_item_id = catalog_set.cat_item_id
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset store_set as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...from store sales in year 2000 ??? rowset store_set as where stor...

  Write stats: received 1139 chars / 1139 bytes; tail: …'store_set.store_item_id = catalog_set.cat_item_id\\nlimit 100;'.
  ```
- `trilogy file write query97_check7.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

rowset store_set <- wher…ique cust-item combos using the rowset
select
    store_set.store_cust_id as sid,
    store_set.store_item_id as siid,
    count(*) as cnt
limit 100;`

  ```text
  refused to write 'query97_check7.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...et.store_item_id as siid,     ??? count(*) as cnt limit 100;

  Write stats: received 394 chars / 394 bytes; tail: …'re_set.store_item_id as siid,\\n    count(*) as cnt\\nlimit 100;'.
  ```
- `trilogy file write query97.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

# Rowset 1: unique (customer, i…else 0 end) as both
union join store_set.store_cust_id = catalog_set.cat_cust_id
    and store_set.store_item_id = catalog_set.cat_item_id
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...ales.item.id as store_item_id ??? by store_sales.customer.id, st...

  Write stats: received 1236 chars / 1236 bytes; tail: …'store_set.store_item_id = catalog_set.cat_item_id\\nlimit 100;'.
  ```
- `trilogy file write query97.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

# Rowset 1: unique (customer, i…else 0 end) as both
union join store_set.store_cust_id = catalog_set.cat_cust_id
    and store_set.store_item_id = catalog_set.cat_item_id
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...les.item.id) as store_item_id ??? by store_sales.customer.id, st...

  Write stats: received 1264 chars / 1264 bytes; tail: …'store_set.store_item_id = catalog_set.cat_item_id\\nlimit 100;'.
  ```
- `trilogy file write query97_check23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

rowset store_set <- whe…cust_id) as cnt
union join hash(store_set.store_cust_id, store_set.store_item_id) = hash(catalog_set.cat_cust_id, catalog_set.cat_item_id)
limit 100;`

  ```text
  refused to write 'query97_check23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 18:42
     |
  18 | union join hash(store_set.store_cust_id, store_set.store_item_id) = hash(catalog_set.cat_cust_id, catalog_set.cat_item_id)
     |                                          ^---
     |
     = expected HASH_TYPE
  Location:
  ...hash(store_set.store_cust_id, ??? store_set.store_item_id) = has...

  Write stats: received 583 chars / 583 bytes; tail: …'catalog_set.cat_cust_id, catalog_set.cat_item_id)\\nlimit 100;'.
  ```

### `planner-recursion`

- `trilogy run query97_check14.preql`

  ```text
  Resolution error in query97_check14.preql: query could not be planned; this is a bug.
  ```
- `trilogy run query97.preql`

  ```text
  Resolution error in query97.preql: query could not be planned; this is a bug.
  ```
- `trilogy run query97.preql`

  ```text
  Resolution error in query97.preql: query could not be planned; this is a bug.
  ```

### `cli-misuse`

- `trilogy explore raw/sold_date.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sold_date.preql' does not exist.
  ```
