# Trilogy failure analysis — 20260627-131753

- Run `20260627-131753` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 222 | failed: 20 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 9 | 45% |
| `syntax-parse` | 9 | 45% |
| `syntax-missing-alias` | 1 | 5% |
| `join-resolution` | 1 | 5% |

## Detail

### `other`

- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 69 column 13 (char 2770). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg', which is not in the SELECT projection (line 24). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query17.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Unexpected error in query64.preql: Invalid reference string found in query:
  WITH
  quizzical as (
  SELECT
      sum(( coalesce("cr_catalog_returns"."CR_REFUNDED_CASH",0) + coalesce("cr_catalog_returns"."CR_REVERSED_CHARGE",0) ) + coalesce("cr_catalog_returns"."CR_STORE_CREDIT",0)) as "_catalog_item_refunds_cat_refund_total"
  FROM
      "catalog_returns" as "cr_catalog_returns"
  GROUP BY
      "cr_catalog_returns"."CR_ITEM_SK"),
  wakeful as (
  SELECT
      "quizzical"."_catalog_item_refunds_cat_refund_total" as "catalog_item_refunds_cat_refund_total"
  FROM
      "quizzical"),
  cheerful as (
  SELECT
      INVALID_REFERENCE_BUG as "qualifying_items_q_item_id"
  FROM
      "wakeful"
  WHERE
      sum(INVALID_REFERENCE_BUG) > 2 * coalesce("wakeful"."catalog_item_refunds_cat_refund_total",0)

  GROUP BY
      1),
  macho as (
  SELECT
      "ss_customer_address_customer_address"."CA_CITY" as "ss_customer_address_city",
      "ss_customer_address_customer_address"."CA_STREET_NAME" as "ss_customer_address_street_name",
      "ss_customer_address_customer_address"."CA_STREET_NUMBER" as "ss_customer_address_street_number",
      "ss_customer_address_customer_address"."CA_ZIP" as "ss_customer_address_zip",
      "ss_customer_first_sales_date_date"."D_YEAR" as "ss_customer_first_sales_date_year",
      "ss_customer_first_shipto_date_date"."D_YEAR" as "ss_customer_first_shipto_date_year",
      "ss_date_date"."D_YEAR" as "ss_date_year",
      "ss_item_items"."I_ITEM_ID" as "ss_item_text_id",
      "ss_item_items"."I_PRODUCT_NAME" as "ss_item_product_name",
      "ss_sale_address_customer_address"."CA_CITY" as "ss_sale_address_city",
      "ss_sale_address_customer_address"."CA_STREET_NAME" as "ss_sale_address_street_name",
      "ss_sale_address_customer_address"."CA_STREET_NUMBER" as "ss_sale_address_street_number",
      "ss_sale_address_customer_address"."CA_ZIP" as "ss_sale_address_zip",
      "ss_store_store"."S_STORE_NAME" as "ss_store_name",
      "ss_store_store"."S_ZIP" as "ss_store_zip"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "store_returns" as "pr_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "pr_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "pr_store_returns"."SR_TICKET_NUMBER"
      INNER JOIN "customer_demographics" as "ss_customer_demographic_customer_demographics" on "ss_store_sales"."SS_CDEMO_SK" = "ss_customer_demographic_customer_demographics"."CD_DEMO_SK"
      INNER JOIN "customer_demographics" as "ss_customer_demographics_customer_demographics" on "ss_customer_customers"."C_CURRENT_CDEMO_SK" = "ss_customer_demographics_customer_demographics"."CD_DEMO_SK"
      INNER JOIN "item" as "ss_item_items" on "pr_store_returns"."SR_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
      LEFT OUTER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
      LEFT OUTER JOIN "customer_address" as "ss_sale_address_customer_address" on "ss_store_sales"."SS_ADDR_SK" = "ss_sale_address_customer_address"."CA_ADDRESS_SK"
      LEFT OUTER JOIN "customer_address" as "ss_customer_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_address_customer_address"."CA_ADDRESS_SK"
      LEFT OUTER JOIN "date_dim" as "ss_customer_first_sales_date_date" on "ss_customer_customers"."C_FIRST_SALES_DATE_SK" = "ss_customer_first_sales_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "date_dim" as "ss_customer_first_shipto_date_date" on "ss_customer_customers"."C_FIRST_SHIPTO_DATE_SK" = "ss_customer_first_shipto_date_date"."D_DATE_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 1999 and "pr_store_returns"."SR_ITEM_SK" in (select cheerful."qualifying_items_q_item_id" from cheerful where cheerful."qualifying_items_q_item_id" is not null) and "pr_store_returns"."SR_ITEM_SK" = "pr_store_returns"."SR_ITEM_SK" and "pr_store_returns"."SR_TICKET_NUMBER" = "pr_store_returns"."SR_TICKET_NUMBER" and "ss_item_items"."I_COLOR" in ('purple','burlywood','indian','spring','floral','medium') and "ss_item_items"."I_CURRENT_PRICE" BETWEEN 65 AND 74 and "ss_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" != "ss_customer_demographics_customer_demographics"."CD_MARITAL_STATUS"

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
      14,
      15,
      "pr_store_returns"."SR_ITEM_SK",
      "pr_store_returns"."SR_TICKET_NUMBER",
      "ss_store_sales"."SS_COUPON_AMT",
      "ss_store_sales"."SS_EXT_LIST_PRICE",
      "ss_store_sales"."SS_EXT_WHOLESALE_COST",
      (cast("pr_store_returns"."SR_TICKET_NUMBER" as string) || '-' || cast("pr_store_returns"."SR_ITEM_SK" as string))),
  scrawny as (
  SELECT
      "macho"."ss_item_product_name" as "_store_agg_1999_product_name"
  FROM
      "macho"
  GROUP BY
      1,
      "macho"."ss_customer_address_city",
      "macho"."ss_customer_address_street_name",
      "macho"."ss_customer_address_street_number",
      "macho"."ss_customer_address_zip",
      "macho"."ss_customer_first_sales_date_year",
      "macho"."ss_customer_first_shipto_date_year",
      "macho"."ss_date_year",
      "macho"."ss_item_text_id",
      "macho"."ss_sale_address_city",
      "macho"."ss_sale_address_street_name",
      "macho"."ss_sale_address_street_number",
      "macho"."ss_sale_address_zip",
      "macho"."ss_store_name",
      "macho"."ss_store_zip"),
  friendly as (
  SELECT
      "scrawny"."_store_agg_1999_product_name" as "store_agg_1999_product_name"
  FROM
      "scrawny")
  SELECT
      "friendly"."store_agg_1999_product_name" as "store_agg_1999_product_name"
  FROM
      "friendly"
  GROUP BY
      1
  LIMIT (10), this should never occur. Please create an issue to report this.
  ```
- `trilogy run query64.preql`

  ```text
  Unexpected error in query64.preql: Invalid reference string found in query:
  WITH
  quizzical as (
  SELECT
      sum(( coalesce("cr_catalog_returns"."CR_REFUNDED_CASH",0) + coalesce("cr_catalog_returns"."CR_REVERSED_CHARGE",0) ) + coalesce("cr_catalog_returns"."CR_STORE_CREDIT",0)) as "_catalog_item_refunds_cat_refund_total"
  FROM
      "catalog_returns" as "cr_catalog_returns"
  GROUP BY
      "cr_catalog_returns"."CR_ITEM_SK"),
  wakeful as (
  SELECT
      "quizzical"."_catalog_item_refunds_cat_refund_total" as "catalog_item_refunds_cat_refund_total"
  FROM
      "quizzical")
  SELECT
      INVALID_REFERENCE_BUG as "qualifying_items_q_item_id"
  FROM
      "wakeful"
  WHERE
      sum(INVALID_REFERENCE_BUG) > 2 * coalesce("wakeful"."catalog_item_refunds_cat_refund_total",0)

  GROUP BY
      1
  LIMIT (10), this should never occur. Please create an issue to report this.
  ```
- `trilogy run query64.preql`

  ```text
  Unexpected error in query64.preql: Invalid reference string found in query:
  WITH
  quizzical as (
  SELECT
      "cr_catalog_returns"."CR_ITEM_SK" as "_catalog_item_refunds_ref_item_id",
      sum(( coalesce("cr_catalog_returns"."CR_REFUNDED_CASH",0) + coalesce("cr_catalog_returns"."CR_REVERSED_CHARGE",0) ) + coalesce("cr_catalog_returns"."CR_STORE_CREDIT",0)) as "_catalog_item_refunds_cat_refund_total"
  FROM
      "catalog_returns" as "cr_catalog_returns"
  GROUP BY
      1),
  wakeful as (
  SELECT
      "quizzical"."_catalog_item_refunds_cat_refund_total" as "catalog_item_refunds_cat_refund_total",
      "quizzical"."_catalog_item_refunds_ref_item_id" as "catalog_item_refunds_ref_item_id"
  FROM
      "quizzical"),
  cheerful as (
  SELECT
      "wakeful"."catalog_item_refunds_ref_item_id" as "catalog_item_refunds_ref_item_id"
  FROM
      "wakeful"
  WHERE
      sum(INVALID_REFERENCE_BUG) <= 2 * "wakeful"."catalog_item_refunds_cat_refund_total"

  GROUP BY
      1,
      "wakeful"."catalog_item_refunds_cat_refund_total")
  SELECT
      count("cheerful"."catalog_item_refunds_ref_item_id") as "item_count"
  FROM
      "cheerful", this should never occur. Please create an issue to report this.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {cs.item.id}
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query05.preql --content import raw.all_sales as s;
# Sales arm: get gross sales and net profit by channel entity
with sales_arm as
where s…
by rollup (combined.channel_type, combined.entity_id)
order by combined.channel_type asc nulls first, combined.entity_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` — a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...es, total_returns, net_profit) ???  select     case when groupin...

  Write stats: received 2469 chars / 2469 bytes; tail: …' nulls first, combined.entity_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql -e -c import raw.all_sales as sales;

# Per-line sale value (quantity * list_price)
auto line_value <- sales.quantity * sale…        sales.item.brand_id asc nulls first,
         sales.item.class_id asc nulls first,
         sales.item.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 25:33
     |
  25 |   and struct(sales.item.brand_id, sales.item.class_id, sales.item.category_id)
     |                                 ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...and struct(sales.item.brand_id ??? , sales.item.class_id, sales.i...

  Write stats: received 1570 chars / 1570 bytes; tail: …',\\n         sales.item.category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# ─────────────────────…ales
order by
    combined_sales.last_name asc nulls first,
    combined_sales.first_name asc nulls first,
    total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...sales as union(     (         ??? import raw.catalog_sales as cs...

  Write stats: received 2753 chars / 3309 bytes; tail: …' asc nulls first,\\n    total_sales asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# ─────────────────────…ales
order by
    combined_sales.last_name asc nulls first,
    combined_sales.first_name asc nulls first,
    total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens — write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ...e.year between 2000 and 2003) ??? by substring(ss.item.desc, 1,

  Write stats: received 2444 chars / 3000 bytes; tail: …' asc nulls first,\\n    total_sales asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws select count(*) from (select cs.bill_customer.id as cid where cs.sold_date.year = 2000 and cs.sold_date.month_of_year = 2 limit 100) as t;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...t raw.web_sales as ws; select ??? count(*) from (select cs.bill_...
  ```
- `trilogy run select date.id, date.date, date.month_of_year, date.month_seq, date.year from raw/date.preql where date.year = 1998 and date.month_of_year = 12; duckdb`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...ar, date.month_seq, date.year ??? from raw/date.preql where date...
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.physica…= catalog_ret_item_stats.ret_item_id
where catalog_item_stats.cat_ext_list_price > 2 * coalesce(catalog_ret_item_stats.cat_refund_total, 0)
limit 20;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:5
     |
  12 |     --sum(coalesce(cr3.refunded_cash,0)+coalesce(cr3.reversed_charge,0)+coalesce(cr3.store_credit,0)) as cat_refund_total
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...ce) as cat_ext_list_price     ??? --sum(coalesce(cr3.refunded_ca...

  Write stats: received 1096 chars / 1096 bytes; tail: …'alesce(catalog_ret_item_stats.cat_refund_total, 0)\\nlimit 20;'.
  ```
- `trilogy file write query64.preql --content import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

with catalog_item_sales as
select
    cs.item.i…_item_sales.cat_item_id = catalog_item_refunds.ref_item_id
where catalog_item_sales.cat_ext_list_price <= 2 * catalog_item_refunds.cat_refund_total
;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...as cat_refund_total ;  select ??? count(*) as item_count inner j...

  Write stats: received 568 chars / 568 bytes; tail: …'xt_list_price <= 2 * catalog_item_refunds.cat_refund_total\\n;'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.physica…   sum(ss.coupon_amt) as total_coupon_amt
left join pr.item.id = ss.item.id
left join pr.ticket_number = ss.ticket_number
;

select count(*) as cnt
;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
   = ss.ticket_number ;  select ??? count(*) as cnt ;

  Write stats: received 1714 chars / 1714 bytes; tail: …'ticket_number = ss.ticket_number\\n;\\n\\nselect count(*) as cnt\\n;'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.catalog_sales:cs select count(cs.order_number) where cs.sold_date.year = 2000 and cs.sold_date.month_of_year = 2 limit 10;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(cs.order_number) as order_number_count`
  Location:
  ...select count(cs.order_number) ??? where cs.sold_date.year = 2000...
  ```

### `join-resolution`

- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local.qualifying_item_id<Purpose.PROPERTY>Derivation.FILTER>'] from current model.
  ```
