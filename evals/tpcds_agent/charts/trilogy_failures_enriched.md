# Trilogy failure analysis — 20260703-023320

- Run `20260703-023320` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 278 | failed: 36 (13%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 24 | 67% |
| `syntax-parse` | 9 | 25% |
| `syntax-missing-alias` | 2 | 6% |
| `join-resolution` | 1 | 3% |

## Detail

### `other`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, all_sales.return_channel_dim_text_id where all_sales.ch…TORE' and all_sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date and all_sales.return_channel_dim_text_id is not null limit 10;`

  ```text
  Unexpected error in stdin: (_duckdb.OutOfMemoryException) Out of Memory Error: could not allocate block of size 256.0 KiB (24.9 GiB/25.0 GiB used)

  Possible solutions:
  * Reducing the number of threads (SET threads=X)
  * Disabling insertion-order preservation (SET preserve_insertion_order=false)
  * Increasing the memory limit (SET memory_limit='...GB')

  See also https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads
  [SQL:
  WITH
  vacuous as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_returns_unified"."CR_RETURNED_DATE_SK" as "all_sales_return_date_id"
  FROM
      "catalog_returns" as "all_sales_catalog_returns_unified"
  WHERE
       'CATALOG'  != 'STORE'

  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_returns_unified"."SR_RETURNED_DATE_SK" as "all_sales_return_date_id"
  FROM
      "store_returns" as "all_sales_store_returns_unified"
  WHERE
       'STORE'  != 'STORE'

  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_returns_unified"."WR_RETURNED_DATE_SK" as "all_sales_return_date_id"
  FROM
      "web_returns" as "all_sales_web_returns_unified"
  WHERE
       'WEB'  != 'STORE'
  ),
  cheerful as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_dim_return_unified"."CP_CATALOG_PAGE_ID" as "all_sales_return_channel_dim_text_id"
  FROM
      "catalog_page" as "all_sales_catalog_dim_return_unified"
  WHERE
       'CATALOG'  != 'STORE' and "all_sales_catalog_dim_return_unified"."CP_CATALOG_PAGE_ID" is not null

  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_dim_return_unified"."S_STORE_ID" as "all_sales_return_channel_dim_text_id"
  FROM
      "store" as "all_sales_store_dim_return_unified"
  WHERE
       'STORE'  != 'STORE' and "all_sales_store_dim_return_unified"."S_STORE_ID" is not null

  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_dim_return_unified"."web_site_id" as "all_sales_return_channel_dim_text_id"
  FROM
      "web_site" as "all_sales_web_dim_return_unified"
  WHERE
       'WEB'  != 'STORE' and "all_sales_web_dim_return_unified"."web_site_id" is not null
  ),
  abundant as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as "all_sales_channel_dim_text_id"
  FROM
      "catalog_page" as "all_sales_catalog_dim_unified"
  WHERE
       'CATALOG'  != 'STORE'

  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_dim_unified"."S_STORE_ID" as "all_sales_channel_dim_text_id"
  FROM
      "store" as "all_sales_store_dim_unified"
  WHERE
       'STORE'  != 'STORE'

  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_dim_unified"."web_site_id" as "all_sales_channel_dim_text_id"
  FROM
      "web_site" as "all_sales_web_dim_unified"
  WHERE
       'WEB'  != 'STORE'
  ),
  concerned as (
  SELECT
      "vacuous"."all_sales_channel" as "all_sales_channel"
  FROM
      "vacuous"
      INNER JOIN "date_dim" as "all_sales_return_date_date" on "vacuous"."all_sales_return_date_id" = "all_sales_return_date_date"."D_DATE_SK"
  WHERE
      cast("all_sales_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06'

  GROUP BY
      1,
      coalesce("all_sales_return_date_date"."D_DATE_SK","vacuous"."all_sales_return_date_id"))
  SELECT
      "cheerful"."all_sales_channel" as "all_sales_channel",
      "abundant"."all_sales_channel_dim_text_id" as "all_sales_channel_dim_text_id",
      "cheerful"."all_sales_return_channel_dim_text_id" as "all_sales_return_channel_dim_text_id"
  FROM
      "concerned"
      INNER JOIN "abundant" on "concerned"."all_sales_channel" = "abundant"."all_sales_channel"
      INNER JOIN "cheerful" on "concerned"."all_sales_channel" = "cheerful"."all_sales_channel"
  GROUP BY
      1,
      2,
      3
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(combined.channel_label)` (line 43), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel_label) as g order by g desc`.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Undefined concept: local.cust_id. Suggestions: ['store_cust_totals.cust_id']
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Nothing was executed: parsed 8 definition statement(s) (4 imports, 3 concepts, 1 rowset) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy file read query64.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query66.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 33 column 12 (char 1596). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query77.preql`

  ```text
  Syntax error in query77.preql: ORDER BY contains aggregate `grouping(s.channel)` (line 3), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(s.channel) as g order by g desc`.
  ```
- `trilogy run query77.preql`

  ```text
  Unexpected error in query77.preql: optimize_ctes() got an unexpected keyword argument 'equal_join_keys'. Did you mean 'full_join_keys'?
  ```
- `trilogy run --import raw.all_sales:s select s.channel, s.outlet_id, sum(s.ext_sales_price) as sales limit 5;`

  ```text
  Unexpected error in stdin: optimize_ctes() got an unexpected keyword argument 'equal_join_keys'. Did you mean 'full_join_keys'?
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.id, sum(ss.ext_sales_price) as sales limit 5;`

  ```text
  Unexpected error in stdin: optimize_ctes() got an unexpected keyword argument 'equal_join_keys'. Did you mean 'full_join_keys'?
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run test_query.preql`

  ```text
  Unexpected error in test_query.preql: optimize_ctes() got an unexpected keyword argument 'equal_join_keys'. Did you mean 'full_join_keys'?
  ```
- `trilogy run test_query2.preql`

  ```text
  Unexpected error in test_query2.preql: optimize_ctes() got an unexpected keyword argument 'equal_join_keys'. Did you mean 'full_join_keys'?
  ```
- `trilogy run test_query3.preql`

  ```text
  Unexpected error in test_query3.preql: UpgradeOuterFromKeySetEquivalence.__init__() got an unexpected keyword argument 'equal_join_keys'. Did you mean 'full_join_keys'?
  ```
- `trilogy run test_query4.preql`

  ```text
  Unexpected error in test_query4.preql: UpgradeOuterFromKeySetEquivalence.__init__() got an unexpected keyword argument 'equal_join_keys'. Did you mean 'full_join_keys'?
  ```
- `trilogy file read raw/repro.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query77.preql`

  ```text
  Syntax error in query77.preql: Undefined concept: cs.return_date.date. Suggestions: ['ss.date.date', 'ss.return_date.date', 'ss.customer.first_sales_date.date', 'ss.customer.first_shipto_date.date', 'ss.return_customer.first_sales_date.date', 'ss.return_customer.first_shipto_date.date']
  ```
- `trilogy run test_query10.preql`

  ```text
  Syntax error in test_query10.preql: Undefined concept: cs.return_date.date (line 4, col 5, in SELECT). Suggestions: ['cs.date.date', 'cs.ship_date.date', 'cs.sold_date.date', 'cs.ship_customer.first_sales_date.date', 'cs.ship_customer.first_shipto_date.date', 'cs.bill_customer.first_sales_date.date']
  ```
- `trilogy run query77.preql`

  ```text
  Resolution error in query77.preql: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['cr.call_center.id', 'cr.item.id', 'cr.order_number', 'cs.call_center.id', 'cs.ext_sales_price', 'cs.sold_date.date', 'cs.sold_date.id'], have {'cr.item.id'} or {'cr.order_number'}. Different paths are is: [{'cr.item.id'}, {'cr.order_number'}]")
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw/all_sales:all_sales select all_sales.is_returned, count(all_sales.order_id) as cnt by all_sales.is_returned;`

  ```text
  Syntax error in stdin:  --> 2:64
    |
  2 | select all_sales.is_returned, count(all_sales.order_id) as cnt by all_sales.is_returned;
    |                                                                ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...nt(all_sales.order_id) as cnt ??? by all_sales.is_returned;
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, count(all_sales.order_id) as cnt where all_sales.date.d…-08-23'::date and '2000-09-06'::date and all_sales.channel_dim_text_id is not null group by all_sales.channel, all_sales.channel_dim_text_id limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...annel_dim_text_id is not null ??? group by all_sales.channel, al...
  ```
- `trilogy file write query23.preql -e -c import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Step 1: Frequent… as total_sales
order by
    combined.last_name asc nulls first,
    combined.first_name asc nulls first,
    total_sales asc nulls first
limit 100
;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:1
     |
  14 | select
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
       item_date_pair_count > 4 ??? select     desc_prefix ;  # St...

  Write stats: received 2288 chars / 2288 bytes; tail: …'asc nulls first,\\n    total_sales asc nulls first\\nlimit 100\\n;'.
  ```
- `trilogy file write query59.preql --content import raw.store_sales as ss;

# Sum store unit price (sales_price) per (store, week_seq, day_of_week)
# Strategy:….saturday
order by weekly.store_name asc nulls first,
         weekly.store_code asc nulls first,
         weekly.week_seq asc nulls first
limit 100;`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:5
     |
  22 |     @sun_price as sunday,
     |     ^---
     |
     = expected select_item, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
      ss.date.year as year,     ??? @sun_price as sunday,     @mon...

  Write stats: received 1656 chars / 1656 bytes; tail: …'s first,\\n         weekly.week_seq asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.catalog_sales:cs select cs.item.id, sum(cs.ext_list_price) as total_list, count(cs.line_item) as cnt group by cs.item.id limit 3;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...t, count(cs.line_item) as cnt ??? group by cs.item.id limit 3;
  ```
- `trilogy run --import raw.catalog_sales:cs select cs.item.id, sum(cs.ext_list_price) as total_list group by cs.item.id limit 3;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ext_list_price) as total_list ??? group by cs.item.id limit 3;
  ```
- `trilogy file write query67.preql -e -c import raw.store_sales as ss;

# Sum of ext_sales_price (treating null as 0)
auto sales <- coalesce(sum(ss.ext_sales_p…,
    ss.date.month_of_year asc nulls first,
    ss.store.text_id asc nulls first,
    sales asc nulls first,
    cat_rank asc nulls first
limit 100;`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
    --> 41:1
     |
  41 | having cat_rank <= 100
     | ^---
     |
     = expected limit or order_by
  Location:
  ...th_of_year, ss.store.text_id) ??? having cat_rank <= 100 order b...

  Hint: HAVING must come *before* the `by rollup/cube/grouping sets` clause in Trilogy (the reverse of SQL's `GROUP BY ... HAVING`). Reorder to:
      select <cols> having <cond> by rollup (<keys>) order by <cols> limit <n>;

  Write stats: received 1829 chars / 1829 bytes; tail: …'les asc nulls first,\\n    cat_rank asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/all_sales:all_sales select distinct all_sales.channel order by all_sales.channel;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...ll_sales as all_sales; select ??? distinct all_sales.channel ord...
  ```
- `trilogy run --import raw.store_sales:ss select ss.date.year, count(ss.line_item) as cnt group by 1 order by 1;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...r, count(ss.line_item) as cnt ??? group by 1 order by 1;
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.item:item select item.color, item.current_price, count(item.id) limit 5;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(item.id) as id_count`
  Location:
  ...current_price, count(item.id) ??? limit 5;
  ```
- `trilogy run --import raw.catalog_sales:cs select cs.date.year, cs.sold_date.year, count(cs.line_item) limit 5;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(cs.line_item) as line_item_count`
  Location:
  ...ate.year, count(cs.line_item) ??? limit 5;
  ```

### `join-resolution`

- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local._cat_qualified_item_id<Purpose.KEY>Derivation.BASIC>', 'local._cat_qualified_cat_list_sum<Purpose.METRIC>Derivation.BASIC>', 'local._cat_qualified_cat_refund_sum<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
