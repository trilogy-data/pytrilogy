# Trilogy failure analysis — 20260704-035023

- Run `20260704-035023` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 256 | failed: 33 (13%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 19 | 58% |
| `syntax-parse` | 13 | 39% |
| `cli-misuse` | 1 | 3% |

## Detail

### `other`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write -c --escapes query05.preql import raw.all_sales as s;

# sales arm
with sales_data as
where s.date.date between '2000-08-23'::date and '20…0::numeric(15,2) as ext_sales,
    s.return_amount as returns,
    0::numeric(15,2) as net_profit,
    s.return_net_loss as ret_net_loss
;

select 1;`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(combined.channel_type)` (line 44), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel_type) as g order by g desc`.
  ```
- `trilogy run --import raw/all_sales:s --import raw/store:st where s.channel = 'STORE' and s.return_channel_dim_text_id is not null and s.return_date.date between '2000-08-23'::date and '2000-09-06'::date select s.channel, s.return_channel_dim_text_id, st.text_id limit 20;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {s.channel, s.return_channel_dim_text_id, s.return_date.date}; {st.text_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run --import raw/catalog_returns:cr where cr.return_date.date between '2000-08-23'::date and '2000-09-06'::date select count_distinct(cr.sales.catalog_page.text_id) as distinct_catalog_pages, count_distinct(cr.sales.catalog_page.id) as distinct_ids;`

  ```text
  Syntax error in stdin: Undefined concept: cr.return_date.date (line 2, col 7, in WHERE). Suggestions: ['cr.sales.date.date', 'cr.sales.ship_date.date', 'cr.sales.sold_date.date', 'cr.sales.ship_customer.first_sales_date.date', 'cr.sales.ship_customer.first_shipto_date.date', 'cr.sales.bill_customer.first_sales_date.date']
  ```
- `trilogy run --import raw/catalog_returns:cr where cr.return_date.date between '2000-08-23'::date and '2000-09-06'::date select count_distinct(cr.sales.catalog_page.text_id) as distinct_catalog_pages;`

  ```text
  Syntax error in stdin: Undefined concept: cr.return_date.date (line 2, col 7, in WHERE). Suggestions: ['cr.sales.date.date', 'cr.sales.ship_date.date', 'cr.sales.sold_date.date', 'cr.sales.ship_customer.first_sales_date.date', 'cr.sales.ship_customer.first_shipto_date.date', 'cr.sales.bill_customer.first_sales_date.date']
  ```
- `trilogy run --import raw/catalog_returns:cr where cr.date.date between '2000-08-23'::date and '2000-09-06'::date select count_distinct(cr.sales.channel_dim_text_id) as sale_entity_text_ids;`

  ```text
  Syntax error in stdin: Undefined concept: cr.sales.channel_dim_text_id (line 2, in SELECT). Suggestions: ['cr.sales.time.text_id', 'cr.sales.item.text_id', 'cr.sales.call_center.text_id']
  ```
- `trilogy file read query05.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:ss select ss.date.week_seq as ws, count(ss.ticket_number) as cnt where ss.date.year = 2002 and ss.store.id is null;`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260704-035023\workspace\_worker_0\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy file read query59.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Unexpected error in query64.preql: Join chain repeats source `y1999` (keys `y1999.agg_by_dim.base_rows.item_id` and `y1999.agg_by_dim.base_rows.store_name`). A `=` chain joins ONE key across distinct sources; join a composite key with `and` or separate clauses (e.g. `a.k1 = b.k1 and a.k2 = b.k2`).
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 24). The requested concepts split into 2 disconnected subgraphs: {_base_rows_cust_city, _base_rows_cust_street_name, _base_rows_cust_street_number, _base_rows_cust_zip, _base_rows_first_sales_year, _base_rows_first_ship_year, _base_rows_item_id, _base_rows_sale_city, _base_rows_sale_street_name, _base_rows_sale_street_number, _base_rows_sale_year, _base_rows_sale_zip, _base_rows_store_name, _base_rows_store_zip, item_color_ok, item_price_ok, marital_mismatch, ss.coupon_amt, ss.ext_list_price, ss.ext_wholesale_cost, ss.item.product_name, ss.ticket_number}; {item_cat_condition}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: local.cat_sum_ext_list. Suggestions: ['cat_agg.cat_sum_ext_list', '_cat_agg_cat_sum_ext_list', 'cs.ext_list_price']
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: 2 undefined concept references; fix all before re-running:
    - local.cat_sum_ext_list (line 31, col 7, in WHERE); did you mean: cat_sales_agg.cat_sum_ext_list, cat_qualified_items.cat_sales_agg.cat_sum_ext_list, _cat_sales_agg_cat_sum_ext_list, cs.ext_list_price?
    - local.cat_sum_refund (line 31, in WHERE); did you mean: cat_refund_agg.cat_sum_refund, cat_qualified_items.cat_refund_agg.cat_sum_refund, _cat_refund_agg_cat_sum_refund, cat_sum_ext_list?
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw/all_sales:s select s.channel, count(s.channel_dim_text_id) as cnt, count(s.return_channel_dim_text_id) as ret_cnt, count(distinct s.channel_dim_text_id) as distinct_sale, count(distinct s.return_channel_dim_text_id) as distinct_ret group by s.channel;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ..._dim_text_id) as distinct_ret ??? group by s.channel;
  ```
- `trilogy file write query05.preql --content import raw.all_sales as s;

# sales arm
with sales_data as
where s.date.date between '2000-08-23'::date and '2000-…0::numeric(15,2) as ext_sales,
    s.return_amount as returns,
    0::numeric(15,2) as net_profit,
    s.return_net_loss as ret_net_loss
;

select 1;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 45:9
     |
  45 | select 1;
     |         ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ss as ret_net_loss ;  select 1 ??? ;

  Write stats: received 1461 chars / 1461 bytes; tail: …'t_profit,\\n    s.return_net_loss as ret_net_loss\\n;\\n\\nselect 1;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as s;

# sales arm - aggregate to get sales metrics per entity
with sales_data as
where s.dat…sce(s.return_amount, 0)) as ret_amount,
    sum(0::numeric(15,2)) as net_profit,
    sum(coalesce(s.return_net_loss, 0)) as ret_net_loss
;

select 1;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 45:9
     |
  45 | select 1;
     |         ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...)) as ret_net_loss ;  select 1 ??? ;

  Write stats: received 1664 chars / 1664 bytes; tail: …'coalesce(s.return_net_loss, 0)) as ret_net_loss\\n;\\n\\nselect 1;'.
  ```
- `trilogy run --import raw/all_sales:s,raw/store:st where s.channel = 'STORE' and s.return_channel_dim_text_id is not null and s.return_date.date between '2000…te and '2000-09-06'::date select s.channel, s.return_channel_dim_text_id, st.text_id, st.id where s.return_channel_dim_text_id = st.text_id limit 20;`

  ```text
  Syntax error in stdin:  --> 1:8
    |
  1 | import raw.all_sales as s,raw/store:st;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw.all_sales as s,raw/store:s...
  ```
- `trilogy file write counts.preql --content import raw.store as st;
import raw.catalog_page as cp;
import raw.web_site as ws;

select
    (select st count(st.id) as stores),
    (select cp count(cp.id) as catalog_pages),
    (select ws count(ws.id) as web_sites);`

  ```text
  refused to write 'counts.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:16
    |
  6 |     (select st count(st.id) as stores),
    |                ^---
    |
    = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, select_grouping, or JOIN_TYPE
  Location:
  ...as ws;  select     (select st ??? count(st.id) as stores),     (...

  Write stats: received 219 chars / 219 bytes; tail: …'s catalog_pages),\\n    (select ws count(ws.id) as web_sites);'.
  ```
- `trilogy run --import raw/all_sales:s where s.channel = 'STORE' and s.return_date.date between '2000-08-23'::date and '2000-09-06'::date and s.return_channel_…rn_channel_dim_text_id as ret_entity, sum(s.return_amount) as total_ret, sum(s.ext_sales_price) as total_sales group by s.return_channel_dim_text_id;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...t_sales_price) as total_sales ??? group by s.return_channel_dim_...
  ```
- `trilogy file write query05_check.preql --content import raw.all_sales as s;

with combined as union(
    (where s.date.date between '2000-08-23'::date and '2…
    combined.channel_type asc nulls first,
    combined.entity_id asc nulls first
where combined.channel_type is null or combined.entity_id is null;`

  ```text
  refused to write 'query05_check.preql': not syntactically valid Trilogy.

  Parse error:
    --> 53:1
     |
  53 | where combined.channel_type is null or combined.entity_id is null;
     | ^---
     |
     = expected limit
  Location:
  ...ned.entity_id asc nulls first ??? where combined.channel_type is...

  Write stats: received 2303 chars / 2303 bytes; tail: …'combined.channel_type is null or combined.entity_id is null;'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.id as sid, ss.store.name as sn, ss.store.text_id as sc, ss.date.week_seq as ws, count(ss.ticket_number) as cnt where ss.date.year = 2001 group by sid, sn, sc, ws having cnt != cnt having 1=0;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...cnt where ss.date.year = 2001 ??? group by sid, sn, sc, ws havin...
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.id as sid, ss.date.week_seq as ws, count(ss.ticket_number) as cnt, count(ss.ticket_number) as cnt2 where ss.date.year = 2001 and ss.store.id is not null group by sid, ws order by cnt desc limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...1 and ss.store.id is not null ??? group by sid, ws order by cnt
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.id as sid, count(ss.ticket_number) as cnt where ss.date.year = 2001 group by sid;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...cnt where ss.date.year = 2001 ??? group by sid;
  ```
- `trilogy file write query59.preql --content import raw.store_sales as ss;

# Aggregate per (store, week, day_of_week): sum of unit prices
# Then use lead to g…_ratio
order by
    weekly_prices.store_name nulls first,
    weekly_prices.store_code nulls first,
    weekly_prices.week_seq nulls first
limit 100;`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
   (select next_prices.week_seq ??? from weekly_prices next_prices...

  Write stats: received 2859 chars / 2859 bytes; tail: …'lls first,\\n    weekly_prices.week_seq nulls first\\nlimit 100;'.
  ```
- `trilogy file write query67.preql --content import raw.store_sales as ss;

auto ext_sales_price_coalesced <- coalesce(ss.ext_sales_price, 0);

auto g_cat <- g…sc nulls first,
         ss.store.text_id asc nulls first,
         summed_sales asc nulls first,
         within_cat_rank asc nulls first
limit 100;`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
    --> 24:1
     |
  24 | having within_cat_rank <= 100
     | ^---
     |
     = expected limit or order_by
  Location:
  ...th_of_year, ss.store.text_id) ??? having within_cat_rank <= 100

  Hint: HAVING must come *before* the `by rollup/cube/grouping sets` clause in Trilogy (the reverse of SQL's `GROUP BY ... HAVING`). Reorder to:
      select <cols> having <cond> by rollup (<keys>) order by <cols> limit <n>;

  Write stats: received 1392 chars / 1392 bytes; tail: …'s first,\\n         within_cat_rank asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query77.preql --content import raw.all_sales as all_sales;

# Filter to the period: sales sold 2000-08-23 through 2000-09-22
# Returns who…l
by rollup (all_sales.channel, all_sales.outlet_id)
order by _level asc, channel asc nulls first, outlet_id asc nulls first, returns desc
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:41
     |
  12 |     case when grouping(all_sales.channel, all_sales.outlet_id) = 1 then 'ALL OUTLETS'
     |                                         ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...hen grouping(all_sales.channel ??? , all_sales.outlet_id) = 1 the...

  Write stats: received 988 chars / 988 bytes; tail: …'ls first, outlet_id asc nulls first, returns desc\\nlimit 100;'.
  ```

### `cli-misuse`

- `trilogy run --import raw/store:st select count(st.id) as stores; --import raw/catalog_page:cp select count(cp.id) as catalog_pages; --import raw/web_site:ws select count(ws.id) as web_sites;`

  ```text
  'select count(cp.id) as catalog_pages;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
