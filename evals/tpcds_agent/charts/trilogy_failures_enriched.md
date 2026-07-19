# Trilogy failure analysis — 20260717-173332

- Run `20260717-173332_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 989 | failed: 70 (7%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 36 | 51% |
| `syntax-parse` | 31 | 44% |
| `cli-misuse` | 2 | 3% |
| `type-error` | 1 | 1% |

## Detail

### `other`

- `trilogy run answer_1858999935.preql`

  ```text
  Syntax error in answer_1858999935.preql: 3 undefined concept references; fix all before re-running:
    - s2.sale_date.week_seq (line 14, col 44, in SELECT); did you mean: s2.sale_date.year, s.sale_date.week_seq, s.return_date.week_seq, s.ship_customer.first_sales_date.week_seq, s.billing_customer.first_sales_date.week_seq, s.purchasing_customer.first_sales_date.week_seq?
    - s2.channel (line 14, col 72, in WHERE); did you mean: s.channel, s.channel_dim_id?
    - s2.sale_date.year (line 14, col 109, in WHERE); did you mean: s2.sale_date.week_seq, s.sale_date.year, s.return_date.year, s.ship_customer.first_sales_date.year, s.billing_customer.first_sales_date.year, s.purchasing_customer.first_sales_date.year?
  ```
- `trilogy file read answer_1858999935.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3863442186.preql`

  ```text
  Syntax error in answer_3863442186.preql: 6 undefined concept references; fix all before re-running:
    - local.s2001 (line 72, col 9, in WHERE); did you mean: sp.s2001, w2001, s2002, c2001?
    - local.c2001 (line 73, col 9, in WHERE); did you mean: cp.c2001, w2001, s2001, c2002?
    - local.w2001 (line 74, col 9, in WHERE); did you mean: wp.w2001, w2002, s2001, c2001?
    - local.s2002 (line 75, col 9, in WHERE); did you mean: sp.s2002, w2002, s2001, c2002?
    - local.c2002 (line 76, col 9, in WHERE); did you mean: cp.c2002, w2002, s2002, c2001?
    - local.w2002 (line 77, col 9, in WHERE); did you mean: wp.w2002, w2001, s2002, c2002?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_3705756794.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 29 column 12 (char 1255). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2524943990.preql`

  ```text
  Syntax error in answer_2524943990.preql: ORDER BY contains aggregate `count(cs.order_number)` (line 26), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --count(cs.order_number) as g order by g desc`.
  ```
- `trilogy run answer_765177085.preql`

  ```text
  Resolution error in answer_765177085.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {catalog_qualifying.catalog_sale_qty, catalog_qualifying.cust_sk, catalog_qualifying.item_sk, store_qualifying.item_desc, store_qualifying.item_id, store_qualifying.store_state}
  ```
- `trilogy run answer_2604809012.preql`

  ```text
  Resolution error in answer_2604809012.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced: {local.item_code, local.warehouse_name}; still unresolved: {local.after_total, local.before_total}
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1798498862.preql`

  ```text
  Syntax error in answer_1798498862.preql: Comparison `ss.return_date.month_of_year <= 12` matches every value of enum field 'ss.return_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy run answer_4207382245.preql`

  ```text
  Syntax error in answer_4207382245.preql: Undefined concept: sale_date.year. Suggestions: ['store.sale_date.year', 'web.sale_date.year', 'web.ship_date.year', 'store.return_date.year', 'web.return_date.year', 'store.customer.first_sales_date.year']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Resolution error in answer_3560698360.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced: {web_run.web_daily.item_sk, web_run.web_daily.sale_date, web_run.web_rt}; still unresolved: {local.web_running_maximum}
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 31 (char 30). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_345752060.preql`

  ```text
  Syntax error in answer_345752060.preql: Undefined concept: item.category. Suggestions: ['sales.item.category', 'sales.item.category_id']
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting value: line 1 column 908 (char 907). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read answer_3273495117.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Syntax error in answer_3544057080.preql: Undefined concept: item.color. Suggestions: ['ss.item.color', 'cs.item.color', 'ss.item.category']
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Syntax error in answer_3544057080.preql: Undefined concept: a.item_id. Suggestions: ['agg_data.item_id', 'ss.item.id', 'cs.item.id']
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 88 column 12 (char 4589). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 44 column 12 (char 1206). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_1772060640.preql`

  ```text
  Syntax error in answer_1772060640.preql: ORDER BY references 'ss.customer.sk', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.customer.sk order by ss.customer.sk asc`.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_2910545909.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store:store --import raw.all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, store.id where all_sales.channel = 'STORE' and all_sales.channel_dim_id = store.sk limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {all_sales.channel, all_sales.channel_dim_id, all_sales.channel_dim_text_id}; {store.id, store.sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_4111870542.preql`

  ```text
  Syntax error in answer_4111870542.preql: Undefined concept: local.by_channel. Suggestions: ['a.channel', 'by_channel.channel', 'by_channel.item_code']
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_3734509910.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write --content import raw.catalog_sales as cs;

# Filter: ship date year = 2000, and all three surrogate keys non-null
where cs.ship_date.year …y 
    wh_name_20 asc nulls first,
    ship_mode_type asc nulls first,
    call_center_name_lower asc nulls first
limit 100;
 answer_3498056762.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```

### `syntax-parse`

- `trilogy run --import raw.date:d select min(d.week_seq) as min_ws_2001, max(d.week_seq) as max_ws_2001 by d.year having d.year = 2001;`

  ```text
  Syntax error in stdin:  --> 2:71
    |
  2 | select min(d.week_seq) as min_ws_2001, max(d.week_seq) as max_ws_2001 by d.year having d.year = 2001;
    |                                                                       ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...ax(d.week_seq) as max_ws_2001 ??? by d.year having d.year = 2001...
  ```
- `trilogy run --import raw.date:d where d.week_seq >= 5323 and d.week_seq <= 5375 select min(d.week_seq) as min_ws, max(d.week_seq) as max_ws, d.year, count(distinct d.week_seq) as num_weeks by d.year order by d.year;`

  ```text
  Syntax error in stdin:  --> 2:158
    |
  2 | where d.week_seq >= 5323 and d.week_seq <= 5375 select min(d.week_seq) as min_ws, max(d.week_seq) as max_ws, d.year, count(distinct d.week_seq) as num_weeks by d.year order by d.year;
    |                                                                                                                                                              ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...inct d.week_seq) as num_weeks ??? by d.year order by d.year;
  ```
- `trilogy run --import raw.date:d where d.week_seq >= 5323 and d.week_seq <= 5375 select min(d.week_seq) as min_ws, max(d.week_seq) as max_ws, d.year, count_distinct(d.week_seq) as num_weeks by d.year order by d.year;`

  ```text
  Syntax error in stdin:  --> 2:158
    |
  2 | where d.week_seq >= 5323 and d.week_seq <= 5375 select min(d.week_seq) as min_ws, max(d.week_seq) as max_ws, d.year, count_distinct(d.week_seq) as num_weeks by d.year order by d.year;
    |                                                                                                                                                              ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...inct(d.week_seq) as num_weeks ??? by d.year order by d.year;
  ```
- `trilogy run --import raw.all_sales:s where s.channel in ('WEB', 'CATALOG') select s.sale_date.week_seq, s.sale_date.day_of_week, sum(s.ext_sales_price) as sales by s.sale_date.week_seq, s.sale_date.day_of_week order by s.sale_date.week_seq, s.sale_date.day_of_week limit 10;`

  ```text
  Syntax error in stdin:  --> 2:125
    |
  2 | where s.channel in ('WEB', 'CATALOG') select s.sale_date.week_seq, s.sale_date.day_of_week, sum(s.ext_sales_price) as sales by s.sale_date.week_seq, s.sale_date.day_of_week order by s.sale_date.week_seq, s.sale_date.day_of_week limit 10;
    |                                                                                                                             ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...m(s.ext_sales_price) as sales ??? by s.sale_date.week_seq, s.sal...
  ```
- `trilogy file write answer_3863442186.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Pe…verything and apply filters
select
    ci.cid as customer_id,
    ci.fn as first_name,
    ci.ln as last_name,
    ci.pcf as preferred_customer_flag
`

  ```text
  refused to write 'answer_3863442186.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...cf as preferred_customer_flag ???

  Write stats: received 1602 chars / 1602 bytes; tail: …'  ci.ln as last_name,\\n    ci.pcf as preferred_customer_flag\\n'.
  ```
- `trilogy run --import raw.all_sales:all select all.channel, all.return_channel_dim_text_id, count(all.return_channel_dim_id) as cnt where all.return_date.date…ull and all.channel = 'CATALOG' and all.return_channel_dim_text_id = 'AAAAAAAAAABBAAAA' group by all.channel, all.return_channel_dim_text_id limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ..._text_id = 'AAAAAAAAAABBAAAA' ??? group by all.channel, all.retu...
  ```
- `trilogy run --import raw.all_sales:all select count(*) as sales_count where all.sale_date.date between '2000-08-23'::date and '2000-09-06'::date and all.channel_dim_id is not null and all.channel = 'CATALOG' and all.channel_dim_text_id = 'AAAAAAAAAABBAAAA' limit 5;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a NON-NULL GRAIN KEY: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). It MUST be a key, and one that is not nullable: `count(x)` skips rows where `x` is NULL, so counting a nullable property (a name, a date, any optional field) silently undercounts. When the grain takes SEVERAL keys, name them with `grain(...)`: `count(grain(order_id, item.id))` counts order+item combinations, and `count_distinct(grain(first_name, last_name, sale_date))` counts distinct combinations - `grain()` is never NULL, so combinations with a missing member still count. For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
   raw.all_sales as all; select ??? count(*) as sales_count where
  ```
- `trilogy file write answer_4199102535.preql --content import raw/customer as c;
import raw/store_sales as ss;
import raw/web_sales as ws;
import raw/catalog_s…rent_demographics.dependent_count,
    c.current_demographics.employed_dependent_count,
    c.current_demographics.college_dependent_count
limit 100;`

  ```text
  refused to write 'answer_4199102535.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/customer as c;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/customer as c; import raw/...

  Write stats: received 1938 chars / 1938 bytes; tail: …'   c.current_demographics.college_dependent_count\\nlimit 100;'.
  ```
- `trilogy file write answer_219203908.preql --content import raw.all_sales as s;

# Step 1: Find (brand_id, class_id, category_id) tuples with no nulls
# that …survivors.brand_id asc nulls first,
         leaf_survivors.class_id asc nulls first,
         leaf_survivors.category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_219203908.preql': not syntactically valid Trilogy.

  Parse error:
    --> 46:6
     |
  46 |     (import raw.all_sales as s;
     |      ^---
     |
     = expected select_statement
  Location:
  ...ing_tuples as intersect(     ( ??? import raw.all_sales as s;

  Write stats: received 3786 chars / 3786 bytes; tail: …'      leaf_survivors.category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_219203908.preql --content import raw.all_sales as s;

# Step 1: Find (brand_id, class_id, category_id) with no nulls that appear in…survivors.brand_id asc nulls first,
         leaf_survivors.class_id asc nulls first,
         leaf_survivors.category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_219203908.preql': not syntactically valid Trilogy.

  Parse error:
    --> 38:4
     |
  38 |    where s.sale_date.year between 1999 and 2001;
     |    ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or window_sql_over
  Location:
  ...ntity * s.list_price) by *    ??? where s.sale_date.year between...

  Write stats: received 2690 chars / 2690 bytes; tail: …'      leaf_survivors.category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Ide…l_sales
having
    total_sales > 0
order by
    last_name asc nulls first,
    first_name asc nulls first,
    total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...rst_name, item_sk, line_total) ???  where combined_sales.line_to...

  Write stats: received 2899 chars / 2899 bytes; tail: …' asc nulls first,\\n    total_sales asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_751385098.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with common_co…      ws.sale_date.date as sale_date
    )
) -> (last_name, first_name, sale_date)

select count(common_combos.sale_date) as channel_count
limit 100;`

  ```text
  refused to write 'answer_751385098.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...t_name, first_name, sale_date) ???  select count(common_combos.s...

  Write stats: received 1025 chars / 1025 bytes; tail: …'t count(common_combos.sale_date) as channel_count\\nlimit 100;'.
  ```
- `trilogy file write answer_1663418777.preql -e -c import raw.item as item;

# Step 1: Get the item sks matching the 8 profiles
with matching_sks as
where (ite…rer_id between 1 and 500
  and item.manufact in matching_sks.manufact
select distinct
    item.product_name
order by
    item.product_name
limit 100;`

  ```text
  refused to write 'answer_1663418777.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
   matching_sks.manufact select ??? distinct     item.product_name...

  Write stats: received 1172 chars / 1172 bytes; tail: …' item.product_name\\norder by\\n    item.product_name\\nlimit 100;'.
  ```
- `trilogy file write answer_3553309440.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Dec 1998 month_…stomer_id,
    sum(ss.ext_sales_price) as total_store_price
group by customer_id
having total_store_price is not null
order by customer_id
limit 100;`

  ```text
  refused to write 'answer_3553309440.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...s_price) as total_store_price ??? group by customer_id having to...

  Write stats: received 1352 chars / 1352 bytes; tail: …'otal_store_price is not null\\norder by customer_id\\nlimit 100;'.
  ```
- `trilogy file write answer_145690531.preql --content import raw.all_sales as all_sales;

# Week containing 2000-01-03
where all_sales.sale_date.week_seq = 5218
select
    all_sales.item.id as item_code##
    all_sales.channel##
    sum(all_sales.ext_sales_price) as channel_total
;
limit 100;`

  ```text
  refused to write 'answer_145690531.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:5
    |
  7 |     all_sales.channel##
    |     ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...es.item.id as item_code##     ??? all_sales.channel##     sum(al...

  Write stats: received 239 chars / 239 bytes; tail: …'sum(all_sales.ext_sales_price) as channel_total\\n;\\nlimit 100;'.
  ```
- `trilogy file write answer_145690531.preql --content import raw.all_sales as all_sales;

# Week containing 2000-01-03
where all_sales.sale_date.week_seq = 521…lect
    all_sales.item.id as item_code,
    -- all_sales.channel as channel_hidden,
    sum(all_sales.ext_sales_price) as channel_total
;
limit 100;`

  ```text
  refused to write 'answer_145690531.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:1
     |
  10 | limit 100;
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...les_price) as channel_total ; ??? limit 100;

  Write stats: received 258 chars / 258 bytes; tail: …'sum(all_sales.ext_sales_price) as channel_total\\n;\\nlimit 100;'.
  ```
- `trilogy file write answer_1623435181.preql --content import raw.store_sales as ss;

# For each (store, calendar week), build 7 day-of-week columns
# week_seq…order by this_year.store_name asc nulls first,
         this_year.store_code asc nulls first,
         this_year.week_seq asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_1623435181.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...week_seq = this.week_seq + 52 ??? subset join this_year.store_sk...

  Write stats: received 3351 chars / 3351 bytes; tail: …'rst,\\n         this_year.week_seq asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.item:item select distinct item.brand_name where item.brand_name ilike '%scholaramalgamalg%' or item.brand_name ilike '%exportiunivam…mporto%' or item.brand_name ilike '%edu packscholar%' or item.brand_name ilike '%exportiimporto%' or item.brand_name ilike '%importoamalg%' limit 30;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.item as item; select ??? distinct item.brand_name where...
  ```
- `trilogy file write answer_3544057080.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.customer as cust;

# Filter ite…gg_data.sale_year as yr2,
    agg_data.line_count as cnt2
order by 
    product_name asc,
    store_name asc,
    cnt2 asc,
    ws1 asc,
    ws2 asc;`

  ```text
  refused to write 'answer_3544057080.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...ata.store_name = a.store_name ??? subset join agg_data.store_zip...

  Write stats: received 3269 chars / 3269 bytes; tail: …'\\n    store_name asc,\\n    cnt2 asc,\\n    ws1 asc,\\n    ws2 asc;'.
  ```
- `trilogy file write answer_3544057080.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.customer as cust;

# Filter ite…l
    and yr1999.item_id is not null
    and cnt2 <= cnt1
order by 
    product_name asc,
    store_name asc,
    cnt2 asc,
    ws1 asc,
    ws2 asc;`

  ```text
  refused to write 'answer_3544057080.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:1
     |
  27 | rowset yr1999 as
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...os_address fields - year 1999 ??? rowset yr1999 as where      ss...

  Write stats: received 4199 chars / 4199 bytes; tail: …'\\n    store_name asc,\\n    cnt2 asc,\\n    ws1 asc,\\n    ws2 asc;'.
  ```
- `trilogy run --import raw.web_sales:web select distinct web.ship_mode.carrier where web.ship_mode.carrier is not null;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
   raw.web_sales as web; select ??? distinct web.ship_mode.carrier...
  ```
- `trilogy file write answer_755724379.preql --content import raw.store_sales as ss;

# Web sale billing customers from Apr-Jun 2001
with web_billers as
  impor…cnt2,
  count(grain(ss.customer.sk)) as cnt3
order by
  gender,
  marital_status,
  education_status,
  purchase_estimate,
  credit_rating
limit 100;`

  ```text
  refused to write 'answer_755724379.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:3
    |
  5 |   import raw.web_sales as ws
    |   ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, or tvf_intersect_invocation
  Location:
  ...un 2001 with web_billers as   ??? import raw.web_sales as ws whe...

  Write stats: received 1274 chars / 1274 bytes; tail: …'tion_status,\\n  purchase_estimate,\\n  credit_rating\\nlimit 100;'.
  ```
- `trilogy file write answer_2874833976.preql --content import raw.store_sales as ss;

# Step 1: Define top 5 states by total store net profit in year 2000
auto…) over (partition by grouping(ss.store.state) + grouping(ss.store.county) + coalesce(ss.store.state, '') order by sum(ss.net_profit) desc)
limit 100;`

  ```text
  refused to write 'answer_2874833976.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:3
    |
  5 |   select
    |   ^---
    |
    = expected _and_conditional
  Location:
  ...ear 2000 auto top_states <-   ??? select     ss.store.state,

  Write stats: received 852 chars / 852 bytes; tail: …"tore.state, '') order by sum(ss.net_profit) desc)\\nlimit 100;".
  ```
- `trilogy run --import raw.catalog_sales:cs select cs.promotion.sk, count(grain(cs.order_number, cs.item.sk)) as cnt group by 1 order by 2 desc limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...r_number, cs.item.sk)) as cnt ??? group by 1 order by 2 desc lim...
  ```
- `trilogy file write answer_3840922834.preql -e -c import raw.store_sales as ss;
import raw.web_sales as ws;

# Store sales totals per customer per year
with s…here store_totals.cust_id is not null
  and web_totals.cust_id is not null
  and store_totals.yr in (2001, 2002)
  and web_totals.yr in (2001, 2002)
`

  ```text
  refused to write 'answer_3840922834.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...fname,     store_totals.lname ??? subset join store_totals.cust_...

  Write stats: received 918 chars / 918 bytes; tail: …'tals.yr in (2001, 2002)\\n  and web_totals.yr in (2001, 2002)\\n'.
  ```
- `trilogy file write answer_747155668.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as cat…channel nulls first,
  combined.missing_ref nulls first,
  combined.yr nulls first,
  combined.qtr nulls first,
  combined.cat nulls first
limit 100;`

  ```text
  refused to write 'answer_747155668.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...int, total_ext_price numeric?) ???  where combined.cat is not nu...

  Write stats: received 2528 chars / 2528 bytes; tail: …'bined.qtr nulls first,\\n  combined.cat nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_3210116865.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales a… combined.channel asc nulls first,
    grouping(combined.outlet) asc,
    combined.outlet asc nulls first,
    sum(combined.returns) desc
limit 100;
`

  ```text
  refused to write 'answer_3210116865.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...::int, sales, returns, profit ??? from store_combined),     (sel...

  Write stats: received 3971 chars / 3971 bytes; tail: …' asc nulls first,\\n    sum(combined.returns) desc\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3210116865.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales a…asc,
    all_channels.channel asc,
    grouping(all_channels.outlet) asc,
    all_channels.outlet asc,
    sum(all_channels.returns) desc
limit 100;
`

  ```text
  refused to write 'answer_3210116865.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...c(15,2), profit numeric(15,2)) ??? select     all_channels.chann...

  Write stats: received 4301 chars / 4301 bytes; tail: …'s.outlet asc,\\n    sum(all_channels.returns) desc\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, count(grain(all_sales.item.sk, all_sales.order_id, all_sales.channel)) as rows where all_sales.is_returned is not null and all_sales.return_date.week_seq in (5244, 5257, 5264) by all_sales.channel;`

  ```text
  Syntax error in stdin: Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...eek_seq in (5244, 5257, 5264) ??? by all_sales.channel;
  ```
- `trilogy run --import raw.web_sales:ws select ws.item.category, ws.item.class, sum(ws.net_paid) as total where ws.sale_date.year = 2000 and ws.item.category = 'Men' group by rollup (ws.item.category, ws.item.class) order by sum(ws.net_paid) desc`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   and ws.item.category = 'Men' ??? group by rollup (ws.item.categ...
  ```
- `trilogy run --import raw.reason:reason select distinct reason.desc limit 50;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
   raw.reason as reason; select ??? distinct reason.desc limit 50;...
  ```

### `cli-misuse`

- `trilogy explore raw/ --regex item`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw`

  ```text
  Invalid value for 'PATH': File 'raw' is a directory.
  ```

### `type-error`

- `trilogy run answer_2874833976.preql`

  ```text
  Type error in answer_2874833976.preql: Invalid argument type Trait<STRING, ['us_state_short']>' passed into ADD function from function COALESCE in position 2. Valid: 'BIGINT', 'DOUBLE', 'FLOAT', 'INTEGER', 'NUMBER', 'NUMERIC'
  ```
