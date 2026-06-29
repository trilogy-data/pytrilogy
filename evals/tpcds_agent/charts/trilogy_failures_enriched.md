# Trilogy failure analysis — 20260629-013151

- Run `20260629-013151` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 261 | failed: 24 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 11 | 46% |
| `syntax-parse` | 11 | 46% |
| `join-resolution` | 1 | 4% |
| `syntax-missing-alias` | 1 | 4% |

## Detail

### `other`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query05.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(sales.channel)` (line 23), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(sales.channel) as g order by g desc`.
  ```
- `trilogy file read query23.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: catalog_returns. Suggestions: ['catalog_returns.time.id', 'catalog_returns.item.id', 'catalog_returns.date.id']
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Ambiguous reference 'store_agg.year': matches ['store_agg.store_sales.customer.first_sales_date.year', 'store_agg.store_sales.customer.first_shipto_date.year', 'store_agg.store_sales.date.year']. Qualify the full path to disambiguate.
  ```
- `trilogy run query70.preql`

  ```text
  Syntax error in query70.preql: grouping()/grouping_id() cannot be used in a WHERE clause: WHERE is evaluated before grouping, so there is no grouping set to anchor to. It is a post-aggregate level indicator - use it in SELECT / HAVING / ORDER BY of a query carrying a `by rollup/cube/grouping sets` clause (e.g. filter subtotal rows in HAVING: `having grouping(state) = 1`).
  ```
- `trilogy run --import raw.all_sales:sales --import raw.item:item select sales.item.id, sales.channel, sales.order_id, sales.quantity, sales.return_quantity, sales.ext_sales_price, sales.return_amount, sales.is_returned, item.brand_id, item.class_id, item.category_id, item.manufacturer_id limit 20;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {item.brand_id, item.category_id, item.class_id, item.manufacturer_id}; {sales.channel, sales.ext_sales_price, sales.is_returned, sales.item.id, sales.order_id, sales.quantity, sales.return_amount, sales.return_quantity}.
    - `item.brand_id` is disconnected — did you mean `sales.item.brand_id`? (connected to the other concepts)
    - `item.category_id` is disconnected — did you mean `sales.item.category_id`? (connected to the other concepts)
    - `item.class_id` is disconnected — did you mean `sales.item.class_id`? (connected to the other concepts)
    - `item.manufacturer_id` is disconnected — did you mean `sales.item.manufacturer_id`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `sales.item.brand_id`) instead of importing a second, disconnected copy.
  ```
- `trilogy run query78.preql`

  ```text
  Syntax error in query78.preql: Undefined concept: cat.cat_qty. Suggestions: ['catalog.cat_qty', '_catalog_cat_qty']
  ```
- `trilogy run query78.preql`

  ```text
  Syntax error in query78.preql: 8 undefined concept references; fix all before re-running:
    - store.other_qty (line 49, in SELECT); did you mean: store.store_qty, _store_store_qty, store.other_wholesale?
    - store.other_qty (line 58, col 5, in SELECT); did you mean: store.store_qty, _store_store_qty, store.other_wholesale?
    - store.other_wholesale (line 59, col 5, in SELECT); did you mean: store.store_wholesale, _store_store_wholesale, store.other_sales_price?
    - store.other_sales_price (line 60, col 5, in SELECT); did you mean: store.store_sales_price, _store_store_sales_price, ws.ext_sales_price?
    - store.other_qty (line 49, col 7, in WHERE); did you mean: store.store_qty, _store_store_qty, store.other_wholesale?
    - store.other_qty (line 71, col 5, in ORDER BY); did you mean: store.store_qty, _store_store_qty, store.other_wholesale?
    - store.other_wholesale (line 72, col 5, in ORDER BY); did you mean: store.store_wholesale, _store_store_wholesale, store.other_sales_price?
    - store.other_sales_price (line 73, col 5, in ORDER BY); did you mean: store.store_sales_price, _store_store_sales_price, ws.ext_sales_price?
  ```

### `syntax-parse`

- `trilogy run --import raw.all_sales:all_sales select count(*) as cnt where all_sales.return_amount is null and all_sales.return_net_loss is not null limit 10;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...ll_sales as all_sales; select ??? count(*) as cnt where all_sale...
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Concatenated key for (brand, class, category) combination
auto bcc_key <- concat…ass_id) asc,
    sales.item.class_id asc nulls first,
    grouping(sales.item.category_id) asc,
    sales.item.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:5
    |
  8 |     where sales.date.year between 1999 and 2001;
    |     ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or window_sql_over
  Location:
   * sales.list_price) by *     ??? where sales.date.year between

  Write stats: received 1509 chars / 1509 bytes; tail: …') asc,\\n    sales.item.category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:st select count(st.item.id) as cnt, st.item.desc, st.date.id where st.date.year between 2000 and 2003 group by st.item.id, substring(st.item.desc,1,30), st.date.id limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...te.year between 2000 and 2003 ??? group by st.item.id, substring...
  ```
- `trilogy run --import raw.store_sales:st --import raw.catalog_sales:cs select cs.bill_customer.last_name, cs.bill_customer.first_name, sum(cs.quantity * cs.li… cs.date.year = 2000 and cs.date.month_of_year = 2 and cs.item.id in (select st.item.id as item_id where st.date.year between 2000 and 2003) limit 5;`

  ```text
  Syntax error in stdin: Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...of_year = 2 and cs.item.id in ??? (select st.item.id as item_id
  ```
- `trilogy file write query38.preql --content import raw.all_sales as s;

# First, find (last_name, first_name, sale_date, channel) combos in year 2000
# Then c…   s.date.date,
    count_distinct(s.channel) as channel_count
having
    channel_count = 3
;

select
    count(*) as unique_combinations
limit 100;
`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...annel_count = 3 ;  select     ??? count(*) as unique_combination...

  Write stats: received 505 chars / 505 bytes; tail: …' 3\\n;\\n\\nselect\\n    count(*) as unique_combinations\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.all_sales:s select s.date.year, count(s.channel) as cnt group by 1 limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...year, count(s.channel) as cnt ??? group by 1 limit 5;
  ```
- `trilogy run --import raw.store_sales:ss where ss.date.year = 2000 and ss.customer.last_name is not null select ss.customer.last_name, ss.customer.first_name, ss.date.date, count(ss.ticket_number) as num_sales group by 1, 2, 3 limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...s.ticket_number) as num_sales ??? group by 1, 2, 3 limit 5;
  ```
- `trilogy file write query64.preql --content import raw.store_sales as store_sales;
import raw.catalog_returns as catalog_returns;

# Rowset: compute items tha…d r2.sale_year = 2000
  and r2.cnt <= r1.cnt
order by
    r1.product_name,
    r1.store_name,
    r2.cnt,
    r1.wholesale_sum,
    r2.wholesale_sum;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 68:22
     |
  68 | inner join store_row r1 = store_row r2
     |                      ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
    r2.cnt inner join store_row ??? r1 = store_row r2     on r1.it...

  Write stats: received 2801 chars / 2801 bytes; tail: …'ame,\\n    r2.cnt,\\n    r1.wholesale_sum,\\n    r2.wholesale_sum;'.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as ss;

# Filter to year 2000
where ss.date.year = 2000

# Grouping helpers
auto g_county <…t_profit desc) as rnk
by rollup (ss.store.state, ss.store.county)
order by level desc, ss.store.state asc nulls first, rnk asc nulls first
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...ar = 2000  # Grouping helpers ??? auto g_county <- grouping(ss.s...

  Write stats: received 1023 chars / 1023 bytes; tail: …'.store.state asc nulls first, rnk asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.all_sales:sales --import raw.item:item select sales.item.id, sales.channel, sales.order_id, sales.quantity, sales.return_quantity, sales.ext_sales_price, sales.return_amount, sales.is_returned, item.brand_id, item.class_id, item.category_id, item.manufacturer_id from sales limit 20;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...gory_id, item.manufacturer_id ??? from sales limit 20;
  ```
- `trilogy run --import raw.all_sales:sales select sales.date.year, sales.item.category, count(sales.item.id) from sales where sales.date.year in (2001, 2002) group by 1, 2 limit 30;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...ategory, count(sales.item.id) ??? from sales where sales.date.ye...
  ```

### `join-resolution`

- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local._virt_filter_ticket_number_8426183738541935<Purpose.PROPERTY>Derivation.FILTER>'] from current model.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.all_sales:sales select sales.date.year, sales.item.category, count(sales.item.id) limit 30;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(sales.item.id) as id_count`
  Location:
  ...ategory, count(sales.item.id) ??? limit 30;
  ```
