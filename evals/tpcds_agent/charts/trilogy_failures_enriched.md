# Trilogy failure analysis — 20260701-013044

- Run `20260701-013044` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 282 | failed: 42 (15%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 23 | 55% |
| `other` | 14 | 33% |
| `syntax-missing-alias` | 3 | 7% |
| `type-error` | 1 | 2% |
| `cli-misuse` | 1 | 2% |

## Detail

### `syntax-parse`

- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, count(all_sales.order_id) as cnt by rollup all_sales.channel;`

  ```text
  Syntax error in stdin:  --> 2:60
    |
  2 | select all_sales.channel, count(all_sales.order_id) as cnt by rollup all_sales.channel;
    |                                                            ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...nt(all_sales.order_id) as cnt ??? by rollup all_sales.channel;
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, count(all_sales.order_id) as cnt, count(all_sales.order_id ? all_sales.return_channel_dim_id is not null) as has_return_entity where all_sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date by all_sales.channel;`

  ```text
  Syntax error in stdin: Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...::date and '2000-09-06'::date ??? by all_sales.channel;
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, count(all_sales.order_id) as cnt, count(all_sales.order_id ? all_sales.return_channel_dim_id is not null) as has_return_entity where all_sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date by all_sales.channel;`

  ```text
  Syntax error in stdin: Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...::date and '2000-09-06'::date ??? by all_sales.channel;
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, count(all_sales.order_id) as cnt where all_sales.return_channel_dim_id is not null and all_sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date by all_sales.channel;`

  ```text
  Syntax error in stdin: Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...::date and '2000-09-06'::date ??? by all_sales.channel;
  ```
- `trilogy run --import raw.all_sales:all_sales select distinct all_sales.return_channel_dim_text_id where all_sales.channel = 'WEB' and all_sales.return_channel_dim_id is not null limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...ll_sales as all_sales; select ??? distinct all_sales.return_chan...
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.store.text_id, sum(store_sales.ext_sales_price) as ext_sales, sum(store_sales.return_amou…_sales.net_profit) as net_profit where store_sales.date.date between '2000-08-23'::date and '2000-09-06'::date by store_sales.store.text_id limit 20;`

  ```text
  Syntax error in stdin: Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...::date and '2000-09-06'::date ??? by store_sales.store.text_id l...
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.return_channel_dim_text_id where all_sales.channel = 'CATALOG' and all_sales.return_channel_dim_id is not null and all_sales.return_channel_dim_text_id in (select store.text_id from store) limit 10;`

  ```text
  Syntax error in stdin: Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries - joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...return_channel_dim_text_id in ??? (select store.text_id from sto...
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id as sale_entity, all_sales.return_channel_dim_text_id as …ate between '2000-08-23'::date and '2000-09-06'::date and all_sales.return_channel_dim_text_id not in (select all_sales.channel_dim_text_id) limit 5;`

  ```text
  Syntax error in stdin: Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries - joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...rn_channel_dim_text_id not in ??? (select all_sales.channel_dim_...
  ```
- `trilogy run --import raw.all_sales:all_sales select 
    case when grouping(s.channel) = 1 then null
         when s.channel = 'STORE' then 'store channel'
 …d '2000-09-06'::date and s.channel_dim_id is not null
order by _level asc,
    channel_label asc nulls first,
    entity_id asc nulls first
limit 10;`

  ```text
  Syntax error in stdin:   --> 16:1
     |
  16 | where s.date.date between '2000-08-23'::date and '2000-09-06'::date and s.channel_dim_id is not null
     | ^---
     |
     = expected limit or order_by
  Location:
  ...annel, s.channel_dim_text_id) ??? where s.date.date between '200...
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…tem.brand_id in (
        select web_sales.item.brand_id
        where web_sales.date.year >= 1999 and web_sales.date.year <= 2001
    )
;

select 1;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries - joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
   store_sales.item.brand_id in ??? (         select catalog_sales...

  Write stats: received 1778 chars / 1778 bytes; tail: …'r >= 1999 and web_sales.date.year <= 2001\\n    )\\n;\\n\\nselect 1;'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Stack all three chann…s_1999_2001.channel in ('STORE') and
    all_channels_1999_2001.channel in ('CATALOG') and
    all_channels_1999_2001.channel in ('WEB')
;

select 1;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 43:9
     |
  43 | select 1;
     |         ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...channel in ('WEB') ;  select 1 ??? ;

  Write stats: received 1307 chars / 1307 bytes; tail: …"d\\n    all_channels_1999_2001.channel in ('WEB')\\n;\\n\\nselect 1;".
  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Find distinct…gory_id
select
    store_combos.brand_id as brand_id,
    store_combos.class_id as class_id,
    store_combos.category_id as category_id
;

select 1;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 45:9
     |
  45 | select 1;
     |         ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ..._id as category_id ;  select 1 ??? ;

  Write stats: received 1365 chars / 1365 bytes; tail: …'id,\\n    store_combos.category_id as category_id\\n;\\n\\nselect 1;'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Find qualifying (bran…el.brand_id asc nulls first,
         nov2001_by_channel.class_id asc nulls first,
         nov2001_by_channel.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries - joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...001_by_channel.total_sales) > ??? (select overall_avg.avg_sale_v...

  Write stats: received 4310 chars / 4310 bytes; tail: …'   nov2001_by_channel.category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Overall average
rowse…id)
order by bc.channel asc nulls first, bc.brand_id asc nulls first,
         bc.class_id asc nulls first, bc.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries - joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
   having sum(bc.total_sales) > ??? (select overall_avg.avg_val) b...

  Write stats: received 2040 chars / 2040 bytes; tail: …'d asc nulls first, bc.category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;

# Test: auto concept in having with rollup
auto total_sales_val <- sum(ss.quantity …l_count_val
where ss.date.year = 2001 and ss.date.month_of_year = 11
by rollup (ss.item.brand_id)
order by ss.item.brand_id asc nulls first
limit 20;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:1
     |
  12 | where ss.date.year = 2001 and ss.date.month_of_year = 11
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...l_sales_val / total_count_val ??? where ss.date.year = 2001 and

  Write stats: received 604 chars / 604 bytes; tail: …'rand_id)\\norder by ss.item.brand_id asc nulls first\\nlimit 20;'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Overall average
rowse…s.quantity * ws.list_price) as sales, count(ws.line_item) as cnt)
) -> (channel, brand_id, class_id, category_id, total_sales, num_sales);

select 1;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 34:1
     |
  34 | with bc as union(
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...l_sales > overall_avg.avg_val ??? with bc as union(     (where s...

  Write stats: received 2674 chars / 2674 bytes; tail: …', class_id, category_id, total_sales, num_sales);\\n\\nselect 1;'.
  ```
- `trilogy run --import raw.store_sales:store select store.customer.id, sum(store.ext_sales_price) as total where store.date.month_seq between 1188 and 1190 and store.store.county = store.customer.address.county and store.store.state = store.customer.address.state group by store.customer.id;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   store.customer.address.state ??? group by store.customer.id;
  ```
- `trilogy run --import raw/date:date select date.year, min(date.week_seq) as min_ws, max(date.week_seq) as max_ws where date.week_seq between 5270 and 5375 group by date.year order by date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...eek_seq between 5270 and 5375 ??? group by date.year order by da...
  ```
- `trilogy file write query59_debug.preql --content import raw.store_sales as ss;

def day_sum(dow) -> sum(ss.sales_price ? ss.date.day_of_week = dow);

with th…de,
    ss.date.week_seq as week_seq
    ## note: adding days here would add to grain
;

select count(this_year.store_name) as n_rows from this_year;`

  ```text
  refused to write 'query59_debug.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...is_year.store_name) as n_rows ??? from this_year;

  Write stats: received 366 chars / 366 bytes; tail: …'select count(this_year.store_name) as n_rows from this_year;'.
  ```
- `trilogy file write query59_debug2.preql --content import raw.store_sales as ss;

def day_sum(dow) -> sum(ss.sales_price ? ss.date.day_of_week = dow);

with t…_seq + 52 = next_year.week_seq
    inner join this_year.store_code = next_year.store_code
    inner join this_year.store_name = next_year.store_name;`

  ```text
  refused to write 'query59_debug2.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
   @day_sum(0) as sun ;  select ??? count(*) as n_total     inner

  Write stats: received 666 chars / 666 bytes; tail: …'\\n    inner join this_year.store_name = next_year.store_name;'.
  ```
- `trilogy run --import raw/date:date select date.date, date.week_seq from date where date.date in ('2000-06-30'::date, '2000-09-27'::date, '2000-11-17'::date);`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...lect date.date, date.week_seq ??? from date where date.date in (...
  ```
- `trilogy run --import raw/item:i where i.text_id in (select i2.text_id from item as i2 group by i2.text_id having count(i2.id) > 1) select i.text_id as item_code, count(i.id) as version_count limit 10;`

  ```text
  Syntax error in stdin: Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries - joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...item as i; where i.text_id in ??? (select i2.text_id from item a...
  ```
- `trilogy run --import raw/item:i select i.text_id as item_code, count(i.id) as version_count group by i.text_id order by version_count desc limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   count(i.id) as version_count ??? group by i.text_id order by ve...
  ```

### `other`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, count(all_sales.order_id) as cnt where all_sales.channe…rollup (all_sales.channel, all_sales.channel_dim_text_id) order by all_sales.channel nulls first, all_sales.channel_dim_text_id nulls first limit 20;`

  ```text
  Syntax error in stdin: Comparison `all_sales.channel in ('STORE', 'WEB', 'CATALOG')` matches every value of enum field 'all_sales.channel', which contains only these values: 'WEB', 'CATALOG', 'STORE'. It is always true and should be removed.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(channel_data.channel_label)` (line 72), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(channel_data.channel_label) as g order by g desc`.
  ```
- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {bc.brand_id, bc.category_id, bc.channel, bc.class_id, threshold.avg_val}
  ```
- `trilogy run query14.preql`

  ```text
  Unexpected error in query14.preql: Unsupported datatype NUMERIC for parameter overall_avg_threshold.
  ```
- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {ss.item.brand_id, ss.item.category_id, ss.item.class_id, ss.item.id, ss.list_price, ss.quantity, ss.ticket_number}
  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/repro.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query54.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write -c query59_test.preql import raw.store_sales as ss;

def day_sum(dow) -> sum(ss.sales_price ? ss.date.day_of_week = dow);

where ss.date.y… @day_sum(1) as mon,
    @day_sum(2) as tue,
    @day_sum(3) as wed,
    @day_sum(4) as thu,
    @day_sum(5) as fri,
    @day_sum(6) as sat
limit 10;`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run query75.preql duckdb`

  ```text
  Syntax error in query75.preql: Undefined concept: y2002.yearly.total_amt (line 51, in SELECT). Suggestions: ['yearly.total_amt', '_yearly_total_amt', 'yearly.total_qty']
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query80.preql`

  ```text
  Syntax error in query80.preql: ORDER BY contains aggregate `grouping(combined.channel_label)` (line 44), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel_label) as g order by g desc`.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/date:date select min(date.week_seq), max(date.week_seq) where date.year = 2001;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `min(date.week_seq) as week_seq_min`
  Location:
  ...ate; select min(date.week_seq) ??? , max(date.week_seq) where dat...
  ```
- `trilogy run --import raw/date:date select min(date.week_seq), max(date.week_seq) where date.year = 2002;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `min(date.week_seq) as week_seq_min`
  Location:
  ...ate; select min(date.week_seq) ??? , max(date.week_seq) where dat...
  ```
- `trilogy run --import raw/all_sales:s select s.item.text_id, s.channel, count(s.order_id), sum(s.return_quantity) from s where s.return_date.week_seq in (5244, 5257, 5264) limit 20;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(s.order_id) as order_id_count`
  Location:
  ..., s.channel, count(s.order_id) ??? , sum(s.return_quantity) from
  ```

### `type-error`

- `trilogy run --import raw.all_sales:all_sales select all_sales.date.date, count(all_sales.order_id) as cnt where all_sales.date.date between '2000-08-23' and '2000-09-06' limit 10;`

  ```text
  Syntax error in stdin: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```

### `cli-misuse`

- `trilogy run  select 1 as a;`

  ```text
  'select 1 as a;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
