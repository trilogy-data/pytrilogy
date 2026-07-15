# Trilogy failure analysis — 20260715-131148

- Run `explore_namespaced_top10_restart_20260715-091146_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 273 | failed: 20 (7%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 9 | 45% |
| `other` | 6 | 30% |
| `syntax-missing-alias` | 3 | 15% |
| `cli-misuse` | 1 | 5% |
| `join-resolution` | 1 | 5% |

## Detail

### `syntax-parse`

- `trilogy file write answer_765177085.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

where ss.date.year = 2001
  and ss.return_…nt(cs.quantity) as cat_qty_count,
    count(*) as total_count
union join ss.customer.sk = cs.billing_customer.sk
union join ss.item.sk = cs.item.sk;
`

  ```text
  refused to write 'answer_765177085.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a NON-NULL GRAIN KEY: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). It MUST be a key, and one that is not nullable: `count(x)` skips rows where `x` is NULL, so counting a nullable property (a name, a date, any optional field) silently undercounts. When the grain takes SEVERAL keys, name them with `grain(...)`: `count(grain(order_id, item.id))` counts order+item combinations, and `count_distinct(grain(first_name, last_name, sale_date))` counts distinct combinations - `grain()` is never NULL, so combinations with a missing member still count. For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...antity) as cat_qty_count,     ??? count(*) as total_count union

  Write stats: received 450 chars / 450 bytes; tail: …' cs.billing_customer.sk\\nunion join ss.item.sk = cs.item.sk;\\n'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.item as item;
import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sale…um(combined.total_sales) as total_sales
order by
    combined.last_name asc,
    combined.first_name asc,
    total_sales asc nulls first
limit 100
;`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 86:6
     |
  86 |     (cat_feb2000),
     |      ^---
     |
     = expected select_statement
  Location:
   with combined as union(     ( ??? cat_feb2000),     (web_feb2000...

  Write stats: received 3286 chars / 3286 bytes; tail: …'.first_name asc,\\n    total_sales asc nulls first\\nlimit 100\\n;'.
  ```
- `trilogy run --import raw/date:d select distinct d.month_seq, d.year, d.month_of_year where d.month_seq between 1188 and 1190 order by d.month_seq;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  import raw.date as d; select ??? distinct d.month_seq, d.year,
  ```
- `trilogy file write answer_3544057080.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# ---- Rowset: catalog-level cumulative m….sale_lines <= y1999.sale_lines
order by y1999.product_name, y1999.store_name, y2000.sale_lines, y1999.wholesale_sum, y2000.wholesale_sum
limit 200;
`

  ```text
  refused to write 'answer_3544057080.preql': not syntactically valid Trilogy.

  Parse error:
    --> 52:1
     |
  52 | rowset y1999 as
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...: self-pair 1999 vs 2000 ---- ??? rowset y1999 as where sales_ag...

  Write stats: received 4199 chars / 4199 bytes; tail: …'_lines, y1999.wholesale_sum, y2000.wholesale_sum\\nlimit 200;\\n'.
  ```
- `trilogy run --import raw.catalog_sales:cs select cs.is_returned as ret, cs.return_quantity as qty, cs.return_date.week_seq as wk, cs.item.id as iid limit 5 where cs.is_returned = true and cs.return_date.week_seq in (5244, 5257, 5264);`

  ```text
  Syntax error in stdin:  --> 2:107
    |
  2 | select cs.is_returned as ret, cs.return_quantity as qty, cs.return_date.week_seq as wk, cs.item.id as iid limit 5 where cs.is_returned = true and cs.return_date.week_seq in (5244, 5257, 5264);
    |                                                                                                           ^---
    |
    = expected metadata, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ..._seq as wk, cs.item.id as iid ??? limit 5 where cs.is_returned =...
  ```
- `trilogy run --import raw.web_sales:ws select ws.is_returned as ret, ws.return_quantity as qty, ws.return_date.week_seq as wk, ws.item.id as iid limit 5 where ws.is_returned = true and ws.return_date.week_seq in (5244, 5257, 5264);`

  ```text
  Syntax error in stdin:  --> 2:107
    |
  2 | select ws.is_returned as ret, ws.return_quantity as qty, ws.return_date.week_seq as wk, ws.item.id as iid limit 5 where ws.is_returned = true and ws.return_date.week_seq in (5244, 5257, 5264);
    |                                                                                                           ^---
    |
    = expected metadata, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ..._seq as wk, ws.item.id as iid ??? limit 5 where ws.is_returned =...
  ```
- `trilogy run --import raw.store_sales:ss select ss.customer.current_address.city, count(1) as cnt where ss.is_returned = true group by ss.customer.current_address.city order by cnt desc limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...t where ss.is_returned = true ??? group by ss.customer.current_a...
  ```
- `trilogy run --import raw.store_sales:ss select ss.customer.id as customer_code, concat(ss.customer.last_name, ', ', ss.customer.first_name) as full_name wher…return_customer_demographic.sk from raw.store_sales as ss2 where ss2.is_returned = true and ss2.return_customer_demographic.sk is not null) limit 20;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...eturn_customer_demographic.sk ??? from raw.store_sales as ss2 wh...
  ```
- `trilogy file write _check_nonnull.preql --content import raw.store_sales as ss;

where ss.return_reason.desc = 'reason 28'

select
    ss.customer.id,
    su…tity * ss.sales_price
        end
    ) as adjusted_total
order by adjusted_total asc nulls first, ss.customer.id asc nulls first
limit 5
offset 260;`

  ```text
  refused to write '_check_nonnull.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:1
     |
  14 | order by adjusted_total asc nulls first, ss.customer.id asc nulls first
     | ^---
     |
     = expected metadata, where, having, select_grouping, or JOIN_TYPE
  Location:
    end     ) as adjusted_total ??? order by adjusted_total asc nu...

  Write stats: received 474 chars / 474 bytes; tail: …'ls first, ss.customer.id asc nulls first\\nlimit 5\\noffset 260;'.
  ```

### `other`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Syntax error in answer_3705756794.preql: `by rollup (…)` requires at least one aggregate (or re-aggregable pre-aggregated measure) in the select to group; found none.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Syntax error in answer_3544057080.preql: Nothing was executed: parsed 5 definition statement(s) (3 rowsets, 2 imports) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 4475 (char 4474). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 97 column 12 (char 5383). Re-issue the call with valid JSON arguments.
  ```

### `syntax-missing-alias`

- `trilogy file write answer_3979964698.preql --content import raw.all_sales as s;

# Filter conditions: year 2001, time between 30838-59638, DHL/BARIAN carrier…then s.quantity * s.net_paid_inc_tax
        else 0 end) as m12_net
order by s.warehouse.name asc nulls first, s.date.year asc nulls first
limit 100;`

  ```text
  refused to write 'answer_3979964698.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `hidden warehouse sk (not displayed)
      --s.warehouse.sk as hidden_warehouse_sk_not_displayed_s_ware`
  Location:
  ...TALOG')  select     -- hidden ??? warehouse sk (not displayed)

  Write stats: received 9748 chars / 9748 bytes; tail: …'name asc nulls first, s.date.year asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.catalog_sales:cs select cs.is_returned, cs.return_quantity, cs.return_date.week_seq, cs.item.id limit 5 where cs.is_returned = true and cs.return_date.week_seq in (5244, 5257, 5264);`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `cs.item.id as cs_item_id`
  Location:
  ...urn_date.week_seq, cs.item.id ??? limit 5 where cs.is_returned =...
  ```
- `trilogy run --import raw.web_sales:ws select ws.is_returned, ws.return_quantity, ws.return_date.week_seq, ws.item.id limit 5 where ws.is_returned = true and ws.return_date.week_seq in (5244, 5257, 5264);`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `ws.item.id as ws_item_id`
  Location:
  ...urn_date.week_seq, ws.item.id ??? limit 5 where ws.is_returned =...
  ```

### `cli-misuse`

- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```

### `join-resolution`

- `trilogy run answer_2928586490.preql`

  ```text
  Resolution error in answer_2928586490.preql: Could not resolve connections for query with output ['item.sk<Purpose.KEY>Derivation.ROOT>', 'local._frequent_items_trip_cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
