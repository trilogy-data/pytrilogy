# Trilogy failure analysis — 20260701-033309

- Run `20260701-033309` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 239 | failed: 31 (13%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 18 | 58% |
| `syntax-parse` | 10 | 32% |
| `cli-misuse` | 3 | 10% |

## Detail

### `other`

- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(combined.channel_type)` (line 40), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel_type) as g order by g desc`.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: Undefined concept: s.line_item. Suggestions: ['s.sale_line_item_counter', 's.net_profit', 's.item.id']
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(per_channel.channel)` (line 90), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(per_channel.channel) as g order by g desc`.
  ```
- `trilogy run test_rollup12.preql`

  ```text
  Resolution error in test_rollup12.preql: Cannot resolve cross-rowset INNER join ch.s.channel = overall_stats.overall_avg: it intersects two independent rowsets but the collapse dropped overall_stats.overall_avg, silently losing the intersection. Rewrite the intersection as a `union(...)` of the arms with a channel marker, then keep tuples whose `count_distinct(channel)` equals the number of arms.
  ```
- `trilogy file delete test_rollup5.preql`

  ```text
  No such path: test_rollup5.preql
  ```
- `trilogy file delete test_rollup11.preql`

  ```text
  No such path: test_rollup11.preql
  ```
- `trilogy file delete test_rollup13.preql`

  ```text
  No such path: test_rollup13.preql
  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query59.preql`

  ```text
  Resolution error in query59.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {next_year.weekly.friday, next_year.weekly.monday, next_year.weekly.saturday, next_year.weekly.store_id, next_year.weekly.sunday, next_year.weekly.thursday, next_year.weekly.tuesday, next_year.weekly.wednesday, next_year.weekly.week_seq, this_year.weekly.friday, this_year.weekly.monday, this_year.weekly.saturday, this_year.weekly.store_code, this_year.weekly.store_id, this_year.weekly.store_name, this_year.weekly.sunday, this_year.weekly.thursday, this_year.weekly.tuesday, this_year.weekly.wednesday, this_year.weekly.week_seq}
  ```
- `trilogy run query75.preql`

  ```text
  Unexpected error in query75.preql: Join chain repeats source `year2001` (keys `year2001.by_year_dims.line_level.brand_id` and `year2001.by_year_dims.line_level.class_id`). A `=` chain joins ONE key across distinct sources; join a composite key with `and` or separate clauses (e.g. `a.k1 = b.k1 and a.k2 = b.k2`).
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: 2 undefined concept references; fix all before re-running:
    - year2002.by_year_dims.net_amt (line 58, in SELECT); did you mean: by_year_dims.net_amt, year2001.by_year_dims.net_amt, _by_year_dims_net_amt?
    - year2001.by_year_dims.net_amt (line 58, in SELECT); did you mean: by_year_dims.net_amt, year2002.by_year_dims.net_amt, _by_year_dims_net_amt?
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 7). The requested concepts split into 2 disconnected subgraphs: {item.category}; {local._line_level_amt_returned, local._line_level_amt_sold, local._line_level_brand_id, local._line_level_category_id, local._line_level_class_id, local._line_level_manufacturer_id, local._line_level_qty_returned, local._line_level_qty_sold, local._line_level_year}.
    - `item.category` is disconnected — did you mean `sales.item.category`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `sales.item.category`) instead of importing a second, disconnected copy.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs --import raw.web_sales:ws select ss.channel;`

  ```text
  Syntax error in stdin: Undefined concept: ss.channel (line 4, col 8, in SELECT). Suggestions: ['ss.promotion.channel_tv']
  ```
- `trilogy run query83.preql`

  ```text
  Syntax error in query83.preql: ORDER BY references 'sq.store_qty', a measure at a finer grain (Grain<sq.sr.item.text_id>) than the select grain (Grain<local.catalog_pct,local.catalog_return_qty,local.store_pct,local.three_channel_avg,local.web_pct,local.web_return_qty,wq.wr.web_sales.item.text_id>|sq.store_rows > 0 and cq.catalog_rows > 0 and wq.web_rows > 0); it has no single value per output row. Project it (prefix with `--` to keep it out of the rows) and order by that alias instead.
  ```
- `trilogy run query86.preql`

  ```text
  Syntax error in query86.preql: All case expressions must have the same output datatype, got {<DataType.STRING: 'string'>, <DataType.INTEGER: 'int'>} from {'-1': <DataType.INTEGER: 'int'>, 'coalesce(ref:ws.item.category,Unknown)': EnumType(type=<DataType.STRING: 'string'>, values=['Books', 'Children', 'Electronics', 'Home', 'Jewelry', 'Men', 'Music', 'Shoes', 'Sports', 'Women'])}
  ```
- `trilogy run query86.preql`

  ```text
  Syntax error in query86.preql: grouping() requires a concept (column) reference as its argument, not an inline expression like 'coalesce(ref:ws.item.class,Unknown)'. Assign the expression to a named concept and use that concept in both the grouping key and grouping() - e.g. `auto channel <- coalesce(a, b); select ..., grouping(channel) ... by rollup (channel)`.
  ```
- `trilogy run query86.preql`

  ```text
  Unexpected error in query86.preql: (_duckdb.BinderException) Binder Error: GROUPING child "ws_item_category" must be a grouping column

  LINE 64:     grouping("questionable"."ws_item_category") as "g_cat",
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "ws_web_sales"."WS_NET_PAID" as "ws_net_paid",
      coalesce("ws_item_items"."I_CATEGORY",'Unknown') as "cat",
      coalesce("ws_item_items"."I_CLASS",'Unknown') as "cls"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
      INNER JOIN "item" as "ws_item_items" on "ws_web_sales"."WS_ITEM_SK" = "ws_item_items"."I_ITEM_SK"
  WHERE
      "ws_date_date"."D_YEAR" = 2000
  ),
  cooperative as (
  SELECT
      "ws_web_sales"."WS_ITEM_SK" as "ws_item_id",
      "ws_web_sales"."WS_SOLD_DATE_SK" as "ws_date_id"
  FROM
      "web_sales" as "ws_web_sales"
  GROUP BY
      1,
      2),
  juicy as (
  SELECT
      "cheerful"."cat" as "cat",
      "cheerful"."cls" as "cls",
      sum("cheerful"."ws_net_paid") as "total_net_paid"
  FROM
      "cheerful"
  GROUP BY
      ROLLUP (1, 2)),
  thoughtful as (
  SELECT
      "cheerful"."cat" as "cat",
      sum("cheerful"."ws_net_paid") as "_virt_agg_sum_6235724634964384"
  FROM
      "cheerful"
  GROUP BY
      1),
  questionable as (
  SELECT
      "ws_item_items"."I_CATEGORY" as "ws_item_category",
      "ws_item_items"."I_CLASS" as "ws_item_class",
      coalesce("ws_item_items"."I_CATEGORY",'Unknown') as "cat",
      coalesce("ws_item_items"."I_CLASS",'Unknown') as "cls"
  FROM
      "cooperative"
      INNER JOIN "date_dim" as "ws_date_date" on "cooperative"."ws_date_id" = "ws_date_date"."D_DATE_SK"
      INNER JOIN "item" as "ws_item_items" on "cooperative"."ws_item_id" = "ws_item_items"."I_ITEM_SK"
  WHERE
      "ws_date_date"."D_YEAR" = 2000

  GROUP BY
      1,
      2,
      3,
      4,
      "ws_item_items"."I_ITEM_SK"),
  abundant as (
  SELECT
      "questionable"."cat" as "cat",
      "questionable"."cls" as "cls",
      grouping("questionable"."ws_item_category") as "g_cat",
      grouping("questionable"."ws_item_class") as "g_cls"
  FROM
      "questionable"
  GROUP BY
      ROLLUP (1, 2)),
  uneven as (
  SELECT
      "abundant"."cls" as "cls",
      CASE
  	WHEN "abundant"."g_cls" = 1 and "abundant"."g_cat" = 1 THEN 2
  	WHEN "abundant"."g_cls" = 1 THEN 1
  	ELSE 0
  	END as "hierarchy_level",
      coalesce("abundant"."cat","thoughtful"."cat") as "cat",
      rank() over (partition by CASE
  	WHEN "abundant"."g_cls" = 1 and "abundant"."g_cat" = 1 THEN 2
  	WHEN "abundant"."g_cls" = 1 THEN 1
  	ELSE 0
  	END,CASE
  	WHEN "abundant"."g_cls" = 0 THEN coalesce("abundant"."cat","thoughtful"."cat")
  	ELSE null
  	END order by "thoughtful"."_virt_agg_sum_6235724634964384" desc ) as "rnk"
  FROM
      "abundant"
      INNER JOIN "thoughtful" on "abundant"."cat" is not distinct from "thoughtful"."cat")
  SELECT
      "juicy"."cat" as "cat",
      "juicy"."cls" as "cls",
      "juicy"."total_net_paid" as "total_net_paid",
      "uneven"."hierarchy_level" as "hierarchy_level",
      "uneven"."rnk" as "rnk"
  FROM
      "juicy"
      LEFT OUTER JOIN "uneven" on "juicy"."cat" = "uneven"."cat" AND "juicy"."cls" = "uneven"."cls"
  ORDER BY
      "uneven"."hierarchy_level" desc nulls first,
      "juicy"."cat" asc nulls first,
      "uneven"."rnk" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query86.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query14.preql --content import raw.all_sales as s;

# Step 1: Compute overall average sale value (qty * list_price) across all channels 19…vg_sale
order by 
    channel asc nulls first,
    brand_id asc nulls first,
    class_id asc nulls first,
    category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
     --> 103:1
      |
  103 | having sum(per_channel.total_sales) > overall_avg_sale
      | ^---
      |
      = expected limit or order_by
  Location:
  ..._id, per_channel.category_id) ??? having sum(per_channel.total_s...

  Write stats: received 3852 chars / 3852 bytes; tail: …' asc nulls first,\\n    category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Step 1: Compute overall average sale value (qty * list_price) across all channels 19…vg_sale
order by 
    channel asc nulls first,
    brand_id asc nulls first,
    class_id asc nulls first,
    category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
     --> 102:1
      |
  102 | having sum(per_channel.total_sales) > overall_avg_sale
      | ^---
      |
      = expected limit or order_by
  Location:
  ..._id, per_channel.category_id) ??? having sum(per_channel.total_s...

  Write stats: received 3784 chars / 3784 bytes; tail: …' asc nulls first,\\n    category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write test_rollup5.preql --content import raw.all_sales as s;

auto overall_avg_sale <- sum(s.quantity * s.list_price) by * / count(s.sale_line_…s.date.month_of_year = 11
by rollup (s.channel)
having sum(s.quantity * s.list_price) > overall_avg_sale
order by s.channel asc nulls first
limit 10;`

  ```text
  refused to write 'test_rollup5.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:1
     |
  11 | having sum(s.quantity * s.list_price) > overall_avg_sale
     | ^---
     |
     = expected limit or order_by
  Location:
  ...ar = 11 by rollup (s.channel) ??? having sum(s.quantity * s.list...

  Write stats: received 432 chars / 432 bytes; tail: …'verall_avg_sale\\norder by s.channel asc nulls first\\nlimit 10;'.
  ```
- `trilogy file write test_rollup11.preql --content import raw.all_sales as s;

with overall_stats as
where s.date.year between 1999 and 2001
select
    sum(s.q… sum(ch.sale_count) as total_count
by rollup (ch.channel)
having sum(ch.total_sales) > overall_avg_sale
order by ch.channel asc nulls first
limit 10;`

  ```text
  refused to write 'test_rollup11.preql': not syntactically valid Trilogy.

  Parse error:
    --> 25:1
     |
  25 | having sum(ch.total_sales) > overall_avg_sale
     | ^---
     |
     = expected limit or order_by
  Location:
  ..._count by rollup (ch.channel) ??? having sum(ch.total_sales) > o...

  Write stats: received 690 chars / 690 bytes; tail: …'erall_avg_sale\\norder by ch.channel asc nulls first\\nlimit 10;'.
  ```
- `trilogy file write test_rollup13.preql --content import raw.all_sales as s;

with overall_stats as
where s.date.year between 1999 and 2001
select
    sum(s.q…l,
    ch.total_sales,
    ch.sale_count
    --,overall_stats.overall_avg
inner join ch.channel = overall_stats.overall_avg
;

select 1 as x limit 1;`

  ```text
  refused to write 'test_rollup13.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:6
     |
  23 |     --,overall_stats.overall_avg
     |      ^---
     |
     = expected access_chain
  Location:
  ...sales,     ch.sale_count     - ??? -,overall_stats.overall_avg in...

  Write stats: received 587 chars / 587 bytes; tail: …'hannel = overall_stats.overall_avg\\n;\\n\\nselect 1 as x limit 1;'.
  ```
- `trilogy run --import raw/date:date select date.month_of_year, date.year, date.month_seq where date.month_seq between 1188 and 1190 group by date.month_of_year, date.year, date.month_seq order by date.month_seq;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nth_seq between 1188 and 1190 ??? group by date.month_of_year, d...
  ```
- `trilogy run --import raw/date:date select distinct date.month_of_year, date.year, date.month_seq where date.month_seq between 1188 and 1190 order by date.month_seq;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.date as date; select ??? distinct date.month_of_year, d...
  ```
- `trilogy run --import raw/item:item select distinct item.category, item.class where item.category = 'Women' limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.item as item; select ??? distinct item.category, item.c...
  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.store_sales as ss;
import raw.customer as c…price / 50.0) * 50 as segment_times_50
order by
    segment asc nulls first,
    customer_count asc nulls first,
    segment_times_50 asc
limit 100;
`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
    --> 29:6
     |
  29 |     (catalog_customers),
     |      ^---
     |
     = expected select_statement
  Location:
  ...ying_customers as union(     ( ??? catalog_customers),     (web_c...

  Write stats: received 1696 chars / 1696 bytes; tail: …'_count asc nulls first,\\n    segment_times_50 asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query97.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Build sets from each channel, then union them wit… else 0 end) as only_catalog_count,
    sum(case when per_combo.in_store > 0 and per_combo.in_catalog > 0 then 1 else 0 end) as both_count
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:1
     |
  13 | union
     | ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...log where ss.date.year = 2000 ??? union select     cs.bill_custo...

  Write stats: received 1137 chars / 1137 bytes; tail: …'o.in_catalog > 0 then 1 else 0 end) as both_count\\nlimit 100;'.
  ```

### `cli-misuse`

- `trilogy run - --import raw.all_sales:s select s.channel, sum(s.quantity) as q by rollup (s.channel) order by s.channel asc nulls first limit 10;`

  ```text
  'select s.channel, sum(s.quantity) as q by rollup (s.channel) order by s.channel asc nulls first limit 10;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.all_sales:s --import raw.item:i select s.channel, i.brand_id, i.class_id, i.category_id, sum(s.quantity * s.list_price) as total_sal…d, i.category_id) order by s.channel asc nulls first, i.brand_id asc nulls first, i.class_id asc nulls first, i.category_id asc nulls first limit 20;`

  ```text
  's.channel, i.brand_id, i.class_id, i.category_id, sum(s.quantity * s.list_price) as total_sales, count(s.sale_line_item_counter) as total_count by rollup (s.channel, i.brand_id, i.class_id, i.category_id) order by s.channel asc nulls first, i.brand_id asc nulls first, i.class_id asc nulls first, i.category_id asc nulls first limit 20;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run - --import raw.all_sales:s select sum(s.quantity * s.list_price) / count(s.sale_line_item_counter) as ovg where s.date.year between 1999 and 2001 limit 1 ;`

  ```text
  'select sum(s.quantity * s.list_price) / count(s.sale_line_item_counter) as ovg where s.date.year between 1999 and 2001 limit 1 ;' looks like a file path, not a dialect. The dialect argument comes AFTER the input file.
    Try: trilogy run select sum(s.quantity * s.list_price) / count(s.sale_line_item_counter) as ovg where s.date.year between 1999 and 2001 limit 1 ; <dialect>
  ```
