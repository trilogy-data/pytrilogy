# Trilogy failure analysis — 20260627-031142

- Run `20260627-031142` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 277 | failed: 52 (19%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 31 | 60% |
| `syntax-parse` | 18 | 35% |
| `cli-misuse` | 3 | 6% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Undefined concept: wk_sales.sales.date.week_seq. Suggestions: ['sales.date.week_seq', 'sales.return_date.week_seq', 'sales.billing_customer.first_sales_date.week_seq', 'sales.billing_customer.first_shipto_date.week_seq', 'sales.ship_customer.first_sales_date.week_seq', 'sales.ship_customer.first_shipto_date.week_seq']
  ```
- `trilogy run query02.preql`

  ```text
  Unexpected error in query02.preql: name 'connected_equivalent_suggestions' is not defined
  ```
- `trilogy run query02.preql`

  ```text
  Unexpected error in query02.preql: name 'connected_equivalent_suggestions' is not defined
  ```
- `trilogy run query02.preql`

  ```text
  Unexpected error in query02.preql: name 'connected_equivalent_suggestions' is not defined
  ```
- `trilogy run query02.preql`

  ```text
  Unexpected error in query02.preql: name 'connected_equivalent_suggestions' is not defined
  ```
- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/all_sales:all_sales select channel, min(channel_dim_id) as min_id, max(channel_dim_id) as max_id, min(return_channel_dim_id) as min_ret, max(return_channel_dim_id) as max_ret, count(order_id) as cnt order by channel;`

  ```text
  Syntax error in stdin: 5 undefined concept references; fix all before re-running:
    - local.channel (line 2, col 8, in SELECT); did you mean: all_sales.channel, channel_dim_id?
    - local.channel_dim_id (line 2, in SELECT); did you mean: all_sales.channel_dim_id, return_channel_dim_id, channel?
    - local.return_channel_dim_id (line 2, in SELECT); did you mean: all_sales.return_channel_dim_id, channel_dim_id, all_sales.return_channel_dim_text_id?
    - local.order_id (line 2, in SELECT); did you mean: all_sales.order_id?
    - local.channel (line 2, col 189, in ORDER BY); did you mean: all_sales.channel, channel_dim_id?
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(combined.entity_id)` (line 38), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.entity_id) as g order by g desc`.
  ```
- `trilogy run query05.preql`

  ```text
  column \"_virt_agg_sum_2971703028423430\" must appear in the GROUP BY clause or must be part of an aggregate function.\nEither add it to the GROUP BY list, or use \"ANY_VALUE(_virt_agg_sum_2971703028423430)\" if the exact value of \"_virt_agg_sum_2971703028423430\" is not important.\n\nLINE 381:     coalesce(\"vast\".\"_virt_agg_sum_2971703028423430\",0.0) as \"total_returns...\n                       ^\n[SQL: \nWITH \nscrawny as (\nSELECT\n     'CATALOG'  as \"all_sales_channel\",\n    \"all_sales_catalog_sales_unified\".\"CS_CATALOG_PAGE_SK\" as \"all_sales_channel_dim_id\",\n    \"all_sale
  …
  bined_channel\",\"dapper\".\"combined_channel\") asc nulls first,\n    coalesce(\"cool\".\"combined_entity_id\",\"dapper\".\"combined_entity_id\") asc nulls first\nLIMIT (100)]\n[parameters: (0.0, 0.0, 0.0, 0.0)]\n(Background on this error at: https://sqlalche.me/e/20/f405)",
    "error_type": "ProgrammingError"
  }
  {
    "event": "summary",
    "statements": 1,
    "duration_ms": 402.693,
    "ok": false,
    "rows": 0
  }
  {
    "event": "output_truncated",
    "dropped_events": 1,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Ambiguous reference 'sub.chan': matches ['sub.leaf.chan', 'sub.leaf.combined.chan']. Qualify the full path to disambiguate.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY references 'al.e', which is not in the SELECT projection (line 90). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --al.e order by al.e asc`.
  ```
- `trilogy run query05.preql`

  ```text
  column \"_virt_agg_sum_1607841699058989\" must appear in the GROUP BY clause or must be part of an aggregate function.\nEither add it to the GROUP BY list, or use \"ANY_VALUE(_virt_agg_sum_1607841699058989)\" if the exact value of \"_virt_agg_sum_1607841699058989\" is not important.\n\nLINE 381:     coalesce(\"vast\".\"_virt_agg_sum_1607841699058989\",0.0) as \"total_returns...\n                       ^\n[SQL: \nWITH \nscrawny as (\nSELECT\n     'CATALOG'  as \"all_sales_channel\",\n    \"all_sales_catalog_sales_unified\".\"CS_CATALOG_PAGE_SK\" as \"all_sales_channel_dim_id\",\n    \"all_sale
  …
  cool\".\"combined_channel\",\"dapper\".\"combined_channel\") asc nulls first,\n    coalesce(\"cool\".\"combined_eid\",\"dapper\".\"combined_eid\") asc nulls first\nLIMIT (100)]\n[parameters: (0.0, 0.0, 0.0, 0.0)]\n(Background on this error at: https://sqlalche.me/e/20/f405)",
    "error_type": "ProgrammingError"
  }
  {
    "event": "summary",
    "statements": 1,
    "duration_ms": 376.773,
    "ok": false,
    "rows": 0
  }
  {
    "event": "output_truncated",
    "dropped_events": 1,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Ambiguous reference 'chan_sub.chan': matches ['chan_sub.entity_agg.chan', 'chan_sub.entity_agg.combined.chan']. Qualify the full path to disambiguate.
  ```
- `trilogy run query05.preql`

  ```text
  column \"_virt_agg_sum_6631217563195683\" must appear in the GROUP BY clause or must be part of an aggregate function.\nEither add it to the GROUP BY list, or use \"ANY_VALUE(_virt_agg_sum_6631217563195683)\" if the exact value of \"_virt_agg_sum_6631217563195683\" is not important.\n\nLINE 372:     coalesce(\"vast\".\"_virt_agg_sum_6631217563195683\",0.0) as \"total_gros...\n                       ^\n[SQL: \nWITH \nscrawny as (\nSELECT\n     'CATALOG'  as \"all_sales_channel\",\n    \"all_sales_catalog_sales_unified\".\"CS_CATALOG_PAGE_SK\" as \"all_sales_channel_dim_id\",\n    \"all_sales_c
  …
  sce(\"cool\".\"combined_chan\",\"dapper\".\"combined_chan\") asc nulls first,\n    coalesce(\"cool\".\"combined_eid\",\"dapper\".\"combined_eid\") asc nulls first\nLIMIT (100)]\n[parameters: (0.0, 0.0, 0.0, 0.0)]\n(Background on this error at: https://sqlalche.me/e/20/f405)",
    "error_type": "ProgrammingError"
  }
  {
    "event": "summary",
    "statements": 1,
    "duration_ms": 325.237,
    "ok": false,
    "rows": 0
  }
  {
    "event": "output_truncated",
    "dropped_events": 1,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 73 column 12 (char 1874). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read query11.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg', which is not in the SELECT projection (line 11). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(local.channel)` (line 11), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.channel) as g order by g desc`.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'store_cust_totals.cust_total', 'local.global_max_total', which are not in the SELECT projection (line 29). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --store_cust_totals.cust_total, --local.global_max_total
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query54.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Ambiguous reference 'yr1999.product_name': matches ['yr1999.sale_base.item.product_name', 'yr1999.sale_base.product_name']. Qualify the full path to disambiguate.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Ambiguous reference 'yr1999.product_name': matches ['yr1999.sale_base.product_name', 'yr1999.sale_base.store_sales.item.product_name']. Qualify the full path to disambiguate.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Ambiguous reference 'yr1999.product_name': matches ['yr1999.sale_base.product_name', 'yr1999.sale_base.store_sales.item.product_name']. Qualify the full path to disambiguate.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Duplicate select output for yr2000.yr00_cnt; Line: 97
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {yr1999.sale_base.ca_ci, yr1999.sale_base.ca_snam, yr1999.sale_base.ca_snum, yr1999.sale_base.ca_zi, yr1999.sale_base.iid, yr1999.sale_base.pname, yr1999.sale_base.sa_ci, yr1999.sale_base.sa_snam, yr1999.sale_base.sa_snum, yr1999.sale_base.sa_zi, yr1999.sale_base.sname, yr1999.sale_base.szip, yr1999.yr99_cnt, yr1999.yr99_cs, yr1999.yr99_ls, yr1999.yr99_ws, yr1999.yr99_year, yr2000.sale_base.sname, yr2000.sale_base.szip, yr2000.yr00_cnt, yr2000.yr00_cs, yr2000.yr00_ls, yr2000.yr00_ws, yr2000.yr00_year}
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: agg2.iid. Suggestions: ['agg.iid', '_agg_iid', 'agg2.szip']
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query80.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.all_sales as sales;

# Filter to WEB and CATALOG channels only
where sales.channel in ('WEB', 'CATALOG'…n this week_seq is in 2001
    --sum(sales.date.year) by sales.date.week_seq
    sales.date.year = 2001
order by sales.date.week_seq asc nulls first;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
    --> 44:6
     |
  44 |     -- only week sequences that ever occur in 2001
     |      ^---
     |
     = expected access_chain
  Location:
      ) as saturday having     - ??? - only week sequences that eve...

  Write stats: received 2027 chars / 2027 bytes; tail: …'te.year = 2001\\norder by sales.date.week_seq asc nulls first;'.
  ```
- `trilogy run --import raw.all_sales:sales select sales.date.week_seq, sales.channel, sum(sales.ext_sales_price) as amt where sales.channel in ('WEB','CATALOG') group by 1, 2 order by 1 limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ....channel in ('WEB','CATALOG') ??? group by 1, 2 order by 1 limit...
  ```
- `trilogy run --import raw.date:d select d.week_seq, d.year, count(*) as cnt where d.year = 2001 group by 1,2 order by 1;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...d; select d.week_seq, d.year, ??? count(*) as cnt where d.year =...
  ```
- `trilogy run --import raw.all_sales:sales select sales.date.week_seq where sales.channel in ('WEB','CATALOG') and sales.date.week_seq between 5270 and 5322 group by 1 order by 1;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...eek_seq between 5270 and 5322 ??? group by 1 order by 1;
  ```
- `trilogy run --import raw/all_sales:all_sales select channel, min(channel_dim_id) as min_id, max(channel_dim_id) as max_id, min(return_channel_dim_id) as min_ret, max(return_channel_dim_id) as max_ret, count(*) as cnt group by channel order by channel limit 10;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...n_channel_dim_id) as max_ret, ??? count(*) as cnt group by chann...
  ```
- `trilogy run --import raw/all_sales:all_sales select channel, min(channel_dim_id) as min_id, max(channel_dim_id) as max_id, min(return_channel_dim_id) as min_ret, max(return_channel_dim_id) as max_ret, count(order_id) as cnt group by channel order by channel;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...x_ret, count(order_id) as cnt ??? group by channel order by chan...
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Sales & returns union
with combined as union(
    (where all_sales.date.date…    when entity_identifier is null then 1
         else 2 end asc,
    channel_type asc nulls first,
    entity_identifier asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 72:1
     |
  72 | UNION ALL
     | ^---
     |
     = expected metadata, limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...ty_agg.sn, 0.0) as net_profit ??? UNION ALL select     case chan...

  Write stats: received 2943 chars / 2943 bytes; tail: …'ulls first,\\n    entity_identifier asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Sales & returns union
with combined as union(
    (where all_sales.date.date…l then 0 when all_levels.raw_eid is null then 1 else 2 end as _ord
order by
    _ord asc,
    _rc asc nulls first,
    _re asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
       entity_agg.sn     ),     ??? (select         case entity_ag...

  Write stats: received 3401 chars / 3401 bytes; tail: …'\\n    _rc asc nulls first,\\n    _re asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Sales & returns union with text_id
with combined as union(
    (where all_sa…rc is null then 0 when result.re is null then 1 else 2 end as _ord
order by
    _ord asc,
    _rc asc nulls first,
    _re asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
     # Subtotals by channel     ??? (select         case entity_ag...

  Write stats: received 3080 chars / 3080 bytes; tail: …'\\n    _rc asc nulls first,\\n    _re asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/all_sales:all_sales --import raw/store:store select count(*) as cnt;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...rt raw.store as store; select ??? count(*) as cnt;
  ```
- `trilogy run --import raw/all_sales:all_sales select count(*) cnt, sum(all_sales.ext_sales_price) as total_gross, sum(all_sales.return_amount) as total_ret, sum(all_sales.net_profit) - sum(all_sales.return_net_loss) as total_net where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...ll_sales as all_sales; select ??? count(*) cnt, sum(all_sales.ex...
  ```
- `trilogy file write query11.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;

# Store revenue per customer per year
rowset store_rev …2
inner join store_01.cust_id = store_02.cust_id
inner join store_01.cust_id = web_01.cust_id
inner join store_01.cust_id = web_02.cust_id
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset store_rev as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...revenue per customer per year ??? rowset store_rev as select

  Write stats: received 1358 chars / 1358 bytes; tail: …'t_id\\ninner join store_01.cust_id = web_02.cust_id\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…) as total_sales
order by
    combined.last_name asc nulls first,
    combined.first_name asc nulls first,
    total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:94
    |
  6 | auto freq_desc_prefix <- substring(store_sales.item.desc, 1, 30) ? (count(store_sales.item.id, store_sales.date.id) by substring(store_sales.item.desc, 1, 30) > 4) and store_sales.date.year between 2000 and 2003;
    |                                                                                              ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...) ? (count(store_sales.item.id ??? , store_sales.date.id) by subs...

  Write stats: received 2196 chars / 2196 bytes; tail: …' asc nulls first,\\n    total_sales asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…ame,
    sum(combined.sale_amount) as total_sales
order by
    combined.last_name asc nulls first,
    combined.first_name asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:1
    |
  6 | rowset frequent_desc as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...n store_sales (2000-2003) > 4 ??? rowset frequent_desc as where

  Write stats: received 2609 chars / 2609 bytes; tail: …'ls first,\\n    combined.first_name asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…ame,
    sum(combined.sale_amount) as total_sales
order by
    combined.last_name asc nulls first,
    combined.first_name asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:32
     |
  10 |     --count(store_sales.item.id, store_sales.date.id) as pair_count
     |                                ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
     --count(store_sales.item.id ??? , store_sales.date.id) as pair...

  Write stats: received 2251 chars / 2251 bytes; tail: …'ls first,\\n    combined.first_name asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:s --import raw.catalog_sales:cs select s.customer.id, s.date.month_seq, s.store.county, s.store.state, s.customer.addres…re cs.sold_date.month_seq = 1187 and cs.item.category = 'Women' and cs.item.class = 'maternity') and s.date.month_seq between 1188 and 1190 limit 20;`

  ```text
  Syntax error in stdin: Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ....state where s.customer.id in ??? (select cs.bill_customer.id wh...
  ```
- `trilogy run --import raw.store_sales:s select s.customer.id, sum(s.ext_sales_price) as t where s.date.month_seq between 1188 and 1190 and s.store.county = s.…stomer.address.state and s.customer.id in (select cat_cust.cid from (with cat_cust as ...)) ... hmm, let me just run the query file again to confirm.`

  ```text
  Syntax error in stdin: Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...ss.state and s.customer.id in ??? (select cat_cust.cid from (wit...
  ```
- `trilogy file write query75.preql --content import raw.all_sales as sales;
import raw.item as item;

# Net metrics per sales line: sold minus return (missing …;

# Self-join to compare 2002 vs 2001
where yearly.yr = 2002
inner join yearly.yr = yearly.yr  # placeholder - need to think about how to self-join
`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...r_id, net_qty, net_amt) grain ??? rowset per_line <- select

  Write stats: received 1207 chars / 1207 bytes; tail: …'ly.yr  # placeholder - need to think about how to self-join\\n'.
  ```

### `cli-misuse`

- `trilogy run query02.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy explore raw/store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_returns.preql' does not exist.
  ```
- `trilogy run query80.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
