# Trilogy failure analysis — 20260626-144934

- Run `20260626-144934` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 288 | failed: 59 (20%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 41 | 69% |
| `syntax-parse` | 14 | 24% |
| `syntax-missing-alias` | 4 | 7% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: HAVING references 'sales.date.year', which is not in the SELECT projection (line 4). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --sales.date.year
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query02.preql`

  ```text
  Resolution error in query02.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 29). The requested concepts split into 7 disconnected subgraphs: {cur.ws, nxt.amt}; {local._virt_filter_amt_6211677768576751}; {local._virt_filter_amt_6726124732496400}; {local._virt_filter_amt_683845391985942}; {local._virt_filter_amt_6992394422738035}; {local._virt_filter_amt_7077512252953346}; {local._virt_filter_amt_9302059918140430}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query02.preql`

  ```text
  Resolution error in query02.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 109). The requested concepts split into 5 disconnected subgraphs: {cur_sat.ws, cur_sun.ws}; {nxt_sun.amt}; {nxt_thu.amt}; {nxt_tue.amt}; {nxt_wed.amt}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Comparison `cur.dow in (0, 1, 2, 3, 4, 5, 6)` matches every value of enum field 'cur.dow', which contains only these values: 0, 1, 2, 3, 4, 5, 6. It is always true and should be removed.
  ```
- `trilogy run query02.preql`

  ```text
  Resolution error in query02.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 29). The requested concepts split into 7 disconnected subgraphs: {cur.ws, nxt.amt}; {local._virt_filter_amt_6211677768576751}; {local._virt_filter_amt_6726124732496400}; {local._virt_filter_amt_683845391985942}; {local._virt_filter_amt_6992394422738035}; {local._virt_filter_amt_7077512252953346}; {local._virt_filter_amt_9302059918140430}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query02.preql`

  ```text
  Resolution error in query02.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 69). The requested concepts split into 8 disconnected subgraphs: {cur0.amt, cur0.ws, cur1.amt, cur2.amt, cur3.amt, cur4.amt, cur5.amt, cur6.amt}; {nxt0.amt}; {nxt1.amt}; {nxt2.amt}; {nxt3.amt}; {nxt4.amt}; {nxt5.amt}; {nxt6.amt}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.channel_dim_id, all_sales.channel_dim_text_id, all_sales.return_channel_dim_id, all_sales.return_channel_dim_text_id, date.date, return_date.date as return_date limit 10;`

  ```text
  Syntax error in stdin: Undefined concept: date.date (line 2, col 155, in SELECT). Suggestions: ['all_sales.date.date', 'all_sales.return_date.date', 'all_sales.billing_customer.first_sales_date.date', 'all_sales.billing_customer.first_shipto_date.date', 'all_sales.ship_customer.first_sales_date.date', 'all_sales.ship_customer.first_shipto_date.date']
  ```
- `trilogy run --import raw/all_sales:all_sales --import raw/store:store select all_sales.channel, all_sales.channel_dim_text_id, all_sales.return_channel_dim_text_id, store.text_id where all_sales.return_channel_dim_text_id = store.text_id and all_sales.channel='CATALOG' limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {all_sales.channel, all_sales.channel_dim_text_id, all_sales.return_channel_dim_text_id}; {store.text_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: grouping()/grouping_id() requires a `by rollup`/`by cube`/grouping-sets aggregate in the enclosing select; it has no meaning without a grouping set (e.g. add `by rollup <dim>` to an aggregate in the select).
  ```
- `trilogy run query05.preql`

  ```text
  GROUPING statement cannot be used without groups\n\nLINE 352: \tWHEN grouping(\"vast\".\"stacked_channel\") + grouping(\"vast\".\"stack...\n               ^\n[SQL: \nWITH \nbusy as (\nSELECT\n     'CATALOG'  as \"all_sales_channel\",\n    \"all_sales_catalog_sales_unified\".\"CS_CATALOG_PAGE_SK\" as \"all_sales_channel_dim_id\",\n    \"all_sales_catalog_sales_unified\".\"CS_EXT_SALES_PRICE\" as \"all_sales_ext_sales_price\",\n    \"all_sales_catalog_sales_unified\".\"CS_NET_PROFIT\" as \"all_sales_net_profit\"\nFROM\n    \"catalog_sales\" as \"all_sales_catalog_sales_unified\"\n    INNER JOIN
  …
  ping(\"vast\".\"stacked_dim_id\") = 2 THEN 0\n\tWHEN grouping(\"vast\".\"stacked_dim_id\") = 1 THEN 1\n\tELSE 2\n\tEND asc,\n    \"vast\".\"stacked_channel\" asc,\n    \"vast\".\"stacked_dim_id\" asc\nLIMIT (100)]\n(Background on this error at: https://sqlalche.me/e/20/f405)",
    "error_type": "ProgrammingError"
  }
  {
    "event": "summary",
    "statements": 1,
    "duration_ms": 16.672,
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
  GROUPING statement cannot be used without groups\n\nLINE 352: \tWHEN grouping(\"vast\".\"stacked_channel\") + grouping(\"vast\".\"stack...\n               ^\n[SQL: \nWITH \nprotective as (\nSELECT\n     'CATALOG'  as \"all_sales_channel\",\n    \"all_sales_catalog_sales_unified\".\"CS_CATALOG_PAGE_SK\" as \"all_sales_channel_dim_id\",\n    \"all_sales_catalog_sales_unified\".\"CS_EXT_SALES_PRICE\" as \"all_sales_ext_sales_price\",\n    \"all_sales_catalog_sales_unified\".\"CS_NET_PROFIT\" as \"all_sales_net_profit\"\nFROM\n    \"catalog_sales\" as \"all_sales_catalog_sales_unified\"\n    INNE
  …
  ing(\"vast\".\"stacked_dim_id\") = 2 THEN 0\n\tWHEN grouping(\"vast\".\"stacked_dim_id\") = 1 THEN 1\n\tELSE 2\n\tEND asc,\n    \"vast\".\"stacked_channel\" asc,\n    \"vast\".\"stacked_dim_id\" asc\nLIMIT (100)]\n(Background on this error at: https://sqlalche.me/e/20/f405)",
    "error_type": "ProgrammingError"
  }
  {
    "event": "summary",
    "statements": 1,
    "duration_ms": 157.382,
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
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(stacked.channel)` (line 35), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(stacked.channel) as g order by g desc`.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: grouping()/grouping_id() requires a `by rollup`/`by cube`/grouping-sets aggregate in the enclosing select; it has no meaning without a grouping set (e.g. add `by rollup <dim>` to an aggregate in the select).
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: grouping()/grouping_id() requires a `by rollup`/`by cube`/grouping-sets aggregate in the enclosing select; it has no meaning without a grouping set (e.g. add `by rollup <dim>` to an aggregate in the select).
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: 4 undefined concept references; fix all before re-running:
    - rolled.channel (line 46, in SELECT); did you mean: rolled.stacked.channel, all_sales.channel, stacked.channel?
    - rolled.dim_text_id (line 46, in SELECT); did you mean: rolled.stacked.dim_text_id, stacked.dim_text_id, rolled.dim_id?
    - rolled.channel (line 63, col 5, in ORDER BY); did you mean: rolled.stacked.channel, all_sales.channel, stacked.channel?
    - rolled.dim_id (line 64, col 5, in ORDER BY); did you mean: rolled.stacked.dim_id, stacked.dim_id, rolled.dim_text_id, rolled._gs?
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY references 'rolled.stacked.dim_id', which is not in the SELECT projection (line 42). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --rolled.stacked.dim_id order by rolled.stacked.dim_id asc`.
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: Recursion error building concept rolled._gs with grain Grain<rolled.stacked.channel,rolled.stacked.dim_id,rolled.stacked.dim_text_id> and lineage <Rowset<rolled>: ref:local._rolled__gs>. This is likely due to a circular reference.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY references 'rolled.stacked.dim_id', which is not in the SELECT projection (line 42). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --rolled.stacked.dim_id order by rolled.stacked.dim_id asc`.
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: 6 undefined concept references; fix all before re-running:
    - s01.cust_id (line 58, col 5, in WHERE); did you mean: s01.store_rev.cust_id, store_rev.cust_id, web_rev.cust_id, s02.store_rev.cust_id, w01.web_rev.cust_id, w02.web_rev.cust_id?
    - s02.cust_id (line 58, col 20, in WHERE); did you mean: s02.store_rev.cust_id, store_rev.cust_id, web_rev.cust_id, s01.store_rev.cust_id, w01.web_rev.cust_id, w02.web_rev.cust_id?
    - s01.cust_id (line 59, col 9, in WHERE); did you mean: s01.store_rev.cust_id, store_rev.cust_id, web_rev.cust_id, s02.store_rev.cust_id, w01.web_rev.cust_id, w02.web_rev.cust_id?
    - w01.cust_id (line 59, col 24, in WHERE); did you mean: w01.web_rev.cust_id, store_rev.cust_id, web_rev.cust_id, s01.store_rev.cust_id, s02.store_rev.cust_id, w02.web_rev.cust_id?
    - s01.cust_id (line 60, col 9, in WHERE); did you mean: s01.store_rev.cust_id, store_rev.cust_id, web_rev.cust_id, s02.store_rev.cust_id, w01.web_rev.cust_id, w02.web_rev.cust_id?
    - w02.cust_id (line 60, col 24, in WHERE); did you mean: w02.web_rev.cust_id, store_rev.cust_id, web_rev.cust_id, s01.store_rev.cust_id, s02.store_rev.cust_id, w01.web_rev.cust_id?
  ```
- `trilogy run query11.preql`

  ```text
  Resolution error in query11.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 56). The requested concepts split into 5 disconnected subgraphs: {c.first_name, c.last_name, c.preferred_cust_flag, c.text_id}; {local.store_growth}; {local.web_growth}; {s01.cust_id, s01.rev}; {w01.rev}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: HAVING references 's01.rev', 's02.rev', 'w01.rev', 'w02.rev', which are not in the SELECT projection (line 55). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --s01.rev, --s02.rev, --w01.rev, --w02.rev
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: Undefined concept: s.line_item (line 11, in SELECT). Suggestions: ['s.net_profit', 's.item.id']
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg_sale', which is not in the SELECT projection (line 11). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg_sale
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 11). The requested concepts split into 2 disconnected subgraphs: {i.brand_id, i.category_id, i.class_id}; {local.channel_presence, local.total_count, local.total_sales, s.channel, s.date.month_of_year, s.date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced: {local.total_count, local.total_sales, s.channel, s.item.brand_id, s.item.category_id, s.item.class_id}; still unresolved: {local.overall_avg_sale}
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'overall_avg.avg_sale', which is not in the SELECT projection (line 14). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --overall_avg.avg_sale
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'overall_avg.avg_sale', which is not in the SELECT projection (line 16). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --overall_avg.avg_sale
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: 8 undefined concept references; fix all before re-running:
    - qualifying.s_channel (line 33, col 5, in SELECT); did you mean: qualifying.s.channel, qualifying.total_count, qualifying.s_item_class_id?
    - qualifying.s_item_brand_id (line 34, col 5, in SELECT); did you mean: qualifying.s.item.brand_id, qualifying.s_item_class_id, qualifying.s_item_category_id?
    - qualifying.s_item_class_id (line 35, col 5, in SELECT); did you mean: qualifying.s.item.class_id, qualifying.s_item_brand_id, qualifying.s_item_category_id?
    - qualifying.s_item_category_id (line 36, col 5, in SELECT); did you mean: qualifying.s.item.category_id, qualifying.s_item_class_id, qualifying.s_item_brand_id?
    - qualifying.s_channel (line 40, col 5, in ORDER BY); did you mean: qualifying.s.channel, qualifying.total_count, qualifying.s_item_class_id?
    - qualifying.s_item_brand_id (line 41, col 5, in ORDER BY); did you mean: qualifying.s.item.brand_id, qualifying.s_item_class_id, qualifying.s_item_category_id?
    - qualifying.s_item_class_id (line 42, col 5, in ORDER BY); did you mean: qualifying.s.item.class_id, qualifying.s_item_brand_id, qualifying.s_item_category_id?
    - qualifying.s_item_category_id (line 43, col 5, in ORDER BY); did you mean: qualifying.s.item.category_id, qualifying.s_item_class_id, qualifying.s_item_brand_id?
  ```
- `trilogy file read raw/physical_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'ss.date.year', which is not in the SELECT projection (line 9). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --ss.date.year
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'ss.date.id', which is not in the SELECT projection (line 10). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --ss.date.id
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'local.global_max', which is not in the SELECT projection (line 50). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.global_max
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run --import raw.date:d select d.month_seq, d.year, d.month_of_year, d.month_name where d.month_seq between 1187 and 1190 order by d.month_seq;`

  ```text
  Syntax error in stdin: Undefined concept: d.month_name (line 2, col 46, in SELECT). Suggestions: ['d.month_seq', 'd.month_of_year', 'd.day_name']
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: ss.customer_demographics.marital_status. Suggestions: ['ss.customer.demographics.marital_status', 'ss.return_customer.demographics.marital_status', 'ss.customer_demographic.marital_status', 'cs.ship_customer.demographics.marital_status', 'cs.bill_customer.demographics.marital_status', 'cs.bill_customer_demographic.marital_status']
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: 6 undefined concept references; fix all before re-running:
    - items_1999.item_id (line 65, col 23, in WHERE); did you mean: items_1999.base.item_id, base.item_id, items_2000.base.item_id, items_2000.item_id, items_1999.store_zip?
    - items_1999.store_name (line 66, col 26, in WHERE); did you mean: items_1999.base.store_name, base.store_name, items_2000.base.store_name, items_2000.store_name, items_1999.store_zip?
    - items_1999.store_zip (line 67, col 25, in WHERE); did you mean: items_1999.base.store_zip, base.store_zip, items_2000.base.store_zip, items_2000.store_zip, items_1999.store_name?
    - items_2000.item_id (line 68, col 23, in WHERE); did you mean: items_2000.base.item_id, base.item_id, items_1999.item_id, items_1999.base.item_id, items_2000.store_zip?
    - items_2000.store_name (line 69, col 26, in WHERE); did you mean: items_2000.base.store_name, base.store_name, items_1999.store_name, items_1999.base.store_name, items_2000.store_zip?
    - items_2000.store_zip (line 70, col 25, in WHERE); did you mean: items_2000.base.store_zip, base.store_zip, items_1999.store_zip, items_1999.base.store_zip, items_2000.store_name?
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {cs.item.id, local.cat_ext_list_price_item, local.cat_refund_total}
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Have {'GroupNode<cs.item.id,local.cat_ext_list_price_item>': None} and need local.cat_ext_list_price_item > multiply(2,local.cat_refund_total@Grain<cr.item.id>)
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: qualifying_items.item_id. Suggestions: ['qualifying_items.cat_combined.item_id', 'cat_combined.item_id']
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

# Compute daily sales at (week_seq, day_of_week) grain
with daily as
where sales.c…ily.amt ? daily.dow = 6) / (lead(sum(daily.amt ? daily.dow = 6), 53) over (order by daily.ws asc)), 2) as saturday
order by week_seq asc nulls first;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...cur in 2001 where daily.ws in ??? (select sales.date.week_seq wh...

  Write stats: received 1469 chars / 1469 bytes; tail: …'.ws asc)), 2) as saturday\\norder by week_seq asc nulls first;'.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as sales;

# Membership: week sequences appearing in 2001 (web/catalog only)
rowset wk2001 as…ily.amt ? daily.dow = 6) / (lead(sum(daily.amt ? daily.dow = 6), 53) over (order by daily.ws asc)), 2) as saturday
order by week_seq asc nulls first;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | rowset wk2001 as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ng in 2001 (web/catalog only) ??? rowset wk2001 as where sales.c...

  Write stats: received 1547 chars / 1547 bytes; tail: …'.ws asc)), 2) as saturday\\norder by week_seq asc nulls first;'.
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.return_channel_dim_text_id where all_sales.return_amount is not null group by 1,2 limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...les.return_amount is not null ??? group by 1,2 limit 20;
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, all_sales.return_channel_dim_text_id, all_sales.date.da…en '2000-08-23'::date and '2000-09-06'::date and all_sales.return_channel_dim_text_id not in (select cp.text_id from raw.catalog_page as cp) limit 5;`

  ```text
  Syntax error in stdin: Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...rn_channel_dim_text_id not in ??? (select cp.text_id from raw.ca...
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Stack sales and returns separately
with stacked as union(
    # SALES ARM: u…al_net_profit,
    --grouping(stacked.channel) + grouping(stacked.dim_id) as _gs
order by
    _gs,
    stacked.channel,
    stacked.dim_id
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [212]: A `by <grain>` clause must attach directly to an aggregate, not to an expression that wraps one (e.g. `coalesce(...)`, `round(...)`, arithmetic). Move the grain inside, next to the aggregate — write `coalesce(sum(x) by store.id, 0)` — or compute the grouped aggregate first and wrap it: `auto m <- sum(x) by store.id;` then `coalesce(m, 0)`.
  Location:
  ...oss(g) -> coalesce(sum(g), 0) ??? by rollup stacked.channel, sta...

  Write stats: received 2321 chars / 2321 bytes; tail: …'\\n    _gs,\\n    stacked.channel,\\n    stacked.dim_id\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Stack sales and returns separately
with stacked as union(
    # SALES ARM: u…sce(combined.net_profit, 0) as total_net_profit
order by
    combined.sort_key desc,
    combined.channel,
    combined.dim_id nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...and.net_profit, 2 as sort_key ??? from grand),     (select chann...

  Write stats: received 3313 chars / 3313 bytes; tail: …'combined.channel,\\n    combined.dim_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.customer as c;

# Store revenue by customer b…re_rev.cust_id in (select web_rev2.cust_id from web_rev as web_rev2 where web_rev2.yr = 2002)
  and store_rev.rev > 0
select
    store_rev.cust_id
;
`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...02   and store_rev.cust_id in ??? (select store_rev2.cust_id fro...

  Write stats: received 991 chars / 991 bytes; tail: …'002)\\n  and store_rev.rev > 0\\nselect\\n    store_rev.cust_id\\n;\\n'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;
import raw.item as i;

# Step 1: Find (brand, class, category) combos that appear in AL…
order by
    s.channel asc nulls first,
    i.brand_id asc nulls first,
    i.class_id asc nulls first,
    i.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:56
     |
  23 |     sum(s.quantity * s.list_price) as total_sales,  -- or we can use ext_list_price
     |                                                        ^---
     |
     = expected select_transform or IDENTIFIER
  Location:
  ...st_price) as total_sales,  -- ??? or we can use ext_list_price

  Write stats: received 1167 chars / 1167 bytes; tail: …'sc nulls first,\\n    i.category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Step 1: Find (brand, class, category) combos that appear in ALL THREE channels in 19….channel asc nulls first,
    s.item.brand_id asc nulls first,
    s.item.class_id asc nulls first,
    s.item.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 25:6
     |
  25 |     -- Only apply the having filter to non-rollup rows (where all grouping fields are non-null)
     |      ^---
     |
     = expected access_chain
  Location:
  ...id as total_count having     - ??? - Only apply the having filter...

  Write stats: received 1503 chars / 1503 bytes; tail: …'lls first,\\n    s.item.category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.date:date select date.quarter_name, count(date.id) as cnt where date.year = 2001 group by date.quarter_name order by date.quarter_name;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...as cnt where date.year = 2001 ??? group by date.quarter_name ord...
  ```
- `trilogy file write query17.preql --content import raw.catalog_store_returns as csr;

# Filter: store sales in 2001Q1, joined to store returns in 2001Q1-Q3,
#….catalog_quantity), 0) as cv_cs_qty
order by
    item_code asc nulls first,
    item_desc asc nulls first,
    store_state asc nulls first
limit 100;`

  ```text
  refused to write 'query17.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...ore.state as store_state,     ??? count(*) as sale_count,     av...

  Write stats: received 1149 chars / 1149 bytes; tail: …' asc nulls first,\\n    store_state asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Frequent item…st_name
order by feb_sales.last_name asc nulls first,
         feb_sales.first_name asc nulls first,
         total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:23
     |
  13 |     --count(ss.item.id, ss.date.id) by substring(ss.item.desc, 1, 30) as pair_count
     |                       ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ate_id,     --count(ss.item.id ??? , ss.date.id) by substring(ss....

  Write stats: received 3113 chars / 3113 bytes; tail: …'ulls first,\\n         total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query54.preql --content # query54.preql
# Identify customers who bought 'Women'/'maternity' via catalog or web in Dec 1998,
# then compute…round(sum(ss.ext_sales_price) / 50, 0) * 50 as segment_times_50
order by segment nulls first, customer_count nulls first, segment_times_50
limit 100;`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:6
    |
  7 |     (import raw.catalog_sales as cs;
    |      ^---
    |
    = expected select_statement
  Location:
  ...ying_customers as union(     ( ??? import raw.catalog_sales as cs...

  Write stats: received 1305 chars / 1305 bytes; tail: …'rst, customer_count nulls first, segment_times_50\\nlimit 100;'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.item as…g.list_sum as list_sum,
  agg.coupon_sum as coupon_sum
order by
  agg.product_name asc,
  agg.store_name asc,
  agg.cnt asc,
  agg.wholesale_sum asc;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...second where   agg.item_id in ??? (select agg.item_id where agg....

  Write stats: received 3016 chars / 3016 bytes; tail: …' agg.store_name asc,\\n  agg.cnt asc,\\n  agg.wholesale_sum asc;'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, count(all_sales.channel_dim_id) where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date and all_sales.channel_dim_id is not null limit 5;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(all_sales.channel_dim_id) as channel_dim_id_count`
  Location:
  ...unt(all_sales.channel_dim_id) ??? where all_sales.date.date betw...
  ```
- `trilogy file write query11.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;

# Store revenue by customer by year
# Revenue = sum of …  c.text_id asc nulls first,
    c.first_name asc nulls first,
    c.last_name asc nulls first,
    c.preferred_cust_flag asc nulls first
limit 100;
`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `store growth rate
      case when s01.s01_rev > 0 then (s02.s02_rev - s01.s01_rev) / s01.s01_rev else 0 end as store_growth_rate_case_when_s01_s01_rev_`
  Location:
  ...erred_cust_flag,     -- store ??? growth rate     case when s01....

  Write stats: received 1702 chars / 1702 bytes; tail: …'first,\\n    c.preferred_cust_flag asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query64.preql --content import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.item as i;

# Items with color in lis….wholesale_sum,
  base.list_sum,
  base.coupon_sum
order by
  base.product_name asc,
  base.store_name asc,
  base.cnt asc,
  base.wholesale_sum asc;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct qualifying_items as distinct_qualifying_items`
  Location:
  ...qual_items as select distinct ??? qualifying_items as item_id;

  Write stats: received 3330 chars / 3330 bytes; tail: …'se.store_name asc,\\n  base.cnt asc,\\n  base.wholesale_sum asc;'.
  ```
- `trilogy file write query64.preql --content import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Compute cumulative catalog ext_list_price and…and next_cnt <= base.cnt
order by
  base.product_name asc,
  base.store_name asc,
  next_cnt asc,
  base.wholesale_sum asc,
  next_wholesale_sum asc;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `First row (1999): year as first_row_1999_year`
  Location:
  ...,   base.cust_zip,   -- First ??? row (1999): year, cnt, wholesa...

  Write stats: received 4152 chars / 4152 bytes; tail: …'cnt asc,\\n  base.wholesale_sum asc,\\n  next_wholesale_sum asc;'.
  ```
