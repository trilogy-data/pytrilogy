# Trilogy failure analysis — 20260627-120644

- Run `20260627-120644` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 301 | failed: 39 (13%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 25 | 64% |
| `syntax-parse` | 12 | 31% |
| `file-not-found` | 1 | 3% |
| `undefined-concept` | 1 | 3% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  {
    "event": "statement_result",
    "index": 0,
    "total": 1,
    "duration_ms": 4.943,
    "success": false,
    "error": "Invalid reference string found in query: \nWITH \ncheerful as (\nSELECT\n     'CATALOG'  as \"all_sales_channel\",\n    \"all_sales_catalog_sales_unified\".\"CS_SOLD_DATE_SK\" as \"all_sales_date_id\",\n    \"all_sales_catalog_sales_unified\".\"CS_EXT_SALES_PRICE\" as \"all_sales_ext_sales_price\"\nFROM\n    \"catalog_sales\" as \"all_sales_catalog_sales_unified\"\nUNION ALL\nSELECT\n     'WEB'  as \"all_sales_channel\",\n    \"all_sales_web_sales_unified\".\"WS_SOLD_DATE_SK\"
  …
  ium\".\"_virt_filter_sales_amt_6271356325803871\",2) as \"saturday\"\nFROM\n    \"premium\"\nGROUP BY\n    1,\n    2,\n    3,\n    4,\n    5,\n    6,\n    7,\n    8\nORDER BY \n    \"weekday_week_seq\" asc nulls first, this should never occur. Please create an issue to report this.",
    "error_type": "ValueError"
  }
  {
    "event": "summary",
    "statements": 1,
    "duration_ms": 5.261,
    "ok": false,
    "rows": 0
  }
  {
    "event": "output_truncated",
    "dropped_events": 1,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```
- `trilogy run query02.preql`

  ```text
  Resolution error in query02.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 67). The requested concepts split into 7 disconnected subgraphs: {sun_f.amt, sun_f.ws}; {thu.amt}; {thu_f.amt}; {tue.amt}; {tue_f.amt}; {wed.amt}; {wed_f.amt}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Undefined concept: f.amt. Suggestions: ['daily.amt']
  ```
- `trilogy run query02.preql`

  ```text
  Unexpected error in query02.preql: Invalid reference string found in query:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "all_sales_date_id",
      "all_sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "all_sales_ext_sales_price"
  FROM
      "catalog_sales" as "all_sales_catalog_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_sales_unified"."WS_SOLD_DATE_SK" as "all_sales_date_id",
      "all_sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "all_sales_ext_sales_price"
  FROM
      "web_sales" as "all_sales_web_sales_unified"),
  cooperative as (
  SELECT
      "all_sales_date_date"."D_DOW" as "all_sales_date_day_of_week",
      "all_sales_date_date"."D_WEEK_SEQ" as "all_sales_date_week_seq",
      "cheerful"."all_sales_ext_sales_price" as "all_sales_ext_sales_price"
  FROM
      "cheerful"
      LEFT OUTER JOIN "date_dim" as "all_sales_date_date" on "cheerful"."all_sales_date_id" = "all_sales_date_date"."D_DATE_SK"
  WHERE
      "cheerful"."all_sales_channel" in ('WEB','CATALOG')
  ),
  uneven as (
  SELECT
      "cooperative"."all_sales_date_week_seq" as "_future_ws",
      sum("cooperative"."all_sales_ext_sales_price") as "_future_amt"
  FROM
      "cooperative"
  GROUP BY
      1,
      "cooperative"."all_sales_date_day_of_week"),
  questionable as (
  SELECT
      "cooperative"."all_sales_date_day_of_week" as "_daily_dow",
      "cooperative"."all_sales_date_week_seq" as "_daily_ws",
      sum("cooperative"."all_sales_ext_sales_price") as "_daily_amt"
  FROM
      "cooperative"
  GROUP BY
      1,
      2),
  yummy as (
  SELECT
      "uneven"."_future_amt" as "future_amt",
      "uneven"."_future_ws" as "future_ws"
  FROM
      "uneven"),
  abundant as (
  SELECT
      "questionable"."_daily_amt" as "daily_amt",
      "questionable"."_daily_dow" as "daily_dow",
      "questionable"."_daily_ws" as "daily_ws"
  FROM
      "questionable"),
  juicy as (
  SELECT
      "yummy"."future_amt" as "future_amt",
      "yummy"."future_ws" - 53 as "_virt_func_subtract_2729598913764358",
      "yummy"."future_ws" as "future_ws"
  FROM
      "yummy"
  GROUP BY
      1,
      2,
      3),
  vacuous as (
  SELECT
      "abundant"."daily_dow" as "daily_dow",
      "abundant"."daily_dow" as "future_dow",
      "juicy"."future_amt" as "future_amt",
      "juicy"."future_ws" as "future_ws"
  FROM
      "juicy"
      INNER JOIN "abundant" on "juicy"."_virt_func_subtract_2729598913764358" = "abundant"."daily_ws"
  WHERE
      "abundant"."daily_ws" in (select INVALID_REFERENCE_BUG."_virt_filter_week_seq_8929215006315315" from INVALID_REFERENCE_BUG where INVALID_REFERENCE_BUG."_virt_filter_week_seq_8929215006315315" is not null)

  GROUP BY
      1,
      3,
      4),
  concerned as (
  SELECT
      "vacuous"."future_ws" as "future_ws",
      CASE WHEN "vacuous"."future_dow" = 0 THEN "vacuous"."future_amt" ELSE NULL END as "_virt_filter_amt_3768105804604636",
      CASE WHEN "vacuous"."future_dow" = 1 THEN "vacuous"."future_amt" ELSE NULL END as "_virt_filter_amt_8542750418198385",
      CASE WHEN "vacuous"."future_dow" = 2 THEN "vacuous"."future_amt" ELSE NULL END as "_virt_filter_amt_8659369554987272",
      CASE WHEN "vacuous"."future_dow" = 3 THEN "vacuous"."future_amt" ELSE NULL END as "_virt_filter_amt_4058952042352307",
      CASE WHEN "vacuous"."future_dow" = 4 THEN "vacuous"."future_amt" ELSE NULL END as "_virt_filter_amt_2697784922538321",
      CASE WHEN "vacuous"."future_dow" = 5 THEN "vacuous"."future_amt" ELSE NULL END as "_virt_filter_amt_4244070725580287",
      CASE WHEN "vacuous"."future_dow" = 6 THEN "vacuous"."future_amt" ELSE NULL END as "_virt_filter_amt_6555309613325550"
  FROM
      "vacuous"),
  young as (
  SELECT
      "concerned"."_virt_filter_amt_2697784922538321" as "_virt_filter_amt_2697784922538321",
      "concerned"."_virt_filter_amt_3768105804604636" as "_virt_filter_amt_3768105804604636",
      "concerned"."_virt_filter_amt_4058952042352307" as "_virt_filter_amt_4058952042352307",
      "concerned"."_virt_filter_amt_4244070725580287" as "_virt_filter_amt_4244070725580287",
      "concerned"."_virt_filter_amt_6555309613325550" as "_virt_filter_amt_6555309613325550",
      "concerned"."_virt_filter_amt_8542750418198385" as "_virt_filter_amt_8542750418198385",
      "concerned"."_virt_filter_amt_8659369554987272" as "_virt_filter_amt_8659369554987272",
      "concerned"."future_ws" - 53 as "_virt_func_subtract_2729598913764358"
  FROM
      "concerned"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8),
  abhorrent as (
  SELECT
      "abundant"."daily_amt" as "daily_amt",
      "abundant"."daily_dow" as "daily_dow",
      "abundant"."daily_ws" as "daily_ws",
      "young"."_virt_filter_amt_2697784922538321" as "_virt_filter_amt_2697784922538321",
      "young"."_virt_filter_amt_3768105804604636" as "_virt_filter_amt_3768105804604636",
      "young"."_virt_filter_amt_4058952042352307" as "_virt_filter_amt_4058952042352307",
      "young"."_virt_filter_amt_4244070725580287" as "_virt_filter_amt_4244070725580287",
      "young"."_virt_filter_amt_6555309613325550" as "_virt_filter_amt_6555309613325550",
      "young"."_virt_filter_amt_8542750418198385" as "_virt_filter_amt_8542750418198385",
      "young"."_virt_filter_amt_8659369554987272" as "_virt_filter_amt_8659369554987272"
  FROM
      "young"
      INNER JOIN "abundant" on "young"."_virt_func_subtract_2729598913764358" = "abundant"."daily_ws"),
  sweltering as (
  SELECT
      "abhorrent"."_virt_filter_amt_2697784922538321" as "_virt_filter_amt_2697784922538321",
      "abhorrent"."_virt_filter_amt_3768105804604636" as "_virt_filter_amt_3768105804604636",
      "abhorrent"."_virt_filter_amt_4058952042352307" as "_virt_filter_amt_4058952042352307",
      "abhorrent"."_virt_filter_amt_4244070725580287" as "_virt_filter_amt_4244070725580287",
      "abhorrent"."_virt_filter_amt_6555309613325550" as "_virt_filter_amt_6555309613325550",
      "abhorrent"."_virt_filter_amt_8542750418198385" as "_virt_filter_amt_8542750418198385",
      "abhorrent"."_virt_filter_amt_8659369554987272" as "_virt_filter_amt_8659369554987272",
      "abhorrent"."daily_ws" as "daily_ws",
      CASE WHEN "abhorrent"."daily_dow" = 0 THEN "abhorrent"."daily_amt" ELSE NULL END as "_virt_filter_amt_650095515534974",
      CASE WHEN "abhorrent"."daily_dow" = 1 THEN "abhorrent"."daily_amt" ELSE NULL END as "_virt_filter_amt_4779989548823127",
      CASE WHEN "abhorrent"."daily_dow" = 2 THEN "abhorrent"."daily_amt" ELSE NULL END as "_virt_filter_amt_7736816780329151",
      CASE WHEN "abhorrent"."daily_dow" = 3 THEN "abhorrent"."daily_amt" ELSE NULL END as "_virt_filter_amt_1206176311612821",
      CASE WHEN "abhorrent"."daily_dow" = 4 THEN "abhorrent"."daily_amt" ELSE NULL END as "_virt_filter_amt_8317569539795251",
      CASE WHEN "abhorrent"."daily_dow" = 5 THEN "abhorrent"."daily_amt" ELSE NULL END as "_virt_filter_amt_1961485362790620",
      CASE WHEN "abhorrent"."daily_dow" = 6 THEN "abhorrent"."daily_amt" ELSE NULL END as "_virt_filter_amt_8020044413890467"
  FROM
      "abhorrent")
  SELECT
      "sweltering"."daily_ws" as "weekday_week_seq",
      round(("sweltering"."_virt_filter_amt_650095515534974") / nullif(("sweltering"."_virt_filter_amt_3768105804604636"),0),2) as "sunday",
      round(("sweltering"."_virt_filter_amt_4779989548823127") / nullif(("sweltering"."_virt_filter_amt_8542750418198385"),0),2) as "monday",
      round(("sweltering"."_virt_filter_amt_7736816780329151") / nullif(("sweltering"."_virt_filter_amt_8659369554987272"),0),2) as "tuesday",
      round(("sweltering"."_virt_filter_amt_1206176311612821") / nullif(("sweltering"."_virt_filter_amt_4058952042352307"),0),2) as "wednesday",
      round(("sweltering"."_virt_filter_amt_8317569539795251") / nullif(("sweltering"."_virt_filter_amt_2697784922538321"),0),2) as "thursday",
      round(("sweltering"."_virt_filter_amt_1961485362790620") / nullif(("sweltering"."_virt_filter_amt_4244070725580287"),0),2) as "friday",
      round(("sweltering"."_virt_filter_amt_8020044413890467") / nullif(("sweltering"."_virt_filter_amt_6555309613325550"),0),2) as "saturday"
  FROM
      "sweltering"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8
  ORDER BY
      "weekday_week_seq" asc nulls first, this should never occur. Please create an issue to report this.
  ```
- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: 3 undefined concept references; fix all before re-running:
    - first_name (line 27, col 5, in ORDER BY); did you mean: sales.billing_customer.first_name, sales.ship_customer.first_name, sales.purchasing_customer.first_name?
    - last_name (line 28, col 5, in ORDER BY); did you mean: sales.billing_customer.last_name, sales.ship_customer.last_name, sales.purchasing_customer.last_name?
    - preferred_cust_flag (line 29, col 5, in ORDER BY); did you mean: sales.billing_customer.preferred_cust_flag, sales.ship_customer.preferred_cust_flag, sales.purchasing_customer.preferred_cust_flag?
  ```
- `trilogy run --import raw.all_sales:sales select sales.date.year, count(sales.row_counter) as cnt limit 20;`

  ```text
  Syntax error in stdin: Undefined concept: sales.row_counter (line 2, in SELECT). Suggestions: ['sales.return_amount', 'sales.coupon_amt', 'sales.warehouse.country']
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query11.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'overall_avg_val.global_avg', which is not in the SELECT projection (line 104). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --overall_avg_val.global_avg
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read raw/physical_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'ss.date.date', which is not in the SELECT projection (line 8). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --ss.date.date
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'local.per_cust_total', 'local.global_max_total', which are not in the SELECT projection (line 25). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.per_cust_total, --local.global_max_total
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Unterminated string starting at: line 1 column 54 (char 53). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read query54.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write --content import raw.store_sales as ss;
import raw.catalog_returns as cr;
import raw.physical_returns as pr;

# Compute per-item catalog a…ale_cost) as total_wholesale_cost,
    sum(ss.ext_list_price) as total_list_price,
    sum(ss.coupon_amt) as total_coupon_amt
limit 10; query64.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Ambiguous reference 'y1999.product_name': matches ['y1999.base_agg.product_name', 'y1999.base_agg.ss.item.product_name']. Qualify the full path to disambiguate.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {y1999.base_agg.ca_city, y1999.base_agg.ca_street_name, y1999.base_agg.ca_street_number, y1999.base_agg.ca_zip, y1999.base_agg.prod_name, y1999.base_agg.sa_city, y1999.base_agg.sa_street_name, y1999.base_agg.sa_street_number, y1999.base_agg.sa_zip, y1999.base_agg.sale_lines, y1999.base_agg.sale_year, y1999.base_agg.store_name, y1999.base_agg.store_zip, y1999.base_agg.tot_coupon_amt, y1999.base_agg.tot_list_price, y1999.base_agg.tot_wholesale_cost, y2000.base_agg.item_id, y2000.base_agg.sale_lines, y2000.base_agg.sale_year, y2000.base_agg.store_name, y2000.base_agg.store_zip, y2000.base_agg.tot_coupon_amt, y2000.base_agg.tot_list_price, y2000.base_agg.tot_wholesale_cost}
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Ambiguous reference 'yr2001.brand_id': matches ['yr2001.books_agg.books_lines.brand_id', 'yr2001.books_agg.brand_id']. Qualify the full path to disambiguate.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 24 column 12 (char 1113). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 42 column 12 (char 1579). Re-issue the call with valid JSON arguments.
  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Filter to WEB and CATALOG only
# Aggregate sales by week_seq and day_of_week…turday
inner join daily_sales_current.week_seq = daily_sales_future.week_seq - 53
where cur.ws in weeks_in_2001.ws
order by cur.ws asc nulls first
;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:1
    |
  8 | rowset daily_sales_current as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...d daily sales for web+catalog ??? rowset daily_sales_current as

  Write stats: received 4020 chars / 4020 bytes; tail: …'ur.ws in weeks_in_2001.ws\\norder by cur.ws asc nulls first\\n;\\n'.
  ```
- `trilogy file write query02.preql`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...f-join approach with rowsets
   ??? rowset dow_sales as
   select


  Write stats: received 2289 chars / 2289 bytes; tail: …'k_seq in weeks_in_2001\r\\norder by cur.ws asc nulls first\r\\n;\r\\n'.
  ```
- `trilogy file write query02.preql`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | rowset cur_sales as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...f_week) for web+catalog only
   ??? rowset cur_sales as
   where all...

  Write stats: received 2140 chars / 2140 bytes; tail: …'in weeks_in_2001\r\\norder by cur_sales.ws asc nulls first\r\\n;\r\\n'.
  ```
- `trilogy run --import raw.all_sales:sales select sales.date.year, count(sales.row_counter) as cnt, sum(sales.ext_list_price - coalesce(sales.ext_discount_amount, 0)) as rev group by sales.date.year order by sales.date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...t_discount_amount, 0)) as rev ??? group by sales.date.year order...
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Stack all th…2001.cat_id)
order by _level asc, channel asc nulls first, brand_id asc nulls first, class_id asc nulls first, category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 97:5
     |
  97 |     stacked_nov2001.b_id in (select qualifying.b_id where qualifying.c_id = stacked_nov2001.c_id and qualifying.cat_id = stacked_nov2001.cat_id) as _qual
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...nov2001.cat_id) as _level     ??? stacked_nov2001.b_id in (selec...

  Write stats: received 4020 chars / 4020 bytes; tail: …'s_id asc nulls first, category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# ---- Step 1:…id,
         web.quantity * web.list_price as sale_val,
         web.line_item as line_item
    )
) -> (ch, b_id, c_id, cat_id, sale_val, line_item);`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 39:70
     |
  39 |     count(all_channels.sale_val) > 0 and count(all_channels.sale_val by all_channels.b_id, all_channels.c_id, all_channels.cat_id) ???
     |                                                                      ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...d count(all_channels.sale_val ??? by all_channels.b_id, all_chan...

  Write stats: received 2623 chars / 2623 bytes; tail: …'em\\n    )\\n) -> (ch, b_id, c_id, cat_id, sale_val, line_item);'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# ---- Step 1:….global_avg)
order by _level asc, channel asc nulls first, brand_id asc nulls first, class_id asc nulls first, category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
     --> 117:1
      |
  117 | having
      | ^---
      |
      = expected limit or order_by
  Location:
  ...raw.c_id, nov2001_raw.cat_id) ??? having     total_sales > (sele...

  Write stats: received 4854 chars / 4854 bytes; tail: …'s_id asc nulls first, category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# ---- Step 1:….global_avg)
order by _level asc, channel asc nulls first, brand_id asc nulls first, class_id asc nulls first, category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
     --> 113:1
      |
  113 | having total_sales > (select overall_avg_val.global_avg)
      | ^---
      |
      = expected limit or order_by
  Location:
  ...raw.c_id, nov2001_raw.cat_id) ??? having total_sales > (select o...

  Write stats: received 4571 chars / 4571 bytes; tail: …'s_id asc nulls first, category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# ---- Step 1:….global_avg)
order by _level asc, channel asc nulls first, brand_id asc nulls first, class_id asc nulls first, category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
     --> 113:1
      |
  113 | having total_sales > (select overall_avg_val.global_avg)
      | ^---
      |
      = expected limit or order_by
  Location:
  ...raw.c_id, nov2001_raw.cat_id) ??? having total_sales > (select o...

  Write stats: received 4571 chars / 4571 bytes; tail: …'s_id asc nulls first, category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# ---- Step 1:…category_id)
order by _level asc, channel asc nulls first, brand_id asc nulls first, class_id asc nulls first, category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...of_sales having total_sales > ??? (select overall_avg_val.global...

  Write stats: received 5006 chars / 5006 bytes; tail: …'s_id asc nulls first, category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Frequent items: items…mbined.sales_total
order by
    combined.last_name nulls first,
    combined.first_name nulls first,
    combined.sales_total nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 61:25
     |
  61 | with combined as union((catalog_feb), (web_feb)) -> (last_name, first_name, sales_total)
     |                         ^---
     |
     = expected select_statement
  Location:
  ...esult with combined as union(( ??? catalog_feb), (web_feb)) -> (l...

  Write stats: received 2147 chars / 2147 bytes; tail: …'ulls first,\\n    combined.sales_total nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Frequent items: items…mbined.sales_total
order by
    combined.last_name nulls first,
    combined.first_name nulls first,
    combined.sales_total nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` — a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...name, first_name, sales_total) ???  select     combined.last_nam...

  Write stats: received 2325 chars / 2325 bytes; tail: …'ulls first,\\n    combined.sales_total nulls first\\nlimit 100;\\n'.
  ```

### `file-not-found`

- `trilogy run query02_test.preql`

  ```text
  Input 'query02_test.preql' does not exist.
  ```

### `undefined-concept`

- `trilogy explore query64.preql`

  ```text
  Failed to parse query64.preql: (UndefinedConceptException(...), "Ambiguous reference 'y1999.product_name': matches ['y1999.base_agg.product_name', 'y1999.base_agg.ss.item.product_name']. Qualify the full path to disambiguate.")
  ```
