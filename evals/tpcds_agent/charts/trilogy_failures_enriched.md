# Trilogy failure analysis — 20260531-185715

- Run `20260531-185714_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 200 | failed: 28 (14%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `join-resolution` | 10 | 36% |
| `other` | 9 | 32% |
| `syntax-parse` | 6 | 21% |
| `undefined-concept` | 2 | 7% |
| `cli-misuse` | 1 | 4% |

## Detail

### `join-resolution`

- `trilogy run query54.preql`

  ```text
  Could not resolve connections for query with output
  ['local.segment<Purpose.PROPERTY>Derivation.BASIC>',
  'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.segment_times_50<Purpose.PROPERTY>Derivation.BASIC>'] from current
  model.
  ```
- `trilogy run query54.preql`

  ```text
  Could not resolve connections for query with output
  ['local.segment<Purpose.PROPERTY>Derivation.BASIC>',
  'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.segment_times_50<Purpose.PROPERTY>Derivation.BASIC>'] from current
  model.
  ```
- `trilogy run query54.preql`

  ```text
  Could not resolve connections for query with output
  ['local.segment<Purpose.PROPERTY>Derivation.BASIC>',
  'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.segment_times_50<Purpose.PROPERTY>Derivation.BASIC>'] from current
  model.
  ```
- `trilogy run test_simple.preql`

  ```text
  Could not resolve connections for query with output
  ['ps.billing_customer.id<Purpose.KEY>Derivation.ROOT>',
  'local.sale_count<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query54.preql`

  ```text
  Could not resolve connections for query with output
  ['local.segment<Purpose.PROPERTY>Derivation.BASIC>',
  'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.segment_times_50<Purpose.PROPERTY>Derivation.BASIC>'] from current
  model.
  ```
- `trilogy run query54.preql`

  ```text
  Could not resolve connections for query with output
  ['local.segment<Purpose.PROPERTY>Derivation.BASIC>',
  'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.segment_times_50<Purpose.PROPERTY>Derivation.BASIC>'] from current
  model.
  ```
- `trilogy run query54.preql`

  ```text
  Could not resolve connections for query with output
  ['ps.billing_customer.id<Purpose.KEY>Derivation.ROOT>',
  'local.segment<Purpose.PROPERTY>Derivation.BASIC>',
  'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.segment_times_50<Purpose.PROPERTY>Derivation.BASIC>'] from current
  model.
  ```
- `trilogy run query54.preql`

  ```text
  Could not resolve connections for query with output
  ['ps.billing_customer.id<Purpose.KEY>Derivation.ROOT>',
  'local.segment<Purpose.PROPERTY>Derivation.BASIC>',
  'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.segment_times_50<Purpose.PROPERTY>Derivation.BASIC>'] from current
  model.
  ```
- `trilogy run test_simple.preql`

  ```text
  Could not resolve connections for query with output
  ['ps.billing_customer.id<Purpose.KEY>Derivation.ROOT>',
  'local.cust_total<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run test_simple.preql`

  ```text
  Could not resolve connections for query with output
  ['ps.billing_customer.id<Purpose.KEY>Derivation.ROOT>',
  'local.cust_total<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```

### `other`

- `trilogy run query51.preql duckdb`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query51.preql duckdb`

  ```text
  (AmbiguousRelationshipResolutionException(...), "Multiple
  possible concept additions (intermediate join keys) found to resolve
  ['ps.date.date', 'ps.item.id', 'ws.date.date', 'ws.item.id'], have
  {'local.ws_rt_max', 'local.ps_rt_max', 'local.combined_date'} or
  {'local.ws_rt_max', 'local.ps_rt_max', 'local.combined_item'} or
  {'local.ws_rt_max', 'local.ps_rt_max', 'local.combined_item'} or
  {'local.ws_rt_max', 'local.ps_rt_max', 'local.combined_item'} or
  {'local.ws_rt_max', 'local.ps_rt_max', 'local.combined_item'} or
  {'local.ws_rt_max', 'local.ps_rt_max', 'local.combined_item'} or
  {'loca
  …
  cal.ps_rt_max', 'local.combined_item'}. Different paths
  are is: [{'local.combined_date'}, {'local.combined_item'},
  {'local.combined_item'}, {'local.combined_item'}, {'local.combined_item'},
  {'local.combined_item'}, {'local.combined_item'}, {'local.combined_item'},
  {'local.combined_item'}, {'local.combined_item'}, {'local.combined_item'},
  {'local.combined_item'}, {'local.combined_item'}, {'local.combined_item'},
  {'local.combined_item'}, {'local.combined_item'}, {'local.combined_item'},
  {'local.combined_item'}, {'local.combined_item'}, {'local.combined_item'},
  {'local.combined_item'}]")
  ```
- `trilogy run query53.preql`

  ```text
  (_duckdb.BinderException) Binder Error: Table
  "store_sales_store_store" does not have a column named "S_CLOSED_DATE"

  Candidate bindings: : "s_closed_date_sk"

  LINE 12: ...    INNER JOIN "date_dim" as "store_sales_store_date_date" on
  "store_sales_store_store"."S_CLOSED_DATE" = "store_sales_st...
                                                                            ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "store_sales_item_items"."I_MANUFACT_ID" as
  "store_sales_item_manufacturer_id",
      "store_sales_store_date_date"."D_QOY" as "store_sales_store_date_quarter",
      sum("store_sales_sto
  …
  e_sales_store_date_quarter",
      "thoughtful"."quarterly_total" as "quarterly_total"
  FROM
      "thoughtful"
      INNER JOIN "questionable" on
  "thoughtful"."store_sales_item_manufacturer_id" =
  "questionable"."store_sales_item_manufacturer_id"
  WHERE
      abs("thoughtful"."quarterly_total" - "questionable"."avg_quarterly_sales")
  / "questionable"."avg_quarterly_sales" > 0.1

  ORDER BY
      "questionable"."avg_quarterly_sales" asc,
      "thoughtful"."quarterly_total" asc,
      "thoughtful"."store_sales_item_manufacturer_id" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 35 column 12 (char 1475). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query54.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.base_ms', 'local.cat_flag'} out of  with found
  {'cs.billing_customer.id', 'ps.billing_customer.address.state',
  'ps.billing_customer.address.county', 'ps.store.county', 'ps.ticket_number',
  'local.web_flag', 'ps.ext_sales_price', 'ps.store.state',
  'ws.billing_customer.id', 'ps.date.month_seq', 'ps.billing_customer.id',
  'ps.item.id', 'local.cat_flag'}
  ```
- `trilogy run query54.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.base_ms', 'local.qual_flag'} out of  with found
  {'ps.date.month_seq', 'local.qual_flag', 'ws.billing_customer.id',
  'ps.ticket_number', 'ps.store.county', 'ps.ext_sales_price',
  'ps.billing_customer.address.county', 'ps.billing_customer.id',
  'ps.billing_customer.address.state', 'ps.store.state',
  'cs.billing_customer.id', 'ps.item.id'}
  ```
- `trilogy run query54.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.base_ms', 'local.qual_flag'} out of  with found
  {'ps.store.state', 'ps.item.id', 'ps.date.month_seq', 'ps.billing_customer.id',
  'ps.billing_customer.address.state', 'ps.store.county', 'ps.ext_sales_price',
  'ps.ticket_number', 'cs.billing_customer.id', 'local.qual_flag',
  'ps.billing_customer.address.county'}
  ```
- `trilogy run query56.preql`

  ```text
  Unable to import '.\physical_sales.preql': [Errno 2] No such
  file or directory: '.\\physical_sales.preql'. Did you mean: raw.physical_sales?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 41 column 12 (char 1836). Re-issue the call with valid JSON arguments.
  ```

### `syntax-parse`

- `trilogy file write query50.preql --content import raw.catalog_store_returns as csr;

# Filter: return in August 2001
where csr.store_return_date.year = 2001
…e,
    csr.store.street_type,
    csr.store.suite_number,
    csr.store.city,
    csr.store.county,
    csr.store.state,
    csr.store.zip
limit 100;`

  ```text
  refused to write 'query50.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 8, column 1.
  Expected one of:
          * SELECT

  Location:
  ...le and return (positive only) ??? auto elapsed_days <- date_diff...

  Write stats: received 1595 chars / 1595 bytes; tail: …'re.county,\\n
  csr.store.state,\\n    csr.store.zip\\nlimit 100;'.
  ```
- `trilogy file write query51.preql -e -c import raw.all_sales as s;

# Year 2000 only
where s.date.year = 2000

# Daily totals per item per date, split by chan…unning_max,
    ps_rt_max as store_running_max
having web_running_max > store_running_max
order by item_id asc, sale_date asc nulls first
limit 100;
`

  ```text
  refused to write 'query51.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 7, column 1.
  Expected one of:
          * SELECT

  Location:
  ...em per date, split by channel ??? auto ws_daily <- sum(s.sales_p...

  Write stats: received 1014 chars / 1014 bytes; tail: …'\\norder by item_id asc,
  sale_date asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query51.preql -e -c import raw.all_sales as s;

# Year 2000 only
where s.date.year = 2000

# Daily totals per item per date, split by chan…unning_max,
    ps_rt_max as store_running_max
having web_running_max > store_running_max
order by item_id asc, sale_date asc nulls first
limit 100;
`

  ```text
  refused to write 'query51.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 7, column 1.
  Expected one of:
          * SELECT

  Location:
  ...em per date, split by channel ??? auto ws_daily <- sum(s.sales_p...

  Write stats: received 1014 chars / 1014 bytes; tail: …'\\norder by item_id asc,
  sale_date asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query53.preql --content import raw.physical_sales as store_sales;

# Filter: store sales in year 2000
where store_sales.store.date.year = …g_quarterly_sales) / avg_quarterly_sales > 0.1
order by avg_quarterly_sales asc, quarterly_total asc, store_sales.item.manufacturer_id asc
limit 100;`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 22, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...y manufacturer_id and quarter ??? auto quarterly_total <- sum(st...

  Write stats: received 1548 chars / 1548 bytes; tail: …'y_total asc,
  store_sales.item.manufacturer_id asc\\nlimit 100;'.
  ```
- `trilogy file write query58.preql --content import raw.all_sales as all;
import raw.item as item;

# Date filter for the week containing 2000-01-03
where all.…eb_total
    and catalog_total >= 0.9 * web_total and catalog_total <= 1.1 * web_total
order by item_code asc, store_total asc nulls first
limit 100;`

  ```text
  refused to write 'query58.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 8, column 1.
  Expected one of:
          * SELECT

  Location:
  ...hannel totals by item text_id ??? auto store_total <- sum(all.ex...

  Write stats: received 1206 chars / 1206 bytes; tail: …'der by item_code asc,
  store_total asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/physical_sales:ss select ss.date.year, ss.date.week_seq, sum(ss.sales_price) as total_sales where ss.date.year in (2001,2002) group by ss.date.year, ss.date.week_seq order by ss.date.year, ss.date.week_seq limit 20;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...e ss.date.year in (2001,2002) ??? group by ss.date.year, ss.date...
  ```

### `undefined-concept`

- `trilogy run query50.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  ticket_number. Suggestions: ['csr.ticket_number']")
  ```
- `trilogy run query54.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  ws.sold_date.year. Suggestions: ['cs.sold_date.year', 'ws.date.year',
  'ws.ship_date.year']")
  ```

### `cli-misuse`

- `trilogy explore raw/as catalog_store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/as catalog_store_returns.preql' does not exist.
  ```
