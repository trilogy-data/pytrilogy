# Trilogy failure analysis — 20260530-212726

- Run `20260530-212725_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 390 | failed: 42 (11%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `undefined-concept` | 13 | 31% |
| `syntax-parse` | 12 | 29% |
| `other` | 8 | 19% |
| `syntax-missing-alias` | 8 | 19% |
| `cli-misuse` | 1 | 2% |

## Detail

### `undefined-concept`

- `trilogy run query06.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  item.category. Suggestions: ['store_sales.item.category',
  'store_sales.item.category_id']")
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  billing_customer.id. Suggestions: ['ss.billing_customer.id',
  'ss.billing_customer.text_id', 'ss.billing_customer.login']")
  ```
- `trilogy run --import raw.physical_sales:ss select min(ticket_qty) as min_qty, max(ticket_qty) as max_qty, count(ss.ticket_number) as num_tickets 
 where ticket_qty >= 1
 and ticket_qty is not null
`

  ```text
  (UndefinedConceptException(...), 'line: 2: Undefined concept:
  local.ticket_qty.')
  ```
- `trilogy run query10.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  catalog_sales.ship_customer.id. Suggestions: ['catalog_sales.bill_customer.id',
  'catalog_sales.ship_mode.id', 'catalog_sales.ship_date.id']")
  ```
- `trilogy run query14.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  brand_id.')
  ```
- `trilogy run query14.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  brand_id.')
  ```
- `trilogy run query14.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  brand_id.')
  ```
- `trilogy run query14.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  brand_id.')
  ```
- `trilogy run query14.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  brand_id.')
  ```
- `trilogy run query14.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  brand_id.')
  ```
- `trilogy run query14.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  brand_id.')
  ```
- `trilogy run query14.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  brand_id.')
  ```
- `trilogy run query14.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  brand_id.')
  ```

### `syntax-parse`

- `trilogy run --import raw.all_sales:all_sales auto dly <- sum(all_sales.ext_sales_price ? all_sales.sales_channel in ('WEB','CATALOG')) by all_sales.date.id;
… 53);

select all_sales.date.week_seq, s01_sun, s02_sun_aligned where all_sales.date.week_seq between 5270 and 5275 order by all_sales.date.week_seq;`

  ```text
  --> 12:33
     |
  12 | auto s02_sun_aligned <- s02_sun by (all_sales.date.week_seq - 53);
     |                                 ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...to s02_sun_aligned <- s02_sun ??? by (all_sales.date.week_seq -
  ```
- `trilogy file write query05.preql`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 7, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...label based on sales_channel ??? auto channel <- case(     all...

  Write stats: received 1145 chars / 1145 bytes; tail: …'der by channel nulls
  first, outlet nulls first\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 8, column 2.
  Expected one of:
          * WHEN

  Location:
  ...nel = 'WEB',     'web_site' ) ??? ;  # Outlet = channel prefix...

  Write stats: received 1103 chars / 1103 bytes; tail: …'der by channel nulls
  first, outlet nulls first\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql`

  ```text
  …
  by channel, outlet;\r\n\r\n# Date range filter
  and select with rollup\r\nwhere all_sales.date.date between '2000-08-23'::date
  and '2000-09-06'::date\r\n\r\nselect\r\n    channel,\r\n    outlet,\r\n
  sum(outlet_sales)   by rollup channel, outlet as total_sales,\r\n
  sum(outlet_returns) by rollup channel, outlet as total_returns,\r\n
  sum(outlet_profit)  by rollup channel, outlet as total_profit\r\norder by
  channel nulls first, outlet nulls first\r\nlimit 100;\r\n") at line 4, column
  28.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'simple_case')]

  Location:
  ...l auto channel <- simple_case ??? (     all_sales.sales_channel...

  Write stats: received 1062 chars / 1062 bytes; tail: …'der by channel nulls
  first, outlet nulls first\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 8, column 2.
  Expected one of:
          * WHEN

  Location:
  ...nel = 'WEB',     'web_site' ) ??? ;  # Outlet = channel prefix...

  Write stats: received 1103 chars / 1103 bytes; tail: …'der by channel nulls
  first, outlet nulls first\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as ss;

parameter zips string;

# Preferred customer count by ZIP code
auto pref_cust_cn…t) as z 
    where z in (select high_pref_zips)
  )

select
    store.name,
    sum(ss.net_profit) as total_net_profit
order by store.name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 9, column 53.
  Expected one of:
          * _TERMINATOR

  Location:
   billing_customer.address.zip ??? where pref_cust_cnt > 10;  # C...

  Write stats: received 1056 chars / 1056 bytes; tail: …'t_profit) as
  total_net_profit\\norder by store.name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql -e -c import raw.physical_sales as ss;

parameter zips string;

# Split curated ZIPs and unnest to individual values
auto cu…, 2) = substring(store.zip, 1, 2)) by store.id > 10

select
    store.name,
    sum(ss.net_profit) as total_net_profit
order by store.name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', "1));\n\nwhere ss.date.year =
  1998\n  and ss.date.quarter = 2\n  and count(billing_customer.id ?
  billing_customer.preferred_cust_flag = 'Y' and
  substring(billing_customer.address.zip, 1, 2) = substring(store.zip, 1, 2)) by
  store.id > 10\n\nselect\n    store.name,\n    sum(ss.net_profit) as
  total_net_profit\norder by store.name\nlimit 100;") at line 13, column 111.
  Expected one of:
          * "GROUPING"i
          * STAR
          * CUBE
          * ROLLUP
          * IDENTIFIER
          * LPAR
  Previous tokens: [Token('__ANON_7', 'by')]

  Location:
  ...ip ? pref_cnt_by_zip > 10) by ??? 1));  where ss.date.year = 199...

  Write stats: received 847 chars / 847 bytes; tail: …'t_profit) as
  total_net_profit\\norder by store.name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.physical_sales:ss select min(ticket_qty) as min_qty, max(ticket_qty) as max_qty, avg(ticket_qty) as avg_qty, count(distinct ss.ticket_number) as num_tickets from (select sum(ss.quantity) as ticket_qty from ss group by ss.ticket_number)`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...ticket_number) as num_tickets ??? from (select sum(ss.quantity)
  ```
- `trilogy file write query13.preql --content import raw.physical_sales as store_sales;

# Demographic-and-price clause: one of three combinations
# (1) married…vg(store_sales.ext_wholesale_cost) as avg_extended_wholesale_cost,
    sum(store_sales.ext_wholesale_cost) as total_extended_wholesale_cost
limit 10;`

  ```text
  refused to write 'query13.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 9, column 60.
  Expected one of:
          * RPAR
          * COMMA

  Location:
  ...ographic.marital_status = 'M' ??? and store_sales.customer_demog...

  Write stats: received 2163 chars / 2163 bytes; tail: …'t_wholesale_cost) as
  total_extended_wholesale_cost\\nlimit 10;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query13.preql --content import raw.physical_sales as store_sales;

# Demographic-and-price clause: one of three combinations
# (1) married…vg(store_sales.ext_wholesale_cost) as avg_extended_wholesale_cost,
    sum(store_sales.ext_wholesale_cost) as total_extended_wholesale_cost
limit 10;`

  ```text
  refused to write 'query13.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 8, column 76.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ographic.marital_status = 'M' ??? and store_sales.customer_demog...

  Write stats: received 2333 chars / 2333 bytes; tail: …'t_wholesale_cost) as
  total_extended_wholesale_cost\\nlimit 10;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.all_sales:sales select sales.item.brand_id as bid, sales.item.class_id as cid, sales.item.category_id as catid, count(sales.order_id) as cnt by bid, cid, catid limit 5;`

  ```text
  --> 2:126
    |
  2 | select sales.item.brand_id as bid, sales.item.class_id as cid,
  sales.item.category_id as catid, count(sales.order_id) as cnt by bid, cid,
  catid limit 5;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
   count(sales.order_id) as cnt ??? by bid, cid, catid limit 5;
  ```
- `trilogy run --import raw.all_sales:sales select sales.sales_channel as channel, sales.item.brand_id, sales.item.class_id, sales.item.category_id, sum(sales.q…id), (sales.item.class_id), (sales.item.category_id) = 3 order by channel, sales.item.brand_id, sales.item.class_id, sales.item.category_id limit 10;`

  ```text
  Syntax [211]: Expression in `by` clause must be wrapped in
  parens — write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work
  without parens, but any function call, cast, or other expression needs them.
  Location:
  ...e.year between 1999 and 2001) ??? by (sales.item.brand_id), (sal...
  ```

### `other`

- `trilogy run query01.preql`

  ```text
  HAVING references 'local.cust_store_total',
  'local.store_avg', which are not in the SELECT projection (line 9). Add them to
  SELECT, each prefixed with `--` so they stay out of the output rows — keep your
  HAVING as-is:
      select <your existing columns>, --local.cust_store_total, --local.store_avg
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query04.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query04.preql`

  ```text
  HAVING references 'local.catalog_02', 'local.catalog_01',
  'local.store_02', 'local.store_01', 'local.web_02', 'local.web_01', which are
  not in the SELECT projection (line 29). Add them to SELECT, each prefixed with
  `--` so they stay out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.catalog_02, --local.catalog_01,
  --local.store_02, --local.store_01, --local.web_02, --local.web_01
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query04.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query11.preql`

  ```text
  HAVING references 'local.web_rev_2002', 'local.web_rev_2001',
  'local.store_rev_2002', 'local.store_rev_2001', which are not in the SELECT
  projection (line 12). Add them to SELECT, each prefixed with `--` so they stay
  out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.web_rev_2002, --local.web_rev_2001,
  --local.store_rev_2002, --local.store_rev_2001
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query11.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query11.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query18.preql`

  ```text
  Value 'Female' is not valid for enum field
  'catalog_sales.bill_customer.demographics.gender'. Allowed values: 'M', 'F'.
  ```

### `syntax-missing-alias`

- `trilogy run query02.preql`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `ws_sun ? (all_sales.date.week_seq = w2002
  and all_sales.date.year = 2002) / ws_sun as
  ws_sun_all_sales_date_week_seq_w2002_and`
  Location:
  ...d all_sales.date.year = 2002) ??? / ws_sun as sun,     ws_mon ?
  ```
- `trilogy run --import raw.all_sales:all_sales auto dly <- sum(all_sales.ext_sales_price ? all_sales.sales_channel in ('WEB','CATALOG')) by all_sales.date.id;
…1 week
auto ws_map <- all_sales.date.week_seq + 53;

select
    all_sales.date.week_seq,
    w_sun,
    w_sun ? all_sales.date.year = 2002 
limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `w_sun ? all_sales.date.year = 2002 as
  w_sun_all_sales_date_year_2002`
  Location:
  ...? all_sales.date.year = 2002  ??? limit 10;
  ```
- `trilogy run --import raw.all_sales:sales select sales.sales_channel, count(sales.order_id) by sales.item.brand_id, sales.item.class_id, sales.item.category_id limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `sales.item.category_id as
  sales_item_category_id`
  Location:
  ...ss_id, sales.item.category_id ??? limit 5;
  ```
- `trilogy run --import raw.all_sales:sales select sales.item.brand_id, sales.item.class_id, sales.item.category_id, count(sales.order_id) by sales.item.brand_id, sales.item.class_id, sales.item.category_id limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `sales.item.category_id as
  sales_item_category_id`
  Location:
  ...ss_id, sales.item.category_id ??? limit 5;
  ```
- `trilogy run --import raw.all_sales:sales select count(sales.order_id) by sales.item.brand_id, sales.item.class_id, sales.item.category_id limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `sales.item.category_id as
  sales_item_category_id`
  Location:
  ...ss_id, sales.item.category_id ??? limit 5;
  ```
- `trilogy run --import raw.all_sales:sales select count(sales.order_id) by (sales.item.brand_id), (sales.item.class_id), (sales.item.category_id) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(sales.order_id) by
  (sales.item.brand_id) as brand_id_count`
  Location:
  ...r_id) by (sales.item.brand_id) ??? , (sales.item.class_id), (sale...
  ```
- `trilogy run --import raw.date:date select distinct date.quarter_name, date.year, date.quarter order by date.year, date.quarter limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `distinct date.quarter_name as
  distinct_date_quarter_name`
  Location:
  ...date as date; select distinct ??? date.quarter_name, date.year,
  ```
- `trilogy run --import raw.catalog_store_returns:csr where csr.store_sale_date.year = 2001 select csr.store_sale_date.quarter_name, count(csr.ticket_number) order by csr.store_sale_date.quarter_name;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(csr.ticket_number) as
  ticket_number_count`
  Location:
  ...ame, count(csr.ticket_number) ??? order by csr.store_sale_date.q...
  ```

### `cli-misuse`

- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
