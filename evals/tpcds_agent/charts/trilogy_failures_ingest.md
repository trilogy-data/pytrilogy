# Trilogy failure analysis — 20260601-230446

- Run `20260601-230442_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 236 | failed: 29 (12%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `undefined-concept` | 10 | 34% |
| `syntax-parse` | 8 | 28% |
| `other` | 8 | 28% |
| `syntax-missing-alias` | 2 | 7% |
| `join-resolution` | 1 | 3% |

## Detail

### `undefined-concept`

- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['ss.customer.first_name']")
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['ss.customer.first_name']")
  ```
- `trilogy run query05.preql duckdb`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  web_returns.web_site.site_sk. Suggestions: ['web_sales.web_site.site_sk',
  'web_returns.item.item_sk', 'web_sales.web_site.site_id']")
  ```
- `trilogy run query05.preql duckdb`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  web_returns.web_site.site_sk. Suggestions: ['web_sales.web_site.site_sk',
  'web_returns.item.item_sk', 'web_sales.web_site.site_id']")
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  customer.preferred_cust_flag. Suggestions:
  ['store_sales.customer.preferred_cust_flag']")
  ```
- `trilogy run --import raw.customer:customer --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235…st_flag = 'Y') as pref_count where customer.customer_address.zip in unnest(split(zips,',')) having pref_count > 10 order by pref_count desc limit 20;`

  ```text
  (UndefinedConceptException(...), 'Undefined concept: zips.')
  ```
- `trilogy run query09.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  store_sales.quantity.')
  ```
- `trilogy run --import raw/store_sales:store_sales select date_dim.year, count(store_sales.ticket_number) as cnt limit 5;`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  date_dim.year. Suggestions: ['store_sales.date_dim.year',
  'store_sales.date_dim.fy_year']")
  ```
- `trilogy run --import raw/web_sales:web_sales select web_sales.sold_date.year, count(web_sales.ws_item_sk) as cnt limit 5;`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  web_sales.ws_item_sk. Suggestions: ['web_sales.item.item_sk',
  'web_sales.web_site.site_sk', 'web_sales.item.size']")
  ```
- `trilogy run --import raw/catalog_sales:catalog_sales select catalog_sales.sold_date.year, count(catalog_sales.cs_item_sk) as cnt limit 5;`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  catalog_sales.cs_item_sk. Suggestions: ['catalog_sales.item.item_sk',
  'catalog_sales.item.size', 'catalog_sales.item.units']")
  ```

### `syntax-parse`

- `trilogy file write query01.preql --content import raw.store_returns as store_returns;

# Filter: returns to physical stores in Tennessee, year 2000
where sto…customer_id,
    --cust_store_total,
    --store_avg
having cust_store_total > 1.2 * store_avg
order by store_returns.customer.customer_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 9, column 1.
  Expected one of:
          * SELECT

  Location:
  ...but keep explicit for safety) ??? auto cust_store_total <- sum(s...

  Write stats: received 759 chars / 759 bytes; tail: …'e_avg\\norder by
  store_returns.customer.customer_id\\nlimit 100;'.
  ```
- `trilogy file write query05.preql`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'union ') at line 34, column 1.
  Expected one of:
          * _TERMINATOR
          * LIMIT
          * MERGE
          * ORDER
  Previous tokens: [Token('RPAR', ')')]

  Location:
   is null and profit is null) ??? union all select     'catalo...

  Write stats: received 3395 chars / 3395 bytes; tail: …'t is null)\r\\norder by
  channel, outlet nulls first\r\\nlimit 100;'.
  ```
- `trilogy file write query05.preql`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'union') at line 50, column 1.
  Expected one of:
          * LIMIT
          * _TERMINATOR
          * MERGE
          * ORDER

  Location:
  ...eturns != 0 or profit != 0  ??? union all  select     'cata...

  Write stats: received 3986 chars / 3986 bytes; tail: …'fit != 0\r\\n\r\\norder
  by channel, outlet nulls first\r\\nlimit 100;'.
  ```
- `trilogy file write query05.preql`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'union') at line 44, column 1.
  Expected one of:
          * LIMIT
          * _TERMINATOR
          * ORDER
          * MERGE

  Location:
  ...eturns != 0 or profit != 0  ??? union all  select     'cata...

  Write stats: received 3649 chars / 3649 bytes; tail: …'fit != 0\r\\n\r\\norder
  by channel, outlet nulls first\r\\nlimit 100;'.
  ```
- `trilogy file write query06.preql --content import raw.store_sales as store_sales;

auto item_price_at_item <- store_sales.item.current_price by store_sales.i…number) as qualifying_line_items
having qualifying_line_items >= 10
order by qualifying_line_items asc nulls first, state asc nulls first
limit 100;
`

  ```text
  refused to write 'query06.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 3, column 59.
  Expected one of:
          * _TERMINATOR

  Location:
  ...tore_sales.item.current_price ??? by store_sales.item.item_sk; a...

  Write stats: received 691 chars / 691 bytes; tail: …'ine_items asc nulls first,
  state asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query09.preql --content # Query 9: Single row with bucket-based metrics from store_sales
# For each quantity bucket, if line count > thres…val as qty_1_20,
    bucket_2_val as qty_21_40,
    bucket_3_val as qty_41_60,
    bucket_4_val as qty_61_80,
    bucket_5_val as qty_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 11, column 2.
  Expected one of:
          * WHEN

  Location:
  ...s.quantity between 1 and 20) ) ??? ;  # Bucket 2: quantity 21-40,...

  Write stats: received 2111 chars / 2111 bytes; tail: …'_4_val as qty_61_80,\\n
  bucket_5_val as qty_81_100\\nlimit 1;'.
  ```
- `trilogy file write query09.preql --content # Query 9: Single row with bucket-based metrics from store_sales
# For each quantity bucket, if line count > thres…val as qty_1_20,
    bucket_2_val as qty_21_40,
    bucket_3_val as qty_41_60,
    bucket_4_val as qty_61_80,
    bucket_5_val as qty_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'bucket_1_count ') at line 8, column
  10.
  Expected one of:
          * RPAR
          * COMMA
  Previous tokens: [Token('IDENTIFIER', 'when')]

  Location:
  ...ucket_1_val <- case(     when ??? bucket_1_count > 74129 then av...

  Write stats: received 2156 chars / 2156 bytes; tail: …'_4_val as qty_61_80,\\n
  bucket_5_val as qty_81_100\\nlimit 1;'.
  ```
- `trilogy file write query09.preql --content # Query 9: Single row with bucket-based metrics from store_sales
# For each quantity bucket, if line count > thres…val as qty_1_20,
    bucket_2_val as qty_21_40,
    bucket_3_val as qty_41_60,
    bucket_4_val as qty_61_80,
    bucket_5_val as qty_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 11, column 2.
  Expected one of:
          * WHEN

  Location:
  ...s.quantity between 1 and 20) ) ??? ;  # Bucket 2: quantity 21-40,...

  Write stats: received 2111 chars / 2111 bytes; tail: …'_4_val as qty_61_80,\\n
  bucket_5_val as qty_81_100\\nlimit 1;'.
  ```

### `other`

- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 34 column 2 (char 3047). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query05.preql duckdb`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.store_outlet', 'local.store_loss_agg',
  'local.store_profit_agg'} out of  with found {'local.store_sales_agg',
  'local.store_loss_agg', 'store_sales.store.store_sk', 'local.store_profit_agg',
  'store_returns.store.store_sk', 'local.store_returns_agg'}
  ```
- `trilogy run query05.preql duckdb`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.store_loss_agg', 'local.store_profit_agg'} out of  with found
  {'local.store_profit_agg', 'local.store_returns_agg', 'local.store_sales_agg',
  'local.store_loss_agg', 'store_returns.store.store_sk',
  'store_sales.store.store_sk'}
  ```
- `trilogy run query05.preql duckdb`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.sp_amt', 'local.sl_amt'} out of  with found {'local.sp_amt',
  'local.sr_amt', 'local.ss_amt', 'store_sales.store.store_sk', 'local.sl_amt',
  'store_returns.store.store_sk'}
  ```
- `trilogy run query05.preql duckdb`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.store_profit_agg', 'local.store_loss_agg'} out of  with found
  {'store_sales.store.store_sk', 'local.store_returns_agg',
  'local.store_loss_agg', 'local.store_sales_agg',
  'store_returns.store.store_sk', 'local.store_profit_agg'}
  ```
- `trilogy run query05.preql duckdb`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.store_loss_by_sk', 'local.store_profit_by_sk'} out of  with
  found {'local.store_returns_by_sk', 'local.store_sales_by_sk',
  'store_sales.store.store_sk', 'local.store_loss_by_sk',
  'local.store_profit_by_sk', 'store_returns.store.store_sk'}
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: Cannot compare values
  of type VARCHAR and VARCHAR[] in IN/ANY/ALL clause - an explicit cast is
  required

  LINE 36: ...521669" is not null) and "store_sales_store_store"."s_zip" in
  (select quizzical."_virt_func_split_4785012549328100...
                                                                         ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "store_sales_customer_customer_address_customer_address"."ca_zip" as
  "store_sales_customer_customer_address_zip",
      count("store_sales_customer_customer"."c_customer_sk") as
  "_virt_agg_count_1111253264068160
  …
  96521669" is not null) and
  "store_sales_store_store"."s_zip" in (select
  quizzical."_virt_func_split_4785012549328100" from quizzical where
  quizzical."_virt_func_split_4785012549328100" is not null)

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."ss_item_sk",
      "store_sales_store_sales"."ss_ticket_number")
  SELECT
      "concerned"."store_sales_store_store_name" as "store_name",
      sum("concerned"."store_sales_net_profit") as "total_net_profit"
  FROM
      "concerned"
  GROUP BY
      1
  ORDER BY
      "store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: UNNEST not supported
  here

  LINE 33:     unnest(STRING_SPLIT( $1 , ',' )) in (select cooperative...
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "store_sales_customer_customer_address_customer_address"."ca_zip" as
  "store_sales_customer_customer_address_zip",
      count("store_sales_customer_customer"."c_customer_sk") as
  "_virt_agg_count_1111253264068160"
  FROM
      "customer_address" as
  "store_sales_customer_customer_address_customer_address"
      INNER JOIN "customer" as "store_sales_customer_customer" on
  "store_sales_customer_customer_address_
  …
  dim"."d_year" = 1998 and
  SUBSTRING("store_sales_store_store"."s_zip",1,2) in (select
  uneven."_virt_func_substring_9221055437369198" from uneven where
  uneven."_virt_func_substring_9221055437369198" is not null)

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."ss_item_sk",
      "store_sales_store_sales"."ss_ticket_number")
  SELECT
      "young"."store_sales_store_store_name" as "store_name",
      sum("young"."store_sales_net_profit") as "total_net_profit"
  FROM
      "young"
  GROUP BY
      1
  ORDER BY
      "store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/date_dim:d select d.week_seq, d.year, count(d.date_sk) where d.year = 2001 group by d.week_seq, d.year order by d.week_seq;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(d.date_sk) as date_sk_count`
  Location:
  ...seq, d.year, count(d.date_sk) ??? where d.year = 2001 group by d...
  ```
- `trilogy run --import raw/store_sales:store_sales select date_dim.year, count(store_sales.ticket_number) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(store_sales.ticket_number) as
  ticket_number_count`
  Location:
  ...nt(store_sales.ticket_number) ??? limit 5;
  ```

### `join-resolution`

- `trilogy run query05.preql duckdb`

  ```text
  Could not resolve connections for query with output
  ['store_sales.store.store_id<Purpose.PROPERTY>Derivation.ROOT>',
  'local.store_sales_sum<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.store_returns_sum<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.store_profit_sum<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.store_loss_sum<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model. The output draws on models that are not connected in the current graph:
  store_returns (needed by local.store_loss_sum, local.store_returns_sum);
  store_sales (needed by local.store_profit_sum, local.store_sales_sum,
  store_sales.store.store_id). If these should be related, bridge their keys with
  a merge, e.g. `merge store_returns.<key> into ~store_sales.<key>;`.
  ```
