# Trilogy failure analysis — 20260601-230447

- Run `20260601-230442_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 241 | failed: 25 (10%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 12 | 48% |
| `other` | 10 | 40% |
| `syntax-missing-alias` | 1 | 4% |
| `undefined-concept` | 1 | 4% |
| `join-resolution` | 1 | 4% |

## Detail

### `syntax-parse`

- `trilogy file write query01.preql --content import raw.physical_returns as r;

# Total returns per billing customer per store
auto cust_store_total <- sum(r.r…_date.year = 2000

select
    r.billing_customer.text_id,
    --qualifies
having
    qualifies = 1
order by
    r.billing_customer.text_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('RPAR', ')') at line 10, column 69.
  Expected one of:
          * WHEN

  Location:
  ...total > 1.2 * store_avg, 1, 0) ??? ) by r.billing_customer.id;  w...

  Write stats: received 636 chars / 636 bytes; tail: …'ifies = 1\\norder by\\n
  r.billing_customer.text_id\\nlimit 100;'.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as sales;

# Only catalog and web sales (exclude store)
auto cw_sales <- sales.ext_sales_pric…d sales.date.week_seq <= 5322 as is_2001
having sales.date.week_seq >= 5270 and sales.date.week_seq <= 5322
order by sales.date.week_seq nulls first;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', '--sun_r,\n
  --sales.date.week_seq >= 5270 and sales.date.week_seq <= 5322 as
  is_2001\nhaving sales.date.week_seq >= 5270 and sales.date.week_seq <=
  5322\norder by sales.date.week_seq nulls first;') at line 33, column 5.
  Expected one of:
          * METADATA
          * ORDER
          * WHERE
          * HAVING
          * _TERMINATOR
          * MERGE
          * LIMIT
          * COMMA
  Previous tokens: [Token('IDENTIFIER', 'saturday')]

  Location:
  ...ay,     sat_r as saturday     ??? --sun_r,     --sales.date.week...

  Write stats: received 1824 chars / 1824 bytes; tail: …'e.week_seq <=
  5322\\norder by sales.date.week_seq nulls first;'.
  ```
- `trilogy run --import raw.all_sales:sales select sales.date.week_seq as ws, sum(sales.ext_sales_price ? sales.date.day_of_week = 0 and sales.sales_channel in ('CATALOG','WEB')) as sun where sales.date.week_seq in (5270, 5323) group by ws;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...date.week_seq in (5270, 5323) ??? group by ws;
  ```
- `trilogy file write query04.preql --content import raw.all_sales as s;


# The per-line computation: ((ext_list_price - ext_wholesale_cost - ext_discount_amou…    customer_code asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;
`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 23, column 59.
  Expected one of:
          * SELECT

  Location:
  ...alog_2001 > 0 and web_2001 > 0 ??? ;  # Ratios: 2002 / 2001 (only...

  Write stats: received 1929 chars / 1929 bytes; tail: …'s first,\\n
  preferred_cust_flag asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Define the date range filter
where all_sales.date.date between '2000-08-23':…turn_date.date between '2000-08-23'::date and '2000-09-06'::date) as total_profit
order by channel asc nulls first, outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 19, column 7.
  Expected one of:
          * WHEN

  Location:
   = 'WEB', 'web channel'     ) ??? as channel,     case(

  Write stats: received 1627 chars / 1627 bytes; tail: …'y channel asc nulls
  first, outlet asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Filter: rows where sale date is in range OR return date is in range
where al… total_returns,
    coalesce(profit_sales, 0) - coalesce(loss_returns, 0) as total_profit
order by channel nulls first, outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...ate to its own date condition ??? auto sales_amt <- sum(all_sale...

  Write stats: received 1697 chars / 1697 bytes; tail: …'\\norder by channel
  nulls first, outlet nulls first\\nlimit 100;'.
  ```
- `trilogy run - --import raw.all_sales:all_sales`

  ```text
  --> 4:39
    |
  4 | select all_sales.sales_channel, count(*) as cnt,
  sum(all_sales.return_amount) as returns
    |                                       ^---
    |
    = expected access_chain
  Location:
  ...ll_sales.sales_channel, count( ??? *) as cnt, sum(all_sales.retur...
  ```
- `trilogy run - --import raw.all_sales:all_sales`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...unt(all_sales.row_one) as cnt ??? group by all_sales.sales_chann...
  ```
- `trilogy run - --import raw.all_sales:all_sales`

  ```text
  --> 3:104
    |
  3 | select all_sales.sales_channel, all_sales.channel_dim_text_id,
  sum(all_sales.return_amount) as returns sum(all_sales.net_profit) as profit
  limit 20;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...les.return_amount) as returns ??? sum(all_sales.net_profit) as p...
  ```
- `trilogy run - --import raw.all_sales:all_sales`

  ```text
  --> 4:240
    |
  4 | select sum(all_sales.ext_sales_price ? all_sales.date.date between
  '2000-08-23'::date and '2000-09-06'::date) as sales,
  sum(all_sales.return_amount ? all_sales.return_date.date between
  '2000-08-23'::date and '2000-09-06'::date) as returns by
  all_sales.sales_channel limit 10;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...2000-09-06'::date) as returns ??? by all_sales.sales_channel lim...
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Filter: sales whose sale date is in the date range
where all_sales.date.date…l), @outlet_label(all_sales.sales_channel, all_sales.channel_dim_text_id) as total_profit
order by channel nulls first, outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'def ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...ion to keep things consistent ??? def channel_label(c) -> case w...

  Write stats: received 1349 chars / 1349 bytes; tail: …'\\norder by channel
  nulls first, outlet nulls first\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Build channel and outlet labels using def for reuse
def channel_label(c) -> …l), @outlet_label(all_sales.sales_channel, all_sales.channel_dim_text_id) as total_profit
order by channel nulls first, outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS',
  '@channel_label(all_sales.sales_channel),
  @outlet_label(all_sales.sales_channel, all_sales.channel_dim_text_id) as
  total_sales,\n    sum(all_sales.return_amount) by rollup
  @channel_label(all_sales.sales_channel), @outlet_label(all_sales.sales_channel,
  all_sales.channel_dim_text_id) as total_returns,\n    sum(all_sales.net_profit)
  - sum(all_sales.return_net_loss) by rollup
  @channel_label(all_sales.sales_channel), @outlet_label(all_sales.sales_channel,
  all_sales.channel_dim_text_id) as total_profit\norder by channel nulls first,
  outlet nulls first\nlimit 100;') at line 13, column 46.
  Expected one of:
          * LPAR
          * IDENTIFIER
  Previous tokens: [Token('ROLLUP', 'rollup')]

  Location:
  ...es.ext_sales_price) by rollup ??? @channel_label(all_sales.sales...

  Write stats: received 1297 chars / 1297 bytes; tail: …'\\norder by channel
  nulls first, outlet nulls first\\nlimit 100;'.
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
- `trilogy run query01.preql`

  ```text
  Multiple where clauses are not supported
  ```
- `trilogy run query01.preql`

  ```text
  Cannot compare BOOL (ref:local.qualifies) and INTEGER (0) of
  different types with operator > in ref:local.qualifies > 0
  ```
- `trilogy run query01.preql`

  ```text
  HAVING references 'r.billing_customer.id', which is not in
  the SELECT projection (line 9). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --r.billing_customer.id
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query01.preql`

  ```text
  HAVING references 'local.qual_count', which is not in the
  SELECT projection (line 12). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.qual_count
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query02.preql`

  ```text
  Unable to import '.\all_sales.preql': [Errno 2] No such file
  or directory: '.\\all_sales.preql'. Did you mean: raw.all_sales?
  ```
- `trilogy run query04.preql`

  ```text
  HAVING references 'local.store_2001', 'local.catalog_2001',
  'local.web_2001', 'local.catalog_ratio', 'local.store_ratio',
  'local.web_ratio', which are not in the SELECT projection (line 27). Add them
  to SELECT, each prefixed with `--` so they stay out of the output rows — keep
  your HAVING as-is:
      select <your existing columns>, --local.store_2001, --local.catalog_2001,
  --local.web_2001, --local.catalog_ratio, --local.store_ratio, --local.web_ratio
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run - --import raw.all_sales:all_sales`

  ```text
  Received invalid type <class
  'trilogy.core.models.author.Between'> ref:all_sales.return_date.date between
  constant(2000-08-23) and constant(2000-09-06) as input to concept derivation:
  `auto return_date_in_range <- all_sales.return_date.date between
  '2000-08-23'::date and '2000-09-06'::date`
  ```
- `trilogy run query05.preql`

  ```text
  (_duckdb.BinderException) Binder Error: column
  "all_sales_channel_dim_id" must appear in the GROUP BY clause or must be part
  of an aggregate function.
  Either add it to the GROUP BY list, or use
  "ANY_VALUE(all_sales_channel_dim_id)" if the exact value of
  "all_sales_channel_dim_id" is not important.

  LINE 179:     "scrawny"."all_sales_channel_dim_id" as "all_sales_channel_...
                ^
  [SQL:
  WITH
  vacuous as (
  SELECT
      "all_sales_catalog_sales_unified"."CS_CATALOG_PAGE_SK" as
  "all_sales_channel_dim_id",
      "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "all_sales_date_id"
  …
  as "outlet",
      "friendly"."total_sales" as "total_sales",
      "friendly"."total_returns" as "total_returns",
      "friendly"."_virt_agg_sum_8391465486749127" -
  "late"."_virt_agg_sum_8956394585779731" as "total_profit"
  FROM
      "friendly"
      FULL JOIN "late" on "friendly"."all_sales_channel_dim_id" is not distinct
  from "late"."all_sales_channel_dim_id" AND "friendly"."all_sales_sales_channel"
  = "late"."all_sales_sales_channel"
  ORDER BY
      "friendly"."channel" asc nulls first,
      "friendly"."outlet" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: Cannot compare values
  of type VARCHAR and VARCHAR[] in IN/ANY/ALL clause - an explicit cast is
  required

  LINE 15: ..._sales_billing_customer_address_customer_address"."CA_ZIP" in
  (select abundant."zips_list" from abundant where abundant...
                                                                         ^
  [SQL:
  WITH
  abundant as (
  SELECT
      STRING_SPLIT( $1 , ',' ) as "zips_list"
  ),
  wakeful as (
  SELECT
      "physical_sales_billing_customer_address_customer_address"."CA_ZIP" as
  "physical_sales_billing_customer_address_zip",
      count("physical_
  …
  and
  SUBSTRING("physical_sales_store_store"."S_ZIP",1,2) in (select
  uneven."_virt_func_substring_9221055437369198" from uneven where
  uneven."_virt_func_substring_9221055437369198" is not null)

  GROUP BY
      1,
      2,
      "physical_sales_store_sales"."SS_ITEM_SK",
      "physical_sales_store_sales"."SS_TICKET_NUMBER")
  SELECT
      "sparkling"."physical_sales_store_name" as "store_name",
      sum("sparkling"."physical_sales_net_profit") as "total_net_profit"
  FROM
      "sparkling"
  GROUP BY
      1
  ORDER BY
      "store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.all_sales:sales select count(sales.ext_sales_price ? sales.sales_channel in ('WEB','CATALOG'));`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(sales.ext_sales_price ?
  sales.sales_channel in ('WEB','CATALOG')) as catalog_count`
  Location:
  ..._channel in ('WEB','CATALOG')) ??? ;
  ```

### `undefined-concept`

- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  first_name.')
  ```

### `join-resolution`

- `trilogy run query10.preql`

  ```text
  Could not resolve connections for query with output
  ['local._virt_filter_id_1661235927883585<Purpose.PROPERTY>Derivation.FILTER>']
  from current model. The output draws on models that are not connected in the
  current graph: catalog_sales (needed by
  local._virt_filter_id_1661235927883585); customer (needed by
  local._virt_filter_id_1661235927883585); store_sales (needed by
  local._virt_filter_id_1661235927883585); web_sales (needed by
  local._virt_filter_id_1661235927883585). If these should be related, bridge
  their keys with a merge, e.g. `merge catalog_sales.<key> into
  ~customer.<key>;`.
  ```
