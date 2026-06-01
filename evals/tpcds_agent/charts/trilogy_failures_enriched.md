# Trilogy failure analysis — 20260601-143003

- Run `20260601-143003_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 249 | failed: 40 (16%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 24 | 60% |
| `other` | 5 | 12% |
| `undefined-concept` | 5 | 12% |
| `join-resolution` | 3 | 8% |
| `syntax-missing-alias` | 3 | 8% |

## Detail

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

where all_sales.sales_channel in ('WEB', 'CATALOG')
  and all_sales.date.year …ri_2002 / nullif(fri_2001, 0) as fri_ratio,
  sat_2002 / nullif(sat_2001, 0) as sat_ratio
order by all_sales.date.week_seq asc nulls first
limit 200;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 6, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...es.date.year in (2001, 2002)  ??? auto sun_2001 <- sum(all_sales...

  Write stats: received 2563 chars / 2563 bytes; tail: …'\\norder by
  all_sales.date.week_seq asc nulls first\\nlimit 200;'.
  ```
- `trilogy file write query04.preql --content import raw/all_sales as s;

# Per-line amount: ((ext_list_price - ext_wholesale_cost - ext_discount_amount) + ext_…t_flag

order by
  code asc nulls first,
  first_name asc nulls first,
  last_name asc nulls first,
  preferred_cust_flag asc nulls first

limit 100;`

  ```text
  …
  nwhere\n  store_2001 > 0\n  and catalog_2001 > 0\n
  and web_2001 > 0\n  and catalog_ratio > store_ratio\n  and catalog_ratio >
  web_ratio\n\nselect\n  s.billing_customer.text_id as code,\n
  s.billing_customer.first_name,\n  s.billing_customer.last_name,\n
  s.billing_customer.preferred_cust_flag\n\norder by\n  code asc nulls first,\n
  first_name asc nulls first,\n  last_name asc nulls first,\n
  preferred_cust_flag asc nulls first\n\nlimit 100;") at line 1, column 11.
  Expected one of:
          * IMPORT_DOT
          * "as"
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /all_sales as s;  # Per-line a...

  Write stats: received 1725 chars / 1727 bytes; tail: …'lls first,\\n
  preferred_cust_flag asc nulls first\\n\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date

a…es.return_net_loss) by rollup all_sales.sales_channel, channel_outlet as total_profit
order by coalesce(channel, ''), coalesce(outlet, '')
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 5, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...:date and '2000-09-06'::date  ??? auto channel_outlet <- case(

  Write stats: received 890 chars / 890 bytes; tail: …"er by coalesce(channel,
  ''), coalesce(outlet, '')\\nlimit 100;".
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

auto channel_outlet <- case(
    all_sales.sales_channel = 'STORE', concat('st…es.return_net_loss) by rollup all_sales.sales_channel, channel_outlet as total_profit
order by coalesce(channel, ''), coalesce(outlet, '')
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 7, column 2.
  Expected one of:
          * WHEN

  Location:
  ...l_sales.channel_dim_text_id) ) ??? ;  where all_sales.date.date b...

  Write stats: received 890 chars / 890 bytes; tail: …"er by coalesce(channel,
  ''), coalesce(outlet, '')\\nlimit 100;".
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

auto channel_outlet <- case(
    when all_sales.sales_channel = 'STORE' then c…es.return_net_loss) by rollup all_sales.sales_channel, channel_outlet as total_profit
order by coalesce(channel, ''), coalesce(outlet, '')
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'all_sales.sales_channel ') at line
  4, column 10.
  Expected one of:
          * COMMA
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'when')]

  Location:
  ...nnel_outlet <- case(     when ??? all_sales.sales_channel = 'STO...

  Write stats: received 915 chars / 915 bytes; tail: …"er by coalesce(channel,
  ''), coalesce(outlet, '')\\nlimit 100;".
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

auto channel_outlet <- case(
    all_sales.sales_channel = 'STORE', concat('st…es.return_net_loss) by rollup all_sales.sales_channel, channel_outlet as total_profit
order by coalesce(channel, ''), coalesce(outlet, '')
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 7, column 2.
  Expected one of:
          * WHEN

  Location:
  ...l_sales.channel_dim_text_id) ) ??? ;  where all_sales.date.date b...

  Write stats: received 890 chars / 890 bytes; tail: …"er by coalesce(channel,
  ''), coalesce(outlet, '')\\nlimit 100;".
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date

d…,
    @rollup_profit(all_sales.net_profit, all_sales.return_net_loss) as total_profit
order by coalesce(channel, ''), coalesce(outlet, '')
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'def ') at line 5, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...:date and '2000-09-06'::date  ??? def outlet(sales_channel, dim_...

  Write stats: received 1233 chars / 1233 bytes; tail: …"er by coalesce(channel,
  ''), coalesce(outlet, '')\\nlimit 100;".
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

def outlet(sales_channel, dim_text_id) -> concat(
    case
        when sales_…,
    @rollup_profit(all_sales.net_profit, all_sales.return_net_loss) as total_profit
order by coalesce(channel, ''), coalesce(outlet, '')
limit 100;`

  ```text
  …
  l_sales.date.date between
  '2000-08-23'::date and '2000-09-06'::date\n\nselect\n
  all_sales.sales_channel as channel,\n    outlet(all_sales.sales_channel,
  all_sales.channel_dim_text_id) as outlet,\n
  @rollup_sales(all_sales.ext_sales_price) as total_sales,\n
  @rollup_returns(all_sales.return_amount) as total_returns,\n
  @rollup_profit(all_sales.net_profit, all_sales.return_net_loss) as
  total_profit\norder by coalesce(channel, ''), coalesce(outlet, '')\nlimit
  100;") at line 12, column 82.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('__ANON_11', ', outlet')]

  Location:
  ...ll_sales.sales_channel, outlet ??? (all_sales.sales_channel, all_...

  Write stats: received 1233 chars / 1233 bytes; tail: …"er by coalesce(channel,
  ''), coalesce(outlet, '')\\nlimit 100;".
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date

a…turns,
    sum(all_sales.net_profit) - sum(all_sales.return_net_loss) as total_profit
order by coalesce(channel, ''), coalesce(outlet, '')
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 5, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...:date and '2000-09-06'::date  ??? auto channel_outlet <- concat(...

  Write stats: received 714 chars / 714 bytes; tail: …"er by coalesce(channel,
  ''), coalesce(outlet, '')\\nlimit 100;".
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

auto channel_outlet <- concat(
    case
        when all_sales.sales_channel =…l,
    channel_outlet as outlet,
    total_sales,
    total_returns,
    total_profit
order by coalesce(channel, ''), coalesce(outlet, '')
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 14, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...:date and '2000-09-06'::date  ??? auto total_sales <- sum(all_sa...

  Write stats: received 922 chars / 922 bytes; tail: …"er by coalesce(channel,
  ''), coalesce(outlet, '')\\nlimit 100;".
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

auto channel_outlet <- concat(
    case
        when all_sales.sales_channel =…et,
    total_sales,
    total_returns,
    total_profit,
    g_channel,
    g_outlet
order by coalesce(channel, ''), coalesce(outlet, '')
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 18, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...:date and '2000-09-06'::date  ??? auto g_channel <- grouping(all...

  Write stats: received 1148 chars / 1148 bytes; tail: …"er by coalesce(channel,
  ''), coalesce(outlet, '')\\nlimit 100;".
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

def channel_outlet -> concat(
    case
        when all_sales.sales_channel = …@rollup_total_profit(all_sales.net_profit, all_sales.return_net_loss) as total_profit
order by coalesce(channel, ''), coalesce(outlet, '')
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_BINDING', '->') at line 3, column 20.
  Expected one of:
          * LPAR
  Previous tokens: [Token('IDENTIFIER', 'channel_outlet')]

  Location:
  ...ll_sales;  def channel_outlet ??? -> concat(     case         wh...

  Write stats: received 1113 chars / 1113 bytes; tail: …"er by coalesce(channel,
  ''), coalesce(outlet, '')\\nlimit 100;".
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

def channel_outlet() -> concat(
    case
        when all_sales.sales_channel …@rollup_total_profit(all_sales.net_profit, all_sales.return_net_loss) as total_profit
order by coalesce(channel, ''), coalesce(outlet, '')
limit 100;`

  ```text
  …
  sales_channel,
  channel_outlet();\n\nwhere all_sales.date.date between '2000-08-23'::date and
  '2000-09-06'::date\n\nselect\n    all_sales.sales_channel as channel,\n
  channel_outlet() as outlet,\n    @rollup_total_sales(all_sales.ext_sales_price)
  as total_sales,\n    @rollup_total_returns(all_sales.return_amount) as
  total_returns,\n    @rollup_total_profit(all_sales.net_profit,
  all_sales.return_net_loss) as total_profit\norder by coalesce(channel, ''),
  coalesce(outlet, '')\nlimit 100;") at line 3, column 20.
  Expected one of:
          * IDENTIFIER
  Previous tokens: [Token('LPAR', '(')]

  Location:
  ...ll_sales;  def channel_outlet( ??? ) -> concat(     case

  Write stats: received 1115 chars / 1115 bytes; tail: …"er by coalesce(channel,
  ''), coalesce(outlet, '')\\nlimit 100;".
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

def channel_outlet(sales_channel, dim_text_id) -> concat(
    case
        whe…@rollup_total_profit(all_sales.net_profit, all_sales.return_net_loss) as total_profit
order by coalesce(channel, ''), coalesce(outlet, '')
limit 100;`

  ```text
  …
  -08-23'::date and '2000-09-06'::date\n\nselect\n
  all_sales.sales_channel as channel,\n
  channel_outlet(all_sales.sales_channel, all_sales.channel_dim_text_id) as
  outlet,\n    @rollup_total_sales(all_sales.ext_sales_price) as total_sales,\n
  @rollup_total_returns(all_sales.return_amount) as total_returns,\n
  @rollup_total_profit(all_sales.net_profit, all_sales.return_net_loss) as
  total_profit\norder by coalesce(channel, ''), coalesce(outlet, '')\nlimit
  100;") at line 12, column 96.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('__ANON_11', ', channel_outlet')]

  Location:
  ....sales_channel, channel_outlet ??? (all_sales.sales_channel, all_...

  Write stats: received 1309 chars / 1309 bytes; tail: …"er by coalesce(channel,
  ''), coalesce(outlet, '')\\nlimit 100;".
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

auto channel_outlet <- concat(
    case
        when all_sales.sales_channel =…ales as total_sales,
    @rl_returns as total_returns,
    @rl_profit as total_profit
order by coalesce(channel, ''), coalesce(outlet, '')
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_BINDING', '->') at line 12, column 14.
  Expected one of:
          * LPAR
  Previous tokens: [Token('IDENTIFIER', 'rl_sales')]

  Location:
  ..._dim_text_id );  def rl_sales ??? -> sum(all_sales.ext_sales_pri...

  Write stats: received 1149 chars / 1149 bytes; tail: …"er by coalesce(channel,
  ''), coalesce(outlet, '')\\nlimit 100;".
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

auto channel_outlet <- concat(
    case
        when all_sales.sales_channel =…urns,
    @rl_profit(all_sales.net_profit, all_sales.return_net_loss) as total_profit
order by coalesce(channel, ''), coalesce(outlet, '')
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', ") ->
  grouping(all_sales.sales_channel) by rollup all_sales.sales_channel,
  channel_outlet;\ndef rl_g_o() -> grouping(channel_outlet) by rollup
  all_sales.sales_channel, channel_outlet;\n\nwhere all_sales.date.date between
  '2000-08-23'::date and '2000-09-06'::date\n\nselect\n
  all_sales.sales_channel as channel,\n    channel_outlet as outlet,\n
  @rl_sales(all_sales.ext_sales_price) as total_sales,\n
  @rl_returns(all_sales.return_amount) as total_returns,\n
  @rl_profit(all_sales.net_profit, all_sales.return_net_loss) as
  total_profit\norder by coalesce(channel, ''), coalesce(outlet, '')\nlimit
  100;") at line 15, column 12.
  Expected one of:
          * IDENTIFIER
  Previous tokens: [Token('LPAR', '(')]

  Location:
  ...l, channel_outlet; def rl_g_c( ??? ) -> grouping(all_sales.sales_...

  Write stats: received 1245 chars / 1245 bytes; tail: …"er by coalesce(channel,
  ''), coalesce(outlet, '')\\nlimit 100;".
  ```
- `trilogy file write query08.preql --content parameter zips string;

# Preferred customers with their home address ZIP codes
auto preferred_cust_zip_count <- c…10
      group by first2
    )
    group by first2
  )

select
    store.name,
    sum(net_profit) as total_net_profit
order by store.name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_SUBSTRING', 'substring(') at line 16, column 7.
  Expected one of:
          * RPAR
          * COMMA
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
  ...mer ZIP set     select        ??? substring(value, 1, 2) as firs...

  Write stats: received 1055 chars / 1055 bytes; tail: …'t_profit) as
  total_net_profit\\norder by store.name\\nlimit 100;'.
  ```
- `trilogy file write query08.preql --content parameter zips string;

# Preferred customers count by ZIP prefix (first 2 digits)
auto pref_cust_zip_prefix_count…string(value, 1, 2)) from unnest(split(zips, ','))
  )

select
    store.name,
    sum(net_profit) as total_net_profit
order by store.name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 7, column 60.
  Expected one of:
          * _TERMINATOR

  Location:
  ...<- pref_cust_zip_prefix_count ??? where pref_cust_zip_prefix_cou...

  Write stats: received 821 chars / 821 bytes; tail: …'t_profit) as
  total_net_profit\\norder by store.name\\nlimit 100;'.
  ```
- `trilogy file write query08.preql --content parameter zips string;

# Preferred customers count by address ZIP (not prefix)
auto pref_cust_by_zip <- count(bil…(substring(value, 1, 2)) from unnest(split(zips, ','))

select
    store.name,
    sum(net_profit) as total_net_profit
order by store.name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 10, column 87.
  Expected one of:
          * SELECT

  Location:
  ..._customer.address.zip, 1, 2)) ??? where billing_customer.preferr...

  Write stats: received 830 chars / 830 bytes; tail: …'t_profit) as
  total_net_profit\\norder by store.name\\nlimit 100;'.
  ```
- `trilogy file write query08.preql --content parameter zips string;

# Preferred customers count by ZIP prefix
# First, define count of preferred customers by …bstring(value, 1, 2) from unnest(split(zips, ','))
  )

select
    store.name,
    sum(net_profit) as total_net_profit
order by store.name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...ubstring(store.zip, 1, 2)) in ??? (     select substring(value,

  Write stats: received 599 chars / 599 bytes; tail: …'t_profit) as
  total_net_profit\\norder by store.name\\nlimit 100;'.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as sales;

parameter zips string;

# Extract first 2 digits of parameter ZIPs as array
a….store.zip, 1, 2)) in param_prefixes

select
    sales.store.name,
    sum(sales.net_profit) as total_net_profit
order by sales.store.name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'x ') at line 6, column 58.
  Expected one of:
          * "@"
  Previous tokens: [Token('COMMA', ',')]

  Location:
  ...y_transform(split(zips, ','), ??? x -> substring(x, 1, 2));  whe...

  Write stats: received 676 chars / 676 bytes; tail: …'it) as
  total_net_profit\\norder by sales.store.name\\nlimit 100;'.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as sales;

parameter zips string;

# Extract first 2 digits of parameter ZIPs as array
a….store.zip, 1, 2)) in param_prefixes

select
    sales.store.name,
    sum(sales.net_profit) as total_net_profit
order by sales.store.name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', "(x) -> substring(x, 1,
  2));\n\nwhere sales.date.year = 1998\n  and sales.date.quarter = 2\n  and
  (substring(sales.store.zip, 1, 2)) in
  (substring(sales.billing_customer.address.zip, 1, 2))\n  and
  count(sales.billing_customer.id ? sales.billing_customer.preferred_cust_flag =
  'Y') by (substring(sales.billing_customer.address.zip, 1, 2)) > 10\n  and
  (substring(sales.store.zip, 1, 2)) in param_prefixes\n\nselect\n
  sales.store.name,\n    sum(sales.net_profit) as total_net_profit\norder by
  sales.store.name\nlimit 100;") at line 6, column 58.
  Expected one of:
          * "@"
  Previous tokens: [Token('COMMA', ',')]

  Location:
  ...y_transform(split(zips, ','), ??? (x) -> substring(x, 1, 2));  w...

  Write stats: received 678 chars / 678 bytes; tail: …'it) as
  total_net_profit\\norder by sales.store.name\\nlimit 100;'.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as sales;

parameter zips string;

# Preferred customers count by ZIP prefix
# condition…ng_customer.address.zip, 1, 2)) > 10

select
    sales.store.name,
    sum(sales.net_profit) as total_net_profit
order by sales.store.name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 9, column 3.
  Expected one of:
          * _TERMINATOR

  Location:
  ...stomer.address.zip, 1, 2))    ??? where count(sales.billing_cust...

  Write stats: received 838 chars / 838 bytes; tail: …'it) as
  total_net_profit\\norder by sales.store.name\\nlimit 100;'.
  ```
- `trilogy file write query09.preql --content import raw.physical_sales as ss;

select
    case(
        count(ss.ticket_number ? ss.quantity between 1 and 20) …g(ss.ext_discount_amount ? ss.quantity between 81 and 100),
        avg(ss.net_paid ? ss.quantity between 81 and 100)
    ) as bucket_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 8, column 7.
  Expected one of:
          * WHEN

  Location:
  ...ntity between 1 and 20)     ) ??? as bucket_1_20,     case(

  Write stats: received 1210 chars / 1210 bytes; tail: …'quantity between 81 and
  100)\\n    ) as bucket_81_100\\nlimit 1;'.
  ```

### `other`

- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 44 column 12 (char 2992). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query02.preql`

  ```text
  HAVING references 'all_sales.date.year', which is not in the
  SELECT projection (line 32). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --all_sales.date.year
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query08.preql --param zips=24128`

  ```text
  (_duckdb.BinderException) Binder Error: Table
  "sales_store_store" does not have a column named "S_CLOSED_DATE"

  Candidate bindings: : "s_closed_date_sk"

  LINE 19:     INNER JOIN "date_dim" as "sales_store_date_date" on
  "sales_store_store"."S_CLOSED_DATE" = "sales_store_date_date...
                                                                   ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "sales_store_sales"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
      "sales_store_sales"."SS_STORE_SK" as "sales_store_id"
  FROM
      "store_sales" as "sales_store_sales"
  GROUP BY
      1,
      2),
  thoughtful as
  …
  akeful"."sales_store_id" =
  "sales_store_store"."S_STORE_SK"
      INNER JOIN "date_dim" as "sales_store_date_date" on
  "sales_store_store"."S_CLOSED_DATE" = "sales_store_date_date"."D_DATE_SK"
  WHERE
      "sales_store_date_date"."D_YEAR" = 1998 and "sales_store_date_date"."D_QOY"
  = 2

  GROUP BY
      1,
      2)
  SELECT
      "thoughtful"."sales_store_name" as "sales_store_name",
      count("thoughtful"."sales_billing_customer_id") as "cust_count"
  FROM
      "thoughtful"
  GROUP BY
      1
  ORDER BY
      "thoughtful"."sales_store_name" asc
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: Cannot compare values
  of type VARCHAR and VARCHAR[] in IN/ANY/ALL clause - an explicit cast is
  required

  LINE 42: ...er_address"."CA_ZIP",1,2)) and "sales_store_store"."S_ZIP" in
  (select quizzical."_virt_func_split_4785012549328100...
                                                                         ^
  [SQL:
  WITH
  quizzical as (
  SELECT
      STRING_SPLIT( $1 , ',' ) as "_virt_func_split_4785012549328100"
  ),
  wakeful as (
  SELECT
      "sales_billing_customer_address_customer_address"."CA_ZIP" as
  "sales_billing_customer_address_zip"
  FROM
      "customer_
  …
  s_net_profit" as "sales_net_profit",
      "yummy"."sales_store_name" as "sales_store_name"
  FROM
      "yummy"
      INNER JOIN "thoughtful" on "yummy"."_virt_func_substring_9224200219381508"
  = "thoughtful"."_virt_func_substring_9224200219381508"
  GROUP BY
      1,
      2,
      "yummy"."sales_item_id",
      "yummy"."sales_ticket_number")
  SELECT
      "juicy"."sales_store_name" as "sales_store_name",
      sum("juicy"."sales_net_profit") as "total_net_profit"
  FROM
      "juicy"
  GROUP BY
      1
  ORDER BY
      "juicy"."sales_store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816`

  ```text
  (_duckdb.BinderException) Binder Error: Referenced table
  "quizzical" not found!
  Candidate tables: "sales_billing_customer_customers"

  LINE 42: ... ( SUBSTRING("sales_store_store"."S_ZIP",1,2) ) in
  (SUBSTRING("quizzical"."param_zips",1,2)) and ( SUBSTRING("sales_store...
                                                                            ^
  [SQL:
  WITH
  quizzical as (
  SELECT
      unnest(STRING_SPLIT( $1 , ',' )) as "param_zips"
  ),
  wakeful as (
  SELECT
      "sales_billing_customer_address_customer_address"."CA_ZIP" as
  "sales_billing_customer_address_zip"
  FROM
      "customer_address" as "sal
  …
  s_net_profit" as "sales_net_profit",
      "yummy"."sales_store_name" as "sales_store_name"
  FROM
      "yummy"
      INNER JOIN "thoughtful" on "yummy"."_virt_func_substring_9224200219381508"
  = "thoughtful"."_virt_func_substring_9224200219381508"
  GROUP BY
      1,
      2,
      "yummy"."sales_item_id",
      "yummy"."sales_ticket_number")
  SELECT
      "juicy"."sales_store_name" as "sales_store_name",
      sum("juicy"."sales_net_profit") as "total_net_profit"
  FROM
      "juicy"
  GROUP BY
      1
  ORDER BY
      "juicy"."sales_store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```

### `undefined-concept`

- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  first_name.')
  ```
- `trilogy run query07.preql`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  item.text_id. Suggestions: ['store_sales.item.text_id',
  'store_sales.time.text_id']")
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  billing_customer.id.')
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  billing_customer.id.')
  ```
- `trilogy run query08.preql --param zips=24128`

  ```text
  (UndefinedConceptException(...), 'line: 3: Undefined concept:
  store.name.')
  ```

### `join-resolution`

- `trilogy run query02.preql`

  ```text
  Could not resolve connections for query with output
  ['date_dim.week_seq<Purpose.PROPERTY>Derivation.ROOT>',
  'local.sun_ratio<Purpose.PROPERTY>Derivation.BASIC>',
  'local.mon_ratio<Purpose.PROPERTY>Derivation.BASIC>',
  'local.tue_ratio<Purpose.PROPERTY>Derivation.BASIC>',
  'local.wed_ratio<Purpose.PROPERTY>Derivation.BASIC>',
  'local.thu_ratio<Purpose.PROPERTY>Derivation.BASIC>',
  'local.fri_ratio<Purpose.PROPERTY>Derivation.BASIC>',
  'local.sat_ratio<Purpose.PROPERTY>Derivation.BASIC>'] from current model. The
  output draws on models that are not connected in the current graph: all_sales
  (needed by local.fri_ratio, local.mon_ratio, local.sat_ratio, local.sun_ratio,
  local.thu_ratio, local.tue_ratio, local.wed_ratio); date_dim (needed by
  date_dim.week_seq). If these should be related, bridge their keys with a merge,
  e.g. `merge all_sales.<key> into ~date_dim.<key>;`.
  ```
- `trilogy run query06.preql`

  ```text
  Could not resolve connections for query with output
  ['local.state<Purpose.PROPERTY>Derivation.BASIC>',
  'local.qualifying_line_items<Purpose.METRIC>Derivation.AGGREGATE>'] from
  current model.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084`

  ```text
  Could not resolve connections for query with output
  ['sales.store.name<Purpose.PROPERTY>Derivation.ROOT>',
  'local.total_net_profit<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```

### `syntax-missing-alias`

- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Get data for both years, no filtering yet
auto sales_sun <- sum(all_sales.ex…l_sales.date.week_seq,
  (sales_sun ? all_sales.date.year = 2002) by (all_sales.date.week_seq + 53) /
    nullif(sales_sun, 0) as sun_ratio
limit 10;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `(sales_sun ? all_sales.date.year = 2002) by
  (all_sales.date.week_seq + 53) /
      nullif(sales_sun, 0) as sales_sun_all_sales_date_year_2002_by_al`
  Location:
  ...all_sales.date.year = 2002) by ??? (all_sales.date.week_seq + 53...

  Write stats: received 1541 chars / 1541 bytes; tail: …'_seq + 53) /\\n
  nullif(sales_sun, 0) as sun_ratio\\nlimit 10;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date

s…'web_site_'
        end,
        all_sales.channel_dim_text_id
    )) as total_profit
order by coalesce(channel, ''), coalesce(outlet, '')
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `(concat(
          case
              when all_sales.sales_channel = 'STORE' then 'store_'
              when all_sales.sales_channel = 'CATALOG' then 'catalog_page_'
              when all_sales.sales_channel = 'WEB' then 'web_site_'
          end,
          all_sales.channel_dim_text_id
      )) as concat_case_when_all_sales_sales_channel`
  Location:
  ...ollup all_sales.sales_channel, ??? (concat(         case

  Write stats: received 1662 chars / 1662 bytes; tail: …"er by coalesce(channel,
  ''), coalesce(outlet, '')\\nlimit 100;".
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

auto channel_outlet <- concat(
    case
        when all_sales.sales_channel =…s.return_net_loss)) by rollup all_sales.sales_channel, channel_outlet as total_profit
order by coalesce(channel, ''), coalesce(outlet, '')
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `coalesce(sum(all_sales.net_profit) -
  sum(all_sales.return_net_loss), sum(all_sales.net_profit) -
  sum(all_sales.return_net_loss)) by rollup all_sales.sales_channel as
  coalesce_sum_all_sales_net_profit_sum_al`
  Location:
  ...all_sales.return_net_loss)) by ??? rollup all_sales.sales_channe...

  Write stats: received 934 chars / 934 bytes; tail: …"er by coalesce(channel,
  ''), coalesce(outlet, '')\\nlimit 100;".
  ```
