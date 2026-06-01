# Trilogy failure analysis — 20260531-194745

- Run `20260531-194745_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 194 | failed: 50 (26%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 32 | 64% |
| `other` | 10 | 20% |
| `syntax-missing-alias` | 8 | 16% |

## Detail

### `syntax-parse`

- `trilogy file write query70.preql --content import raw.physical_sales as sales;

# Filter to year 2000
where sales.date.year = 2000

# Define the multi-level … g_county,
    g_state + g_county as level  # 0=state+county, 1=state subtotal, 2=grand total
order by
    level desc,
    net_profit desc
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'def') at line 7, column 1.
  Expected one of:
          * SELECT

  Location:
  ...-level aggregate using ROLLUP ??? def profit_by_geo(metric) -> s...

  Write stats: received 662 chars / 662 bytes; tail: …'otal\\norder by\\n
  level desc,\\n    net_profit desc\\nlimit 100;'.
  ```
- `trilogy file write query70.preql --content import raw.physical_sales as sales;

# Filter to year 2000
where sales.date.year = 2000

# Compute total net profi…store.state order by net_profit desc)
    end as within_parent_rank
order by
    level desc,
    sales.store.state,
    within_parent_rank
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 7, column 1.
  Expected one of:
          * SELECT

  Location:
  ...for identifying top 5 states) ??? auto state_profit <- sum(sales...

  Write stats: received 1198 chars / 1198 bytes; tail: …'sc,\\n
  sales.store.state,\\n    within_parent_rank\\nlimit 100;'.
  ```
- `trilogy file write query70.preql --content import raw.physical_sales as sales;

# Compute total net profit by state (for identifying top 5 states)
auto state… order by net_profit desc) as rank_by_level
order by
    level desc,
    case when level = 0 then sales.store.state end,
    rank_by_level
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHEN', 'when') at line 23, column 10.
  Expected one of:
          * MERGE
          * LIMIT
          * _TERMINATOR

  Location:
  ...r by     level desc,     case ??? when level = 0 then sales.stor...

  Write stats: received 994 chars / 994 bytes; tail: …'= 0 then sales.store.state
  end,\\n    rank_by_level\\nlimit 100;'.
  ```
- `trilogy file write query70.preql --content import raw.physical_sales as sales;

# Compute total net profit by state (for identifying top 5 states)
auto state…oalesce(sales.store.county, '') as county_coalesced
order by
    g_state + g_county desc,
    sales.store.state,
    --net_profit_raw desc
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', "--net_profit_raw,\n
  grouping(sales.store.state) by rollup sales.store.state, sales.store.county as
  g_state,\n    grouping(sales.store.county) by rollup sales.store.state,
  sales.store.county as g_county,\n    --g_state + g_county as level,\n
  coalesce(sales.store.state, '') as state_coalesced,\n
  coalesce(sales.store.county, '') as county_coalesced\norder by\n    g_state +
  g_county desc,\n    sales.store.state,\n    --net_profit_raw desc\nlimit 100;")
  at line 17, column 78.
  Expected one of:
          * IDENTIFIER
  Previous tokens: [Token('AS', 'as')]

  Location:
  ....state, sales.store.county as ??? --net_profit_raw,     grouping...

  Write stats: received 964 chars / 964 bytes; tail: …'\\n
  sales.store.state,\\n    --net_profit_raw desc\\nlimit 100;'.
  ```
- `trilogy file write query70.preql --content import raw.physical_sales as sales;

# Compute total net profit by state (for identifying top 5 states)
auto state…esce(sales.store.state, ''), net_profit desc) as within_parent_rank
order by
    level desc,
    sales.store.state,
    within_parent_rank
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens — write `by
  (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any
  function call, cast, or other expression needs them.
  Location:
  ...les.store.county) over (order ??? by g_state + g_county desc, co...

  Write stats: received 991 chars / 991 bytes; tail: …'sc,\\n
  sales.store.state,\\n    within_parent_rank\\nlimit 100;'.
  ```
- `trilogy file write query70.preql --content import raw.physical_sales as sales;

# Compute total net profit by state (for identifying top 5 states)
auto state…re.state) over (order by total_by_state desc)
where sales.store.state is not null
union all
select
    null,
    null,
    grand_total,
    2,
    1;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 14, column 1.
  Expected one of:
          * SELECT

  Location:
  ...evel 0: state + county detail ??? auto county_profit <- sum(sale...

  Write stats: received 1218 chars / 1218 bytes; tail: …'ll\\nselect\\n
  null,\\n    null,\\n    grand_total,\\n    2,\\n    1;'.
  ```
- `trilogy file write query70.preql --content import raw.physical_sales as sales;

# Compute total net profit by state (for identifying top 5 states)
auto state…  0 as level
where sales.store.state is not null and sales.store.county is not null
order by
    sales.store.state,
    detail_profit desc
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 14, column 1.
  Expected one of:
          * SELECT

  Location:
  ...ggregates at different grains ??? auto county_agg <- sum(sales.n...

  Write stats: received 1001 chars / 1001 bytes; tail: …' by\\n
  sales.store.state,\\n    detail_profit desc\\nlimit 100;'.
  ```
- `trilogy file write query77.preql --content import all_sales as s;

# Map channel labels
auto s_chan_label <- case(s.sales_channel, 'STORE', 'store channel', …m_id as g_outlet,
    g_chan + g_outlet as level
order by
    g_chan asc nulls first,
    g_outlet asc nulls first,
    total_returns desc
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 4, column 121.
  Expected one of:
          * WHEN

  Location:
  ...hannel', 'WEB', 'web channel') ??? ;  where s.date.date between '...

  Write stats: received 1371 chars / 1371 bytes; tail: …' g_outlet asc nulls
  first,\\n    total_returns desc\\nlimit 100;'.
  ```
- `trilogy file write query77.preql --content import all_sales as s;

auto chan_label <- simple_case(s.sales_channel, 'STORE', 'store channel', 'CATALOG', 'cata…m_id as g_outlet,
    g_chan + g_outlet as level
order by
    g_chan asc nulls first,
    g_outlet asc nulls first,
    total_returns desc
limit 100;`

  ```text
  …
  _date.date between '2000-08-23'::date and '2000-09-22'::date) by
  rollup chan_label, s.channel_dim_id as total_return_loss,\n
  total_sales_profit - total_return_loss as profit,\n    grouping(chan_label) by
  rollup chan_label, s.channel_dim_id as g_chan,\n    grouping(s.channel_dim_id)
  by rollup chan_label, s.channel_dim_id as g_outlet,\n    g_chan + g_outlet as
  level\norder by\n    g_chan asc nulls first,\n    g_outlet asc nulls first,\n
  total_returns desc\nlimit 100;") at line 3, column 31.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'simple_case')]

  Location:
  ...auto chan_label <- simple_case ??? (s.sales_channel, 'STORE', 'st...

  Write stats: received 1337 chars / 1337 bytes; tail: …' g_outlet asc nulls
  first,\\n    total_returns desc\\nlimit 100;'.
  ```
- `trilogy file write query77.preql --content import all_sales as s;

def rollup_sales(metric) -> sum(metric) by rollup simple_case(s.sales_channel, 'STORE', 's… @rollup_sales(coalesce(s.return_net_loss, 0) ? s.return_date.date between '2000-08-23'::date and '2000-09-22'::date) as total_return_loss
limit 100;`

  ```text
  …
   '2000-08-23'::date and '2000-09-22'::date) as total_sales,\n
  @rollup_sales(coalesce(s.return_amount, 0) ? s.return_date.date between
  '2000-08-23'::date and '2000-09-22'::date) as total_returns,\n
  @rollup_sales(s.net_profit ? s.date.date between '2000-08-23'::date and
  '2000-09-22'::date) as total_sales_profit,\n
  @rollup_sales(coalesce(s.return_net_loss, 0) ? s.return_date.date between
  '2000-08-23'::date and '2000-09-22'::date) as total_return_loss\nlimit 100;")
  at line 3, column 62.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'simple_case')]

  Location:
  ...(metric) by rollup simple_case ??? (s.sales_channel, 'STORE', 'st...

  Write stats: received 1032 chars / 1032 bytes; tail: …"date and
  '2000-09-22'::date) as total_return_loss\\nlimit 100;".
  ```
- `trilogy file write query77.preql --content import all_sales as s;

where s.date.date between '2000-08-23'::date and '2000-09-22'::date
   or s.return_date.da…profit,
    sum(coalesce(s.return_net_loss, 0) ? s.return_date.date between '2000-08-23'::date and '2000-09-22'::date) as total_return_loss
limit 100`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a
  semicolon `;`.
  Location:
  ...as total_return_loss limit 100 ???

  Write stats: received 723 chars / 723 bytes; tail: …":date and
  '2000-09-22'::date) as total_return_loss\\nlimit 100".
  ```
- `trilogy run --import raw.all_sales:s select simple_case(s.sales_channel, 'STORE', 'store channel', 'CATALOG', 'catalog channel', 'WEB', 'web channel') as channel, s.channel_dim_id as outlet, sum(s.ext_sales_price) as total_sales where s.date.date between '2000-08-23'::date and '2000-09-22'::date limit 10;`

  ```text
  --> 2:19
    |
  2 | select simple_case(s.sales_channel, 'STORE', 'store channel', 'CATALOG',
  'catalog channel', 'WEB', 'web channel') as channel, s.channel_dim_id as
  outlet, sum(s.ext_sales_price) as total_sales where s.date.date between
  '2000-08-23'::date and '2000-09-22'::date limit 10;
    |                   ^---
    |
    = expected limit, order_by, where, having, dot_tail, bracket_tail,
  dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...sales as s; select simple_case ??? (s.sales_channel, 'STORE', 'st...
  ```
- `trilogy run --import raw.all_sales:s select case(s.sales_channel, 'STORE', 'store channel', 'CATALOG', 'catalog channel', 'WEB', 'web channel') as channel, s.channel_dim_id as outlet, sum(s.ext_sales_price) as total_sales where s.date.date between '2000-08-23'::date and '2000-09-22'::date limit 10;`

  ```text
  --> 2:108
    |
  2 | select case(s.sales_channel, 'STORE', 'store channel', 'CATALOG', 'catalog
  channel', 'WEB', 'web channel') as channel, s.channel_dim_id as outlet,
  sum(s.ext_sales_price) as total_sales where s.date.date between
  '2000-08-23'::date and '2000-09-22'::date limit 10;
    |
  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or fcase_simple_when
  Location:
  ...annel', 'WEB', 'web channel') ??? as channel, s.channel_dim_id a...
  ```
- `trilogy file write query77.preql --content import raw.all_sales as s;

where s.date.date between '2000-08-23'::date and '2000-09-22'::date
   or s.return_dat…rofit,
    coalesce(sum(s.return_net_loss ? s.return_date.date between '2000-08-23'::date and '2000-09-22'::date), 0) as total_return_loss
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 7, column 105.
  Expected one of:
          * WHEN

  Location:
  ...annel', 'WEB', 'web channel') ??? as channel,     s.channel_dim_...

  Write stats: received 812 chars / 812 bytes; tail: …" and '2000-09-22'::date),
  0) as total_return_loss\\nlimit 100;".
  ```
- `trilogy file write query77.preql --content import raw.all_sales as s;

def rup_sales(m) -> sum(m) by rollup s.sales_channel, s.channel_dim_id;
def rup_return…es_profit,
    @rup_returns(s.return_net_loss ? s.return_date.date between '2000-08-23'::date and '2000-09-22'::date) as total_return_loss
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 4, column 43.
  Expected one of:
          * _TERMINATOR

  Location:
  ...rns(m) -> coalesce(sum(m), 0) ??? by rollup s.sales_channel, s.c...

  Write stats: received 894 chars / 894 bytes; tail: …"date and
  '2000-09-22'::date) as total_return_loss\\nlimit 100;".
  ```
- `trilogy file write query77.preql --content import raw.all_sales as s;

# Define per-channel-outlet aggregates with rollup
auto total_sales <- sum(s.ext_sales…t,
    total_sales,
    total_returns,
    total_sales_profit,
    total_return_loss,
    total_sales_profit - total_return_loss as profit
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 5, column 128.
  Expected one of:
          * _TERMINATOR

  Location:
  ...e and '2000-09-22'::date), 0) ??? by rollup s.sales_channel, s.c...

  Write stats: received 1097 chars / 1097 bytes; tail: …' total_sales_profit -
  total_return_loss as profit\\nlimit 100;'.
  ```
- `trilogy run --import raw.all_sales:s select s.sales_channel, s.channel_dim_id, sum(s.ext_sales_price) as total_sales where s.date.date between '2000-08-23'::date and '2000-09-22'::date group by rollup s.sales_channel, s.channel_dim_id limit 10;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...::date and '2000-09-22'::date ??? group by rollup s.sales_channe...
  ```
- `trilogy file write query77.preql --content import raw.all_sales as s;

auto total_sales <- sum(s.ext_sales_price ? s.date.date between '2000-08-23'::date and…
    g_chan asc nulls first,
    g_outlet asc nulls first,
    channel asc nulls first,
    outlet asc nulls first,
    total_returns desc
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', "'STORE' then 'store channel',
  when 'CATALOG' then 'catalog channel', when 'WEB' then 'web channel') as
  channel,\n    s.channel_dim_id as outlet,\n    total_sales,\n
  coalesce(raw_returns, 0) as total_returns,\n    total_sales_profit,\n
  coalesce(raw_return_loss, 0) as total_return_loss,\n    total_sales_profit -
  coalesce(raw_return_loss, 0) as profit,\n    --g_chan,\n    --g_outlet\norder
  by\n    g_chan asc nulls first,\n    g_outlet asc nulls first,\n    channel asc
  nulls first,\n    outlet asc nulls first,\n    total_returns desc\nlimit 100;")
  at line 16, column 32.
  Expected one of:
          * RPAR
          * COMMA
  Previous tokens: [Token('IDENTIFIER', 'when')]

  Location:
     case(s.sales_channel, when ??? 'STORE' then 'store channel',

  Write stats: received 1606 chars / 1606 bytes; tail: …'   outlet asc nulls
  first,\\n    total_returns desc\\nlimit 100;'.
  ```
- `trilogy run --import raw.all_sales:s select simple_case(s.sales_channel, 'STORE', 'store channel') as chan where s.date.date between '2000-08-23'::date and '2000-09-22'::date limit 3;`

  ```text
  --> 2:19
    |
  2 | select simple_case(s.sales_channel, 'STORE', 'store channel') as chan where
  s.date.date between '2000-08-23'::date and '2000-09-22'::date limit 3;
    |                   ^---
    |
    = expected limit, order_by, where, having, dot_tail, bracket_tail,
  dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...sales as s; select simple_case ??? (s.sales_channel, 'STORE', 'st...
  ```
- `trilogy run --import raw.all_sales:s select case(s.sales_channel, when 'STORE' then 'store channel', else s.sales_channel) as chan where s.date.date between '2000-08-23'::date and '2000-09-22'::date limit 3;`

  ```text
  --> 2:35
    |
  2 | select case(s.sales_channel, when 'STORE' then 'store channel', else
  s.sales_channel) as chan where s.date.date between '2000-08-23'::date and
  '2000-09-22'::date limit 3;
    |                                   ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ct case(s.sales_channel, when ??? 'STORE' then 'store channel',
  ```
- `trilogy run --import raw.all_sales:s select case(s.sales_channel, 'STORE', 'store channel', 'CATALOG', 'catalog channel', 'WEB', 'web channel') as chan where s.date.date between '2000-08-23'::date and '2000-09-22'::date limit 3;`

  ```text
  --> 2:108
    |
  2 | select case(s.sales_channel, 'STORE', 'store channel', 'CATALOG', 'catalog
  channel', 'WEB', 'web channel') as chan where s.date.date between
  '2000-08-23'::date and '2000-09-22'::date limit 3;
    |
  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or fcase_simple_when
  Location:
  ...annel', 'WEB', 'web channel') ??? as chan where s.date.date betw...
  ```
- `trilogy run --import raw.all_sales:s select simple_case(s.sales_channel, when 'STORE' then 'store channel', when 'CATALOG' then 'catalog channel', when 'WEB' then 'web channel') as chan where s.date.date between '2000-08-23'::date and '2000-09-22'::date limit 3;`

  ```text
  --> 2:19
    |
  2 | select simple_case(s.sales_channel, when 'STORE' then 'store channel', when
  'CATALOG' then 'catalog channel', when 'WEB' then 'web channel') as chan where
  s.date.date between '2000-08-23'::date and '2000-09-22'::date limit 3;
    |                   ^---
    |
    = expected limit, order_by, where, having, dot_tail, bracket_tail,
  dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...sales as s; select simple_case ??? (s.sales_channel, when 'STORE'...
  ```
- `trilogy file write query77.preql --content import raw.all_sales as s;

auto total_sales <- sum(s.ext_sales_price ? s.date.date between '2000-08-23'::date and…
    g_chan asc nulls first,
    g_outlet asc nulls first,
    channel asc nulls first,
    outlet asc nulls first,
    total_returns desc
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', "'STORE' then 'store channel',
  when 'CATALOG' then 'catalog channel', when 'WEB' then 'web channel') as
  channel,\n    s.channel_dim_id as outlet,\n    total_sales,\n
  coalesce(raw_returns, 0) as total_returns,\n    total_sales_profit,\n
  coalesce(raw_return_loss, 0) as total_return_loss,\n    total_sales_profit -
  coalesce(raw_return_loss, 0) as profit,\n    --g_chan,\n    --g_outlet\norder
  by\n    g_chan asc nulls first,\n    g_outlet asc nulls first,\n    channel asc
  nulls first,\n    outlet asc nulls first,\n    total_returns desc\nlimit 100;")
  at line 16, column 32.
  Expected one of:
          * RPAR
          * COMMA
  Previous tokens: [Token('IDENTIFIER', 'when')]

  Location:
     case(s.sales_channel, when ??? 'STORE' then 'store channel',

  Write stats: received 1606 chars / 1606 bytes; tail: …'   outlet asc nulls
  first,\\n    total_returns desc\\nlimit 100;'.
  ```
- `trilogy file write query77.preql --content import raw.all_sales as s;

case s.sales_channel
    when 'STORE' then 'store channel'
    when 'CATALOG' then 'catalog channel'
    when 'WEB' then 'web channel'
end as channel;

select
    channel,
    s.channel_dim_id as outlet
limit 3;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'case ') at line 3, column 1.
  Expected one of:
          * _PROPERTIES
          * CREATE
          * PARSE_COMMENT
          * PROPERTY
          * MOCK
          * ROWSET
          * DATASOURCE
          * UNIQUE
          * WHERE
          * DEF
          * DATASOURCE_PARTIAL
          * PURPOSE
          * PARAM
          * IMPORT
          * SHOW
          * SELECT
          * "merge"i
          * _DEF_TABLE
          * PARAMETER
          * VALIDATE
          * WITH
          * FROM
          * PUBLISH_ACTION
          * COPY
          * SELF_IMPORT
          * DATASOURCE_ROOT
          * $END
          * AUTO
          * CHART
          * RAW_SQL
          * TYPE
          * SHORTHAND_MODIFIER
          * PERSIST_MODE
  Previous tokens: [Token('LINE_SEPARATOR', '\n\n')]

  Location:
  import raw.all_sales as s;  ??? case s.sales_channel     when

  Write stats: received 239 chars / 239 bytes; tail: …'\\nselect\\n
  channel,\\n    s.channel_dim_id as outlet\\nlimit 3;'.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

# Channel label mapping
auto channel_label <- case(
    all_sales.sales_channe…utlet,
    total_sales as sales,
    total_returns as returns,
    total_profit as profit
order by channel nulls first, outlet nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 8, column 2.
  Expected one of:
          * WHEN

  Location:
  ...annel = 'WEB', 'web channel' ) ??? ;  # Outlet identifier: busine...

  Write stats: received 1482 chars / 1482 bytes; tail: …'\\norder by channel
  nulls first, outlet nulls first\\nlimit 100;'.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

# Channel label mapping
auto channel_label <- case(
    all_sales.sales_channe…utlet,
    total_sales as sales,
    total_returns as returns,
    total_profit as profit
order by channel nulls first, outlet nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 9, column 2.
  Expected one of:
          * WHEN

  Location:
  ...l',     'WEB', 'web channel' ) ??? ;  # Outlet identifier: busine...

  Write stats: received 1229 chars / 1229 bytes; tail: …'\\norder by channel
  nulls first, outlet nulls first\\nlimit 100;'.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

# Channel label mapping
auto channel_label <- simple_case(
    all_sales.sales…utlet,
    total_sales as sales,
    total_returns as returns,
    total_profit as profit
order by channel nulls first, outlet nulls first
limit 100;`

  ```text
  …
  fit) - sum(coalesce(all_sales.return_net_loss,
  0));\n\nwhere all_sales.date.date between '2000-08-23'::date and
  '2000-09-22'::date\n  and all_sales.item.current_price > 50\n  and
  all_sales.promotion.channel_tv = 'N'\n  and all_sales.channel_dim_id is not
  null\nselect\n    channel_label as channel,\n    outlet_id as outlet,\n
  total_sales as sales,\n    total_returns as returns,\n    total_profit as
  profit\norder by channel nulls first, outlet nulls first\nlimit 100;") at line
  4, column 34.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'simple_case')]

  Location:
  ...o channel_label <- simple_case ??? (     all_sales.sales_channel,...

  Write stats: received 1243 chars / 1243 bytes; tail: …'\\norder by channel
  nulls first, outlet nulls first\\nlimit 100;'.
  ```
- `trilogy file write query80.preql`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 9, column 2.
  Expected one of:
          * WHEN

  Location:
  ...,     'WEB', 'web channel' ) ??? ;  # Outlet identifier: busi...

  Write stats: received 1272 chars / 1272 bytes; tail: …'der by channel nulls
  first, outlet nulls first\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query80.preql`

  ```text
  …
  s.return_net_loss,
  0));\r\n\r\nwhere all_sales.date.date between '2000-08-23'::date and
  '2000-09-22'::date\r\n  and all_sales.item.current_price > 50\r\n  and
  all_sales.promotion.channel_tv = 'N'\r\n  and all_sales.channel_dim_id is not
  null\r\nselect\r\n    channel_label as channel,\r\n    outlet_id as outlet,\r\n
  total_sales as sales,\r\n    total_returns as returns,\r\n    total_profit as
  profit\r\norder by channel nulls first, outlet nulls first\r\nlimit 100;\r\n")
  at line 6, column 10.
  Expected one of:
          * RPAR
          * COMMA
  Previous tokens: [Token('IDENTIFIER', 'when')]

  Location:
  ...ales.sales_channel,     when ??? 'STORE', 'store channel',

  Write stats: received 1302 chars / 1302 bytes; tail: …'der by channel nulls
  first, outlet nulls first\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query80.preql`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 4, column 130.
  Expected one of:
          * WHEN

  Location:
  ...hannel', 'WEB', 'web channel') ??? ;  # Outlet identifier: busi...

  Write stats: received 1206 chars / 1206 bytes; tail: …'der by channel nulls
  first, outlet nulls first\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query80.preql`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 4, column 150.
  Expected one of:
          * WHEN

  Location:
  ...talog channel', 'web channel') ??? ;  # Outlet identifier: busi...

  Write stats: received 1246 chars / 1246 bytes; tail: …'der by channel nulls
  first, outlet nulls first\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write test_case.preql`

  ```text
  refused to write 'test_case.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 2, column 80.
  Expected one of:
          * WHEN

  Location:
  ...l = 'STORE', 'store channel') ??? ; select channel_label limit

  Write stats: received 149 chars / 149 bytes; tail: …"'STORE', 'store channel')
  ;\r\\nselect channel_label limit 1;\r\\n".
  ```

### `other`

- `trilogy run query70.preql`

  ```text
  ORDER BY references 'local.state', which is not in the SELECT
  projection (line 10). Add it to SELECT to sort by it — prefix with `--` to keep
  it out of the output rows, e.g. `select ..., --local.state order by local.state
  asc`.
  ```
- `trilogy run query70.preql`

  ```text
  Multiple where clauses are not supported
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Illegal trailing comma before end of array: line 1 column 128 (char 127). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 39 column 12 (char 1588). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 36 column 12 (char 1530). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 32 column 12 (char 1276). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy file write query77.preql -e -c`

  ```text
  Option '-c' requires an argument.
  ```
- `trilogy run query77.preql`

  ```text
  Unable to import '.\all_sales.preql': [Errno 2] No such file
  or directory: '.\\all_sales.preql'. Did you mean: raw.all_sales?
  ```
- `trilogy run query77.preql`

  ```text
  ORDER BY references 'local.g_chan', which is not in the
  SELECT projection (line 11). Add it to SELECT to sort by it — prefix with `--`
  to keep it out of the output rows, e.g. `select ..., --local.g_chan order by
  local.g_chan asc`.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 38 column 12 (char 1523). Re-issue the call with valid JSON arguments.
  ```

### `syntax-missing-alias`

- `trilogy file write query70.preql --content select distinct store.state
order by store.state
limit 20;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `distinct store.state as distinct_store_state`
  Location:
  select distinct store.state ??? order by store.state limit 20;...

  Write stats: received 58 chars / 58 bytes; tail: …'select distinct
  store.state\\norder by store.state\\nlimit 20;'.
  ```
- `trilogy file write query77.preql --content import all_sales as s;

where s.date.date between '2000-08-23'::date and '2000-09-22'::date
   or s.return_date.da…m_id as g_outlet,
    g_chan + g_outlet as level
order by
    g_chan asc nulls first,
    g_outlet asc nulls first,
    total_returns desc
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `total_returns desc as total_returns_desc`
  Location:
   total_returns desc limit 100; ???

  Write stats: received 1979 chars / 1979 bytes; tail: …' g_outlet asc nulls
  first,\\n    total_returns desc\\nlimit 100;'.
  ```
- `trilogy file write query77.preql --content import all_sales as s;

where s.date.date between '2000-08-23'::date and '2000-09-22'::date
   or s.return_date.da…hannel', 'CATALOG', 'catalog channel', 'WEB', 'web channel') asc nulls first,
    s.channel_dim_id asc nulls first,
    total_returns desc
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `total_returns desc as total_returns_desc`
  Location:
   total_returns desc limit 100; ???

  Write stats: received 1708 chars / 1708 bytes; tail: …'el_dim_id asc nulls
  first,\\n    total_returns desc\\nlimit 100;'.
  ```
- `trilogy file write query77.preql --content import all_sales as s;

where s.date.date between '2000-08-23'::date and '2000-09-22'::date
   or s.return_date.da…ase(s.sales_channel, 'STORE', 'store channel', 'CATALOG', 'catalog channel', 'WEB', 'web channel')), s.channel_dim_id as total_return_loss
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `s.channel_dim_id as s_channel_dim_id`
  Location:
  ...s total_return_loss limit 100; ???

  Write stats: received 1363 chars / 1363 bytes; tail: …"channel')),
  s.channel_dim_id as total_return_loss\\nlimit 100;".
  ```
- `trilogy file write query77.preql --content import all_sales as s;

where s.date.date between '2000-08-23'::date and '2000-09-22'::date
   or s.return_date.da…case(s.sales_channel, 'STORE', 'store channel', 'CATALOG', 'catalog channel', 'WEB', 'web channel')), s.channel_dim_id as total_return_loss
limit 100`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `s.channel_dim_id as s_channel_dim_id`
  Location:
  ...as total_return_loss limit 100 ???

  Write stats: received 1362 chars / 1362 bytes; tail: …" channel')),
  s.channel_dim_id as total_return_loss\\nlimit 100".
  ```
- `trilogy run --import raw.all_sales:s select s.sales_channel, count(s.order_id) where s.date.date between '2000-08-23'::date and '2000-09-22'::date group by s.sales_channel limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(s.order_id) as order_id_count`
  Location:
  ...es_channel, count(s.order_id) ??? where s.date.date between '200...
  ```
- `trilogy file write query77.preql --content import raw.all_sales as s;

where s.date.date between '2000-08-23'::date and '2000-09-22'::date
   or s.return_dat…s.return_date.date between '2000-08-23'::date and '2000-09-22'::date), 0) by rollup s.sales_channel, s.channel_dim_id as total_return_loss
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `coalesce(sum(s.return_amount ? s.return_date.date between
  '2000-08-23'::date and '2000-09-22'::date), 0) by rollup s.sales_channel as
  coalesce_sum_s_return_amount_s_return_da`
  Location:
  ...and '2000-09-22'::date), 0) by ??? rollup s.sales_channel, s.cha...

  Write stats: received 904 chars / 904 bytes; tail: …'es_channel,
  s.channel_dim_id as total_return_loss\\nlimit 100;'.
  ```
- `trilogy file write query77.preql --content import raw.all_sales as s;

# Per-channel aggregates with rollup
where s.date.date between '2000-08-23'::date and …s.return_date.date between '2000-08-23'::date and '2000-09-22'::date), 0) by rollup s.sales_channel, s.channel_dim_id as total_return_loss
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `coalesce(sum(s.return_amount ? s.return_date.date between
  '2000-08-23'::date and '2000-09-22'::date), 0) by rollup s.sales_channel as
  coalesce_sum_s_return_amount_s_return_da`
  Location:
  ...and '2000-09-22'::date), 0) by ??? rollup s.sales_channel, s.cha...

  Write stats: received 941 chars / 941 bytes; tail: …'es_channel,
  s.channel_dim_id as total_return_loss\\nlimit 100;'.
  ```
