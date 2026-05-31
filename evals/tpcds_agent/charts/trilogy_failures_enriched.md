# Trilogy failure analysis — 20260531-160146

- Run `20260531-160145_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 163 | failed: 20 (12%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 8 | 40% |
| `other` | 5 | 25% |
| `syntax-missing-alias` | 5 | 25% |
| `undefined-concept` | 1 | 5% |
| `type-error` | 1 | 5% |

## Detail

### `syntax-parse`

- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

# Label for channel
auto channel_label <-
    case(
        all_sales.sales_ch…sales as sales,
    total_returns as returns,
    total_profit as profit
order by
    channel_label nulls first,
    outlet_id nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 9, column 6.
  Expected one of:
          * WHEN

  Location:
  ...l = 'WEB', 'web channel'     ) ??? ;  # Outlet identifier: prefix...

  Write stats: received 1407 chars / 1407 bytes; tail: …'nnel_label nulls
  first,\\n    outlet_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

# Label for channel
auto channel_label <-
    case(
        all_sales.sales_ch…sales as sales,
    total_returns as returns,
    total_profit as profit
order by
    channel_label nulls first,
    outlet_id nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 9, column 6.
  Expected one of:
          * WHEN

  Location:
  ...l = 'WEB', 'web channel'     ) ??? ;  # Outlet identifier: prefix...

  Write stats: received 1407 chars / 1407 bytes; tail: …'nnel_label nulls
  first,\\n    outlet_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

# Label for channel
auto channel_label <-
    simple_case(all_sales.sales_chan…sales as sales,
    total_returns as returns,
    total_profit as profit
order by
    channel_label nulls first,
    outlet_id nulls first
limit 100;`

  ```text
  …
  profit) -
  coalesce(sum(all_sales.return_net_loss), 0);\n\nwhere all_sales.date.date
  between '2000-08-23'::date and '2000-09-22'::date\n  and
  all_sales.item.current_price > 50\n  and all_sales.promotion.channel_tv = 'N'\n
  and all_sales.channel_dim_id is not null\n\nselect\n    channel_label,\n
  outlet_id,\n    total_sales as sales,\n    total_returns as returns,\n
  total_profit as profit\norder by\n    channel_label nulls first,\n    outlet_id
  nulls first\nlimit 100;") at line 5, column 16.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'simple_case')]

  Location:
  ...annel_label <-     simple_case ??? (all_sales.sales_channel,

  Write stats: received 1313 chars / 1313 bytes; tail: …'nnel_label nulls
  first,\\n    outlet_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

# Label for channel
auto channel_label <- case(all_sales.sales_channel = 'STOR…sales as sales,
    total_returns as returns,
    total_profit as profit
order by
    channel_label nulls first,
    outlet_id nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 4, column 183.
  Expected one of:
          * WHEN

  Location:
  ...hannel = 'WEB', 'web channel') ??? ;  # Outlet identifier: prefix...

  Write stats: received 1339 chars / 1339 bytes; tail: …'nnel_label nulls
  first,\\n    outlet_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

# Derived fields - these are row-level, not aggregate
auto channel_label <- ca…tlet_id) by rollup channel_label, outlet_id as g_out
order by
    g_chan + g_out,
    channel_label nulls first,
    outlet_id nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens — write `by
  (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any
  function call, cast, or other expression needs them.
  Location:
  ...bel, outlet_id as g_out order ??? by     g_chan + g_out,     cha...

  Write stats: received 1480 chars / 1480 bytes; tail: …'nnel_label nulls
  first,\\n    outlet_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query83.preql --content import raw.catalog_returns as cr;
import raw.web_returns as wr;
import raw.physical_sales as ps;

# Merge the item…_qty + catalog_return_qty + web_return_qty) / 3) as three_channel_avg
order by item_code asc nulls first, store_return_qty asc nulls first
limit 100;`

  ```text
  …
  ) / 3 * 100) as store_return_pct,\n
  catalog_return_qty,\n    (catalog_return_qty / (store_return_qty +
  catalog_return_qty + web_return_qty) / 3 * 100) as catalog_return_pct,\n
  web_return_qty,\n    (web_return_qty / (store_return_qty + catalog_return_qty +
  web_return_qty) / 3 * 100) as web_return_pct,\n    ((store_return_qty +
  catalog_return_qty + web_return_qty) / 3) as three_channel_avg\norder by
  item_code asc nulls first, store_return_qty asc nulls first\nlimit 100;') at
  line 11, column 29.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'array')]

  Location:
  ...4 auto relevant_weeks <- array ??? (5244, 5257, 5264);  # Per-ite...

  Write stats: received 2020 chars / 2020 bytes; tail: …'asc nulls first,
  store_return_qty asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query86.preql --content import raw.web_sales as web_sales;

where web_sales.date.year = 2000

# Define the rolled-up net paid aggregate on…rder by total_net_paid desc
    ) as rnk
order by
    level desc nulls first,
    web_sales.item.category nulls first,
    rnk nulls first
limit 100;`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 6, column 1.
  Expected one of:
          * SELECT

  Location:
  ...ed-up net paid aggregate once ??? auto total_net_paid <- sum(web...

  Write stats: received 850 chars / 850 bytes; tail: …'es.item.category nulls
  first,\\n    rnk nulls first\\nlimit 100;'.
  ```
- `trilogy file write query86.preql --content import raw.web_sales as web_sales;

where web_sales.date.year = 2000

# Define the rolled-up net paid aggregate on…rder by total_net_paid desc
    ) as rnk
order by
    level desc nulls first,
    web_sales.item.category nulls first,
    rnk nulls first
limit 100;`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 6, column 1.
  Expected one of:
          * SELECT

  Location:
  ...ed-up net paid aggregate once ??? auto total_net_paid <- sum(web...

  Write stats: received 850 chars / 850 bytes; tail: …'es.item.category nulls
  first,\\n    rnk nulls first\\nlimit 100;'.
  ```

### `other`

- `trilogy file write --content import raw.all_sales as all_sales;

# Label for channel
auto channel_label <-
    case(
        all_sales.sales_channel = 'STORE…,
    total_returns as returns,
    total_profit as profit
order by
    channel_label nulls first,
    outlet_id nulls first
limit 100; query80.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run query81.preql`

  ```text
  HAVING references 'local.state_avg', which is not in the
  SELECT projection (line 9). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.state_avg
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query82.preql`

  ```text
  Unable to import '.\physical_sales.preql': [Errno 2] No such
  file or directory: '.\\physical_sales.preql'. Did you mean: raw.physical_sales?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 39 column 12 (char 1657). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file write query89.preql --content`

  ```text
  Option '--content' requires an argument.
  ```

### `syntax-missing-alias`

- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

# Label for channel
auto channel_label <- case when all_sales.sales_channel = …el_label, outlet_id as g_out,
    g_chan + g_out as level
order by
    level asc,
    channel_label nulls first,
    outlet_id nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `total_sales by rollup channel_label as
  total_sales_by_rollup_channel_label`
  Location:
   outlet_id,     total_sales by ??? rollup channel_label, outlet_...

  Write stats: received 1687 chars / 1687 bytes; tail: …'nnel_label nulls
  first,\\n    outlet_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

# Derived fields - these are row-level, not aggregate
auto channel_label <- ca…tlet_id) by rollup channel_label, outlet_id as g_out
order by
    g_chan + g_out,
    channel_label nulls first,
    outlet_id nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `coalesce(sum(all_sales.return_amount), 0) by rollup
  channel_label as coalesce_sum_all_sales_return_amount_0_b`
  Location:
  ...ll_sales.return_amount), 0) by ??? rollup channel_label, outlet_...

  Write stats: received 1392 chars / 1392 bytes; tail: …'nnel_label nulls
  first,\\n    outlet_id nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.address:a select distinct a.city limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `distinct a.city as distinct_a_city`
  Location:
  ...address as a; select distinct ??? a.city limit 20;
  ```
- `trilogy run --import raw.physical_returns:r select count(r.ticket_number);`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(r.ticket_number) as
  ticket_number_count`
  Location:
   select count(r.ticket_number) ??? ;
  ```
- `trilogy file write query87.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as cat_sales;
import raw.web_sales as web_sales….date.year = 2000
  and store_key not in cat_key
  and store_key not in web_key

select
  count_distinct(store_key) as unique_combinations
limit 10;
`

  ```text
  refused to write 'query87.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `'string'));
  auto cat_key <- concat(cat_sales.bill_customer.last_name, '|',
  cat_sales.bill_customer.first_name, '|', cast(cat_sales.sold_date.date as
  string_auto_cat_key_concat_cat_sales_bil`
  Location:
  ...', cast(store_sales.date.date, ??? 'string')); auto cat_key <- c...

  Write stats: received 908 chars / 908 bytes; tail: …' count_distinct(store_key)
  as unique_combinations\\nlimit 10;\\n'.
  ```

### `undefined-concept`

- `trilogy run query85.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  reason.desc. Suggestions: ['web_returns.reason.desc']")
  ```

### `type-error`

- `trilogy run query87.preql`

  ```text
  Invalid argument type 'DATE' passed into CONCAT function in
  position 5 from concept: store_sales.date.date. Valid: 'STRING'.
  ```
