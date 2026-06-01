# Trilogy failure analysis — 20260601-190402

- Run `20260601-190401_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 301 | failed: 25 (8%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 9 | 36% |
| `other` | 7 | 28% |
| `undefined-concept` | 6 | 24% |
| `syntax-missing-alias` | 2 | 8% |
| `cli-misuse` | 1 | 4% |

## Detail

### `syntax-parse`

- `trilogy run --import raw.all_sales:sales select sales.date.week_seq, sum(sales.ext_sales_price) as total where sales.date.year = 2001 and sales.sales_channel in ('WEB', 'CATALOG') group by sales.date.week_seq order by sales.date.week_seq limit 5;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...channel in ('WEB', 'CATALOG') ??? group by sales.date.week_seq o...
  ```
- `trilogy file write query04.preql --content import raw.physical_sales as ps;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

merge ps.billing_cus…st_flag
order by customer_code asc nulls first, first_name asc nulls first, last_name asc nulls first, preferred_cust_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 31, column 51.
  Expected one of:
          * _TERMINATOR

  Location:
  ...01_positive <- store_2001 > 0 ??? and catalog_2001 > 0 and web_2...

  Write stats: received 2331 chars / 2331 bytes; tail: …' nulls first,
  preferred_cust_flag asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Date range boundaries
auto sale_in_range <- sales.date.date between '2000-08-23'…y rollup (channel_label, outlet_label) as g_outlet

order by g_channel asc, g_outlet asc, channel asc nulls first, outlet asc nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 8, column 126.
  Expected one of:
          * WHEN

  Location:
  ...hannel', 'WEB', 'web channel') ??? ;  # Outlet label: kind + busi...

  Write stats: received 1911 chars / 1911 bytes; tail: …' channel asc nulls
  first, outlet asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Date range boundaries
auto sale_in_range <- sales.date.date between '2000-08-23'…by rollup (channel_label, outlet_label) as g_outlet
order by g_channel asc, g_outlet asc, channel asc nulls first, outlet asc nulls first
limit 100;
`

  ```text
  …
  ollup (channel_label, outlet_label) as
  total_sales,\n    sum(returns_sum) by rollup (channel_label, outlet_label) as
  total_returns,\n    sum(profit_sum) by rollup (channel_label, outlet_label) as
  total_profit,\n    grouping(channel_label) by rollup (channel_label,
  outlet_label) as g_channel,\n    grouping(outlet_label) by rollup
  (channel_label, outlet_label) as g_outlet\norder by g_channel asc, g_outlet
  asc, channel asc nulls first, outlet asc nulls first\nlimit 100;\n") at line 8,
  column 34.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'simple_case')]

  Location:
  ...o channel_label <- simple_case ??? (sales.sales_channel, 'STORE',...

  Write stats: received 1998 chars / 1998 bytes; tail: …' channel asc nulls
  first, outlet asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Date range boundaries - filter rows where either sale or return is in range

# C…by rollup (channel_label, outlet_label) as g_outlet
order by g_channel asc, g_outlet asc, channel asc nulls first, outlet asc nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WILDCARD_IDENTIFIER', 'channel_label') at line 27,
  column 31.
  Expected one of:
          * RPAR
  Previous tokens: [Token('LPAR', '(')]

  Location:
      sum(sales_amt) by rollup ( ??? channel_label, outlet_label) a...

  Write stats: received 1891 chars / 1891 bytes; tail: …' channel asc nulls
  first, outlet asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Channel label
auto channel_label <- case when sales.sales_channel = 'STORE' then…hannel_label, outlet_label) as g_outlet
order by g_channel asc, g_outlet asc, channel_label asc nulls first, outlet_label asc nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WILDCARD_IDENTIFIER', 'channel_label') at line 14,
  column 111.
  Expected one of:
          * RPAR
  Previous tokens: [Token('LPAR', '(')]

  Location:
  ...2000-09-06'::date) by rollup ( ??? channel_label, outlet_label) a...

  Write stats: received 1683 chars / 1683 bytes; tail: …'el asc nulls first,
  outlet_label asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Channel label
auto channel_label <- case when sales.sales_channel = 'STORE' then…hannel_label, outlet_label) as g_outlet
order by g_channel asc, g_outlet asc, channel_label asc nulls first, outlet_label asc nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WILDCARD_IDENTIFIER', 'channel_label') at line 20,
  column 30.
  Expected one of:
          * RPAR
  Previous tokens: [Token('LPAR', '(')]

  Location:
       sum(sale_val) by rollup ( ??? channel_label, outlet_label) a...

  Write stats: received 1833 chars / 1833 bytes; tail: …'el asc nulls first,
  outlet_label asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Channel label
auto channel_label <- case when sales.sales_channel = 'STORE' then…g1 as g_channel,
    @rl_g2 as g_outlet
order by g_channel asc, g_outlet asc, channel_label asc nulls first, outlet_label asc nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_BINDING', '->') at line 20, column 11.
  Expected one of:
          * LPAR
  Previous tokens: [Token('IDENTIFIER', 'rl_g1')]

  Location:
  ...abel, outlet_label; def rl_g1 ??? -> grouping(channel_label) by

  Write stats: received 2069 chars / 2069 bytes; tail: …'el asc nulls first,
  outlet_label asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query08.preql --content parameter zips string;

# ZIP codes where more than 10 preferred customers have their current home address
auto pr…(zips, ',')

select
    physical_sales.store.name as store_name
    sum(physical_sales.net_profit) as total_net_profit
order by store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_SUM', 'sum(') at line 18, column 5.
  Expected one of:
          * MERGE
          * HAVING
          * LIMIT
          * COMMA
          * METADATA
          * WHERE
          * ORDER
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'store_name')]

  Location:
  ....store.name as store_name     ??? sum(physical_sales.net_profit)...

  Write stats: received 903 chars / 903 bytes; tail: …'t_profit) as
  total_net_profit\\norder by store_name\\nlimit 100;'.
  ```

### `other`

- `trilogy run query01.preql`

  ```text
  HAVING references 'local.qualifies', which is not in the
  SELECT projection (line 14). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.qualifies
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run --import raw.all_sales:sales auto day_total <- sum(sales.ext_sales_price ? sales.sales_channel in ('WEB', 'CATALOG')) by sales.date.week_seq, sal…es.sales_channel in ('WEB', 'CATALOG')
  and sales.date.week_seq between 5270 and 5322
order by sales.date.week_seq, sales.date.day_of_week
limit 14;`

  ```text
  ORDER BY references 'sales.date.day_of_week', which is not in
  the SELECT projection (line 4). Add it to SELECT to sort by it — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --sales.date.day_of_week order by sales.date.day_of_week asc`.
  ```
- `trilogy run query05.preql`

  ```text
  list index out of range
  ```
- `trilogy run query05.preql`

  ```text
  list index out of range
  ```
- `trilogy run query05.preql`

  ```text
  list index out of range
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: Cannot compare values
  of type VARCHAR and VARCHAR[] in IN/ANY/ALL clause - an explicit cast is
  required

  LINE 39: ...380358091742" is not null) and "sales_store_store"."S_ZIP" in
  (select questionable."_virt_func_split_4785012549328100...
                                                                         ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      SUBSTRING("customer_address_customer_address"."CA_ZIP",1,2) as
  "_virt_func_substring_22380358091742"
  FROM
      "customer" as "customer_customers"
      INNER JOIN "customer_address" as "customer_address_customer
  …
  thoughtful."_virt_func_substring_22380358091742" is not null) and
  "sales_store_store"."S_ZIP" in (select
  questionable."_virt_func_split_4785012549328100" from questionable where
  questionable."_virt_func_split_4785012549328100" is not null)

  GROUP BY
      1,
      2,
      "sales_store_sales"."SS_ITEM_SK",
      "sales_store_sales"."SS_TICKET_NUMBER")
  SELECT
      "juicy"."sales_store_name" as "store_name",
      sum("juicy"."sales_net_profit") as "total_net_profit"
  FROM
      "juicy"
  GROUP BY
      1
  ORDER BY
      "store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "render"

  LINE 19:     CASE WHEN ( INVALID_REFERENCE_BUG_<Cannot render aggregate
  local._virt_agg_count_6728219847194714...
                                                         ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "customer_address_customer_address"."CA_ZIP" as "customer_address_zip",
      "customer_customers"."C_CUSTOMER_SK" as "_virt_filter_id_8603331542397217"
  FROM
      "customer" as "customer_customers"
      INNER JOIN "customer_address" as "customer_address_customer_address" on
  "customer_customers"."C_CURRENT_ADDR_SK" =
  …
   = 1998 and "sales_date_date"."D_QOY" = 2 and
  SUBSTRING("sales_store_store"."S_ZIP",1,2) in (select
  cooperative."_virt_func_substring_1010830310934997" from cooperative where
  cooperative."_virt_func_substring_1010830310934997" is not null)

  GROUP BY
      1,
      2,
      "sales_store_sales"."SS_ITEM_SK",
      "sales_store_sales"."SS_TICKET_NUMBER")
  SELECT
      "juicy"."sales_store_name" as "store_name",
      sum("juicy"."sales_net_profit") as "total_net_profit"
  FROM
      "juicy"
  GROUP BY
      1
  ORDER BY
      "store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```

### `undefined-concept`

- `trilogy run test_check3.preql duckdb`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  ws.ext_list_price. Suggestions: ['ws.ext_sales_price', 'ws.sales_price',
  'ws.ext_ship_cost']")
  ```
- `trilogy run query04.preql duckdb`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['c.first_name', 'c.last_name']")
  ```
- `trilogy run query04.preql duckdb`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['c.first_name', 'c.last_name']")
  ```
- `trilogy run query04.preql duckdb`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  first_name.')
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  customer.preferred_cust_flag.')
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), 'line: 11: Undefined
  concept: physical_sales.net_profit.')
  ```

### `syntax-missing-alias`

- `trilogy file write query05.preql --content import raw.all_sales as sales;

select distinct sales.sales_channel, 
    count(1) as cnt
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `distinct sales.sales_channel as distinct_sales_sales_channel`
  Location:
  ...t distinct sales.sales_channel ??? ,      count(1) as cnt limit 1...

  Write stats: received 101 chars / 101 bytes; tail: …'stinct
  sales.sales_channel, \\n    count(1) as cnt\\nlimit 100;\\n'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

auto channel_label <- case when sales.sales_channel = 'STORE' then 'store channel'…id) when sales.sales_channel = 'WEB' then concat('web_site', sales.channel_dim_text_id) end;

select distinct channel_label, outlet_label
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `distinct channel_label as distinct_channel_label`
  Location:
   select distinct channel_label ??? , outlet_label limit 100;

  Write stats: received 579 chars / 579 bytes; tail: …'nd;\\n\\nselect distinct
  channel_label, outlet_label\\nlimit 100;\\n'.
  ```

### `cli-misuse`

- `trilogy run - --import raw/physical_sales:ps --import raw/catalog_sales:cs --import raw/web_sales:ws select ps.billing_customer.text_id, ps.date.year, sum((ps.ext_list_price - ps.ext_wholesale_cost - ps.ext_discount_amount + ps.ext_sales_price) / 2) as store_total limit 5;`

  ```text
  'select ps.billing_customer.text_id, ps.date.year, sum((ps.ext_list_price - ps.ext_wholesale_cost - ps.ext_discount_amount + ps.ext_sales_price) / 2) as store_total limit 5;' looks like a file path, not a dialect. The dialect argument comes AFTER the input file.
    Try: trilogy run select ps.billing_customer.text_id, ps.date.year, sum((ps.ext_list_price - ps.ext_wholesale_cost - ps.ext_discount_amount + ps.ext_sales_price) / 2) as store_total limit 5; <dialect>
  ```
