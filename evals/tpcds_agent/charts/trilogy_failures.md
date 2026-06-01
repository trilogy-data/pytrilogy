# Trilogy failure analysis — 20260531-194745

- Run `20260531-194745_base` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 235 | failed: 26 (11%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 11 | 42% |
| `other` | 10 | 38% |
| `syntax-missing-alias` | 3 | 12% |
| `undefined-concept` | 2 | 8% |

## Detail

### `syntax-parse`

- `trilogy file write query70.preql --content import raw.store_sales as store_sales;

# state-level net profit for year 2000
auto state_profit_2000 <- sum(store…))
order by
    hierarchy_level desc,
    case when hierarchy_level = 0 then store_sales.store.state else NULL end,
    within_parent_rank
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WINDOW_TYPE_SQL_NUMBERING', 'rank(') at line 21, column
  5.
  Expected one of:
          * ORDER
          * LIMIT
          * WHERE
          * _TERMINATOR
          * HAVING
          * METADATA
          * COMMA
          * MERGE
  Previous tokens: [Token('IDENTIFIER', 'test_rank')]

  Location:
  ...--state_rank as test_rank     ??? rank(store_sales.store.state,

  Write stats: received 1552 chars / 1552 bytes; tail: …'store.state else NULL
  end,\\n    within_parent_rank\\nlimit 100;'.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as store_sales;

auto state_profit_2000 <- sum(store_sales.net_profit ? store_sales.date_di…))
order by
    hierarchy_level desc,
    case when hierarchy_level = 0 then store_sales.store.state else NULL end,
    within_parent_rank
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...nd store_sales.store.state in ??? (select store_sales.store.stat...

  Write stats: received 1343 chars / 1343 bytes; tail: …'store.state else NULL
  end,\\n    within_parent_rank\\nlimit 100;'.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as store_sales;

auto state_profit_2000 <- sum(store_sales.net_profit ? store_sales.date_di…nk
order by
    hierarchy_level desc,
    case when hierarchy_level = 0 then store_sales.store.state else NULL end,
    within_parent_rank
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHEN', 'when') at line 24, column 10.
  Expected one of:
          * MERGE
          * LIMIT
          * _TERMINATOR

  Location:
  ...ierarchy_level desc,     case ??? when hierarchy_level = 0 then

  Write stats: received 1205 chars / 1205 bytes; tail: …'store.state else NULL
  end,\\n    within_parent_rank\\nlimit 100;'.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as store_sales;

# state-level net profit for year 2000
auto state_profit_2000 <- sum(store…ounty,
    g_state + g_county as hierarchy_level
order by
    hierarchy_level desc,
    store_sales.store.state,
    total_net_profit desc
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'def ') at line 12, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...e_sales.store.state is null)  ??? def rollup_metrics(metric) ->

  Write stats: received 997 chars / 997 bytes; tail: …'tore_sales.store.state,\\n
  total_net_profit desc\\nlimit 100;'.
  ```
- `trilogy file write query77.preql`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'union ') at line 50, column 1.
  Expected one of:
          * MERGE
          * HAVING
          * ORDER
          * LIMIT
          * _TERMINATOR
  Previous tokens: [Token('NULL', 'null')]

  Location:
  ...store.store_sk is not null  ??? union all  select     'cata...

  Write stats: received 3658 chars / 3658 bytes; tail: …'ls first, outlet asc
  nulls first, returns desc\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query77.preql`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 35, column 44.
  Expected one of:
          * _TERMINATOR

  Location:
  ..._agg <- coalesce(st_sales, 0) ??? by 'store channel', ss.store.s...

  Write stats: received 3496 chars / 3496 bytes; tail: …'s profit\r\\nwhere
  ss.store.store_sk is not null\r\\n\r\\nlimit 50;\r\\n'.
  ```
- `trilogy file write query77.preql`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', "'store channel',
  ss.store.store_sk as chk\r\nwhere ss.store.store_sk is not null\r\norder by
  channel asc nulls first, outlet asc nulls first, returns desc\r\nlimit
  100;\r\n") at line 42, column 42.
  Expected one of:
          * IDENTIFIER
          * LPAR
  Previous tokens: [Token('ROLLUP', 'rollup')]

  Location:
  ...lesce(st_sales, 0)) by rollup ??? 'store channel', ss.store.stor...

  Write stats: received 2777 chars / 2777 bytes; tail: …'ls first, outlet asc
  nulls first, returns desc\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query77.preql`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'from ') at line 34, column 56.
  Expected one of:
          * HAVING
          * WHERE
          * LIMIT
          * ORDER
          * _TERMINATOR
          * COMMA
          * METADATA
          * MERGE
  Previous tokens: [Token('IDENTIFIER', 'y')]

  Location:
  ...as x, coalesce(sto_s, 0) as y ??? from ss where ss.store.store_s...

  Write stats: received 2377 chars / 2377 bytes; tail: …"between
  '2000-08-23'::date and '2000-09-22'::date limit 3;\r\\n".
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.store.store_id, count(*) as cnt, sum(store_sales.net_paid) as total_sales where store_sal…te and store_sales.item.current_price > 50 and store_sales.promotion.channel_tv = 'N' group by store_sales.store.store_id order by cnt desc limit 10;`

  ```text
  --> 2:42
    |
  2 | select store_sales.store.store_id, count(*) as cnt,
  sum(store_sales.net_paid) as total_sales where store_sales.date_dim.date
  between '2000-08-23'::date and '2000-09-22'::date and
  store_sales.item.current_price > 50 and store_sales.promotion.channel_tv = 'N'
  group by store_sales.store.store_id order by cnt desc limit 10;
    |                                          ^---
    |
    = expected access_chain
  Location:
  ...e_sales.store.store_id, count( ??? *) as cnt, sum(store_sales.net...
  ```
- `trilogy file write query80.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

merge sr.ticket_number into ~ss.ticket_number;
merg…_site.site_id) as outlet,
    @rollup_store(ws.net_paid, wr.return_amt, ws.net_profit, wr.net_loss)
order by g_out asc, outlet nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 9, column 37.
  Expected one of:
          * _TERMINATOR

  Location:
  ...m(sales_amt) by rollup outlet ??? as sales_val,     coalesce(sum...

  Write stats: received 2033 chars / 2033 bytes; tail: …'.net_loss)\\norder by
  g_out asc, outlet nulls first\\nlimit 100;'.
  ```
- `trilogy file write query80.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…ore_id) as ol,
    ss_sales as sales,
    ss_returns as returns,
    ss_profit - ss_loss as profit
order by ch nulls first, ol nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 26, column 7.
  Expected one of:
          * _TERMINATOR

  Location:
   and '2000-09-22'::date ), 0) ??? by sr.store.store_id;  auto ss...

  Write stats: received 1535 chars / 1535 bytes; tail: …'as profit\\norder by ch
  nulls first, ol nulls first\\nlimit 100;'.
  ```

### `other`

- `trilogy run query77.preql duckdb`

  ```text
  Concept local.store_outlet_id references itself
  ```
- `trilogy run query77.preql duckdb`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.ss_profit_by_store', 'local.sr_loss_by_store'} out of  with
  found {'sr.store.store_sk', 'local.ss_sales_by_store',
  'local.sr_returns_by_store', 'local.sr_loss_by_store', 'ss.store.store_sk',
  'local.ss_profit_by_store'}
  ```
- `trilogy run query77.preql duckdb`

  ```text
  (_duckdb.BinderException) Binder Error: GROUPING child "$1"
  must be a grouping column

  LINE 40:     grouping($1) as "g_chan",
               ^
  [SQL:
  WITH
  uneven as (
  SELECT
      $1 as "st_channel",
      coalesce("ss_store_sales"."ss_store_sk","ss_store_store"."s_store_sk") as
  "ss_store_store_sk",
      sum("ss_store_sales"."ss_ext_sales_price") as "st_sales",
      sum("ss_store_sales"."ss_net_profit") as "st_prof"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "store" as "ss_store_store" on "ss_store_sales"."ss_store_sk" =
  "ss_store_store"."s_store_sk"
      INNER JOIN "date_dim" as "s
  …
  "."ss_store_store_sk")
  SELECT
      "quizzical"."channel" as "channel",
      "concerned"."outlet" as "outlet",
      "concerned"."sales" as "sales",
      "concerned"."returns" as "returns",
      "concerned"."profit" as "profit",
      "concerned"."sales_rollup" as "sales_rollup",
      "concerned"."g_chan" as "g_chan",
      "concerned"."g_out" as "g_out"
  FROM
      "quizzical"
      FULL JOIN "concerned" on 1=1
  ORDER BY
      "quizzical"."channel" asc nulls first,
      "concerned"."outlet" asc nulls first,
      "concerned"."returns" desc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query77.preql duckdb`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.cs_prof_amt', 'local._virt_agg_sum_6104551283192032',
  'local._virt_agg_sum_3019548774459060'} out of  with found
  {'local.cs_prof_amt', 'cs.call_center.call_center_sk', 'local.cs_sales_amt'}
  ```
- `trilogy run query77.preql duckdb`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.sta_p', 'local.cat_p', 'local.web_p'} out of  with found
  {'local.sta_p', 'sr.store.store_sk', 'local.sta_s', 'ss.store.store_sk',
  'wr.web_page.web_page_sk', 'cr.call_center.call_center_sk', 'local.web_s',
  'local.cat_p', 'cs.call_center.call_center_sk', 'local.cat_s',
  'ws.web_page.web_page_sk', 'local.web_p'}
  ```
- `trilogy run query80.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.store_profit', 'local._virt_agg_sum_6151717147621237'} out of
  with found {'store_sales.store.store_id', 'store_returns.store.store_id',
  'local.store_sales_amt', 'local._virt_agg_sum_7494358638996209',
  'local.store_profit', 'local._virt_agg_sum_6151717147621237'}
  ```
- `trilogy run query80.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.ss_profit', 'local.ss_net_loss'} out of  with found
  {'local.ss_net_loss', 'local.ss_sales', 'local.ss_returns', 'local.ss_profit',
  'store_returns.store.store_id', 'store_sales.store.store_id'}
  ```
- `trilogy run query80.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.sales', 'local._virt_agg_sum_1127084394800731',
  'local._virt_agg_sum_6502742331444045', 'local._virt_agg_sum_9224703855125449'}
  out of  with found {'local.sales', 'local.outlet'}
  ```
- `trilogy run query80.preql`

  ```text
  (_duckdb.BinderException) Binder Error: Referenced table
  "abundant" not found!
  Candidate tables: "questionable"

  LINE 32: ...  sum("questionable"."ss_net_profit") - coalesce(sum(CASE WHEN
  "abundant"."ss_ticket_number" in (select abundant."ss_ticke...
                                                                             ^
  [SQL:
  WITH
  questionable as (
  SELECT
      "sr_store_returns"."sr_net_loss" as "sr_net_loss",
      "sr_store_returns"."sr_return_amt" as "sr_return_amt",
      "ss_store_sales"."ss_net_paid" as "ss_net_paid",
      "ss_store_sales"."ss_net_profit" as "ss_net_profit",
      "ss
  …
  lesce(sum("questionable"."sr_return_amt"),0) as "returns",
      sum("questionable"."ss_net_profit") - coalesce(sum(CASE WHEN
  "abundant"."ss_ticket_number" in (select abundant."ss_ticket_number" from
  abundant where abundant."ss_ticket_number" is not null) and
  "ss_item_item"."i_item_sk" in (select ss_item_item."i_item_sk" from item as
  ss_item_item where ss_item_item."i_item_sk" is not null) THEN
  "questionable"."sr_net_loss" ELSE NULL END),0) as "profit"
  FROM
      "questionable"
  GROUP BY
      2
  ORDER BY
      "outlet" asc nulls first]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr select 1 as ch, concat('store', ss.store.store_id) as outlet, sum(ss.net_paid) as sales…nd '2000-09-22'::date and ss.item.current_price > 50 and ss.promotion.channel_tv = 'N' and ss.store.store_id is not null order by outlet nulls first;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local._virt_agg_sum_6786213035983152',
  'local._virt_agg_sum_5237463246240634', 'local._virt_agg_sum_2856259540536641',
  'local.sales'} out of  with found {'local.outlet', 'local.sales'}
  ```

### `syntax-missing-alias`

- `trilogy file write query70.preql --content import raw.store_sales as store_sales;

auto state_profit_2000 <- sum(store_sales.net_profit ? store_sales.date_di…ls better
    row_number() over (order by total_net_profit desc) as test_rn
order by
    hierarchy_level desc,
    store_sales.store.state
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `use dense_rank which handles nulls better
      row_number() over (order by total_net_profit desc) as
  use_dense_rank_which_handles_nulls_bette`
  Location:
  ..._level,     -- use dense_rank ??? which handles nulls better

  Write stats: received 1129 chars / 1129 bytes; tail: …'hierarchy_level desc,\\n
  store_sales.store.state\\nlimit 100;'.
  ```
- `trilogy file write query80.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

merge sr.ticket_number into ~ss.ticket_number;
merg…it) - coalesce(sum(wr.net_loss), 0) by rollup outlet as profit,
    grouping(outlet) as g_outlet
order by g_outlet asc, outlet nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `coalesce(sum(sr.return_amt), 0) by rollup outlet as
  coalesce_sum_sr_return_amt_0_by_rollup_o`
  Location:
  ...esce(sum(sr.return_amt), 0) by ??? rollup outlet as returns,

  Write stats: received 2139 chars / 2139 bytes; tail: …'_outlet\\norder by
  g_outlet asc, outlet nulls first\\nlimit 100;'.
  ```
- `trilogy file write query80.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r… 0) by outlet as returns,
    sum(ws.net_profit) by outlet - coalesce(sum(wr.net_loss), 0) by outlet as profit
order by outlet nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `coalesce(sum(sr.return_amt), 0) by outlet as
  coalesce_sum_sr_return_amt_0_by_outlet`
  Location:
  ...esce(sum(sr.return_amt), 0) by ??? outlet as returns,     sum(ss...

  Write stats: received 2141 chars / 2141 bytes; tail: …') by outlet as
  profit\\norder by outlet nulls first\\nlimit 100;'.
  ```

### `undefined-concept`

- `trilogy run query80.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  date_dim.date. Suggestions: ['web_returns.date_dim.date',
  'store_sales.date_dim.date', 'store_returns.date_dim.date']")
  ```
- `trilogy run query80.preql`

  ```text
  (UndefinedConceptException(...), "line: 17: Undefined
  concept: store_sales.store_returns.return_amt. Suggestions:
  ['store_returns.return_amt', 'store_sales.store.street_name',
  'store_returns.return_tax']")
  ```
