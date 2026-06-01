# Trilogy failure analysis — 20260601-175357

- Run `20260601-175357_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 324 | failed: 30 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 11 | 37% |
| `syntax-parse` | 10 | 33% |
| `undefined-concept` | 4 | 13% |
| `join-resolution` | 3 | 10% |
| `syntax-missing-alias` | 1 | 3% |
| `type-error` | 1 | 3% |

## Detail

### `other`

- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws merge cs.item.item_sk into ~ws.item.item_sk; auto cs_daily <- sum(cs.ext_sales_price) by …seq, round(wkly_dow / nullif(nxt_wkly_dow, 0), 2) as ratio where cs.sold_date.year between 2001 and 2002 and cs.sold_date.dow = 1 order by 1 limit 5;`

  ```text
  Recursion error building concept local.ratio with grain
  Grain<cs.sold_date.dow,cs.sold_date.week_seq> and lineage
  round(divide(ref:local.wkly_dow,nullif(ref:local.nxt_wkly_dow,0)),2). This is
  likely due to a circular reference.
  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws merge cs.item.item_sk into ~ws.item.item_sk; auto dow_sales <- sum(cs.ext_sales_price) + …week_seq, dow1_sales, nxt1_sales, round(dow1_sales / nullif(nxt1_sales, 0), 2) as ratio where cs.sold_date.week_seq between 5270 and 5275 order by 1;`

  ```text
  Recursion error building concept local.ratio with grain
  Grain<local.dow_sales,local.nxt_sales> and lineage
  round(divide(ref:local.dow1_sales,nullif(ref:local.nxt1_sales,0)),2). This is
  likely due to a circular reference.
  ```
- `trilogy run query04_check2.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query04_debug2.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy file write query05.preql --content`

  ```text
  Option '--content' requires an argument.
  ```
- `trilogy file write query05.preql -e -c`

  ```text
  Option '-c' requires an argument.
  ```
- `trilogy file write query05.preql -e`

  ```text
  --escapes only applies to --content.
  ```
- `trilogy file write query05.preql --content`

  ```text
  Option '--content' requires an argument.
  ```
- `trilogy run query05.preql duckdb`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local._virt_filter_net_loss_5060387553640372', 'local.outlet'} out
  of  with found {'sr.ticket_number', 'sr.item.item_sk',
  'local._virt_filter_net_loss_5060387553640372',
  'local._virt_filter_return_amt_1494936058274708'}
  ```
- `trilogy run query05.preql duckdb`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.outlet', 'local._virt_filter_net_loss_5060387553640372'} out
  of  with found {'sr.item.item_sk', 'sr.ticket_number',
  'local._virt_filter_net_loss_5060387553640372',
  'local._virt_filter_return_amt_1494936058274708'}
  ```
- `trilogy run _debug4.preql`

  ```text
  (_duckdb.BinderException) Binder Error: GROUP BY clause
  cannot contain aggregates!

  LINE 20:     CASE WHEN (
  count("wakeful"."_virt_filter_customer_sk_8358463803893042...
                           ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "customer_customer"."c_customer_sk" as
  "_virt_filter_customer_sk_8358463803893042",
      "customer_customer_address_customer_address"."ca_zip" as
  "customer_customer_address_zip"
  FROM
      "customer_address" as "customer_customer_address_customer_address"
      INNER JOIN "customer" as "customer_customer" on
  "customer_customer_address_customer_address"."ca_addre
  …

  SELECT
      CASE WHEN ( count("wakeful"."_virt_filter_customer_sk_8358463803893042") )
  > 10 THEN "wakeful"."customer_customer_address_zip" ELSE NULL END as
  "pref_zips",
      SUBSTRING(CASE WHEN (
  count("wakeful"."_virt_filter_customer_sk_8358463803893042") ) > 10 THEN
  "wakeful"."customer_customer_address_zip" ELSE NULL END,1,2) as "pref_prefix"
  FROM
      "wakeful"
  GROUP BY
      1,
      2,
      "wakeful"."_virt_filter_customer_sk_8358463803893042",
      "wakeful"."customer_customer_address_zip"
  ORDER BY
      "pref_zips" asc
  LIMIT (50)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Combined daily sales (catalog + web) per sold date
…ek_seq + 53), 0), 2) as sat_ratio
where cs.sold_date.year = 2001 or cs.sold_date.year = 2002
order by cs.sold_date.week_seq asc nulls first
limit 60;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 12, column 70.
  Expected one of:
          * _TERMINATOR

  Location:
  ...y, 0) + coalesce(ws_daily, 0) ??? by cs.sold_date.date_sk;  sele...

  Write stats: received 2332 chars / 2332 bytes; tail: …'002\\norder by
  cs.sold_date.week_seq asc nulls first\\nlimit 60;'.
  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws select cs.sold_date.year, count(cs.order_number) as cnt_cs, sum(ws.ext_sales_price) as ws_sales group by 1 limit 10;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ....ext_sales_price) as ws_sales ??? group by 1 limit 10;
  ```
- `trilogy file write query02.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;

merge cs.item.item_sk into ~ws.item.item_sk;

# Combi…(prev_year_sales ? cs.sold_date.dow = 6), 0), 2) as sat_ratio
where cs.sold_date.year = 2001
order by cs.sold_date.week_seq asc nulls first
limit 60;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 14, column 79.
  Expected one of:
          * _TERMINATOR

  Location:
   coalesce(ws_daily_sales, 0)) ??? by cs.sold_date.week_seq, cs.s...

  Write stats: received 1757 chars / 1757 bytes; tail: …'001\\norder by
  cs.sold_date.week_seq asc nulls first\\nlimit 60;'.
  ```
- `trilogy file write query04.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge customer concep…lls first,
    ss.customer.first_name asc nulls first,
    ss.customer.last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;
`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 25, column 34.
  Expected one of:
          * _TERMINATOR

  Location:
  ...o has_all_2001 <- ss_2001 > 0 ??? and cs_2001 > 0 and ws_2001 >

  Write stats: received 2287 chars / 2287 bytes; tail: …'s first,\\n
  preferred_cust_flag asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query05.preql`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'import ') at line 2, column 1.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'ss')]

  Location:
  import raw.store_sales as ss ??? import raw.store_returns as sr...

  Write stats: received 798 chars / 798 bytes; tail: …"'::date) as
  profit\r\\norder by 1, 2\r\\nnulls first\r\\nlimit 100;\r\\n".
  ```
- `trilogy file write query05.preql --content import raw.store_sales as ss
import raw.store_returns as sr
import raw.catalog_sales as cs
import raw.catalog_retu…-09-06'::date) - sum(sr.net_loss ? sr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) as profit
order by 1, 2
nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'import ') at line 2, column 1.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'ss')]

  Location:
  import raw.store_sales as ss ??? import raw.store_returns as sr...

  Write stats: received 716 chars / 716 bytes; tail: …"09-06'::date) as
  profit\\norder by 1, 2\\nnulls first\\nlimit 100;".
  ```
- `trilogy file write query05.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…oss ? sr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date), 0) as profit
    group by 1, 2
) sub
order by 1, 2
nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'from ') at line 42, column 1.
  Expected one of:
          * COMMA
          * WHERE
          * _TERMINATOR
          * HAVING
          * ORDER
          * LIMIT
          * METADATA
          * MERGE
  Previous tokens: [Token('IDENTIFIER', 'total_profit')]

  Location:
    sum(profit) as total_profit ??? from (     select         'sto...

  Write stats: received 2970 chars / 2970 bytes; tail: …'   group by 1, 2\\n)
  sub\\norder by 1, 2\\nnulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…lesce(sum(sr.net_loss ? sr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date), 0) as profit
order by outlet asc
nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_BINDING', '->') at line 21, column 16.
  Expected one of:
          * LPAR
  Previous tokens: [Token('IDENTIFIER', 'st_metrics')]

  Location:
  ...acro for reuse def st_metrics ??? ->      sum(ss.ext_sales_price...

  Write stats: received 2717 chars / 2717 bytes; tail: …'e), 0) as profit\\norder
  by outlet asc\\nnulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…sum(sr.net_loss ? sr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date), 0) as total_profit
order by outlet asc
nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('CONCAT', 'concat') at line 23, column 109.
  Expected one of:
          * RPAR
  Previous tokens: [Token('LPAR', '(')]

  Location:
  ...2000-09-06'::date) by rollup ( ??? concat('store_', ss.store.stor...

  Write stats: received 1312 chars / 1312 bytes; tail: …' as total_profit\\norder
  by outlet asc\\nnulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store:store store.store_name, store.zip, substring(store.zip,1,2) limit 20`

  ```text
  --> 2:1
    |
  2 | store.store_name, store.zip, substring(store.zip,1,2) limit 20;
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  import raw.store as store; ??? store.store_name, store.zip, s...
  ```

### `undefined-concept`

- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['ss.customer.first_name']")
  ```
- `trilogy run query05.preql duckdb`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  wr.web_site.site_id. Suggestions: ['ws.web_site.site_id',
  'ws.web_site.site_sk', 'ws.web_site.mkt_id']")
  ```
- `trilogy run query05.preql duckdb`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  wr.web_site.site_id. Suggestions: ['ws.web_site.site_id',
  'ws.web_site.site_sk', 'ws.web_site.mkt_id']")
  ```
- `trilogy run query10.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  customer_address.county. Suggestions: ['store_sales.customer_address.county',
  'store_sales.customer_address.country', 'store_sales.customer_address.city']")
  ```

### `join-resolution`

- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws auto cs_wkly_dow <- sum(cs.ext_sales_price) by cs.sold_date.week_seq, cs.sold_date.dow; a…round(total_wkly_dow / nullif(next_total, 0), 2) as ratio where cs.sold_date.year between 2001 and 2002 and cs.sold_date.dow = 1 order by 1 limit 10;`

  ```text
  Could not resolve connections for query with output
  ['cs.sold_date.week_seq<Purpose.PROPERTY>Derivation.ROOT>',
  'cs.sold_date.dow<Purpose.PROPERTY>Derivation.ROOT>',
  'cs.sold_date.year<Purpose.PROPERTY>Derivation.ROOT>',
  'local.total_wkly_dow<Purpose.PROPERTY>Derivation.BASIC>',
  'local.next_total<Purpose.PROPERTY>Derivation.WINDOW>',
  'local.ratio<Purpose.PROPERTY>Derivation.BASIC>'] from current model. The
  output draws on models that are not connected in the current graph: cs (needed
  by cs.sold_date.dow, cs.sold_date.week_seq, cs.sold_date.year,
  local.next_total, local.ratio, local.total_wkly_dow); ws (needed by
  local.next_total, local.ratio, local.total_wkly_dow). If these should be
  related, bridge their keys with a merge, e.g. `merge cs.<key> into ~ws.<key>;`.
  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws select cs.sold_date.week_seq, cs.sold_date.dow, cs.sold_date.year, sum(cs.ext_sales_price…price) as ws_sales where cs.sold_date.year between 2001 and 2002 and cs.sold_date.dow = 1 and cs.sold_date.week_seq between 5270 and 5275 order by 1;`

  ```text
  Could not resolve connections for query with output
  ['cs.sold_date.week_seq<Purpose.PROPERTY>Derivation.ROOT>',
  'cs.sold_date.dow<Purpose.PROPERTY>Derivation.ROOT>',
  'cs.sold_date.year<Purpose.PROPERTY>Derivation.ROOT>',
  'local.cs_sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.ws_sales<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. The
  output draws on models that are not connected in the current graph: cs (needed
  by cs.sold_date.dow, cs.sold_date.week_seq, cs.sold_date.year, local.cs_sales,
  local.ws_sales); ws (needed by local.ws_sales). If these should be related,
  bridge their keys with a merge, e.g. `merge cs.<key> into ~ws.<key>;`.
  ```
- `trilogy run --import raw.web_sales:ws --import raw.web_returns:wr select count(wr.order_number) as ret_cnt, count(ws.order_number) as sale_cnt, sum(wr.return…re ws.sold_date.date between '2000-08-23'::date and '2000-09-06'::date and wr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date; duckdb`

  ```text
  Could not resolve connections for query with output
  ['local.ret_cnt<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.sale_cnt<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.total_ret_amt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  The output draws on models that are not connected in the current graph: wr
  (needed by local.ret_cnt, local.total_ret_amt); ws (needed by local.sale_cnt).
  If these should be related, bridge their keys with a merge, e.g. `merge
  wr.<key> into ~ws.<key>;`.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.web_returns:wr select count(wr.order_number) where wr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date; duckdb`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(wr.order_number) as
  order_number_count`
  Location:
  ...select count(wr.order_number) ??? where wr.date_dim.date between...
  ```

### `type-error`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Invalid argument type 'ArrayType<STRING>' passed into
  SUBSTRING function in position 1 from concept: local.param_zips. Valid:
  'STRING'.
  ```
