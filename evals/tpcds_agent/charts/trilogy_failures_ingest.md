# Trilogy failure analysis — 20260601-143003

- Run `20260601-143003_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 282 | failed: 43 (15%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 23 | 53% |
| `syntax-missing-alias` | 8 | 19% |
| `other` | 5 | 12% |
| `join-resolution` | 4 | 9% |
| `undefined-concept` | 2 | 5% |
| `type-error` | 1 | 2% |

## Detail

### `syntax-parse`

- `trilogy run --import raw.catalog_sales:cs select cs.sold_date.week_seq, cs.sold_date.year, sum(cs.ext_sales_price) as total where cs.sold_date.year in (2001,2002) group by cs.sold_date.week_seq, cs.sold_date.year limit 20;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...sold_date.year in (2001,2002) ??? group by cs.sold_date.week_seq...
  ```
- `trilogy file write query02.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Bridge the two fact tables through their shared dat…as sat_sales_combined
where cs.sold_date.year = 2001
    and cs.ext_sales_price is not null
order by cs.sold_date.week_seq asc nulls first
limit 100;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 9, column 116.
  Expected one of:
          * _TERMINATOR

  Location:
  ...e.week_seq, cs.sold_date.dow) ??? where cs.sold_date.year = 2001...

  Write stats: received 1954 chars / 1954 bytes; tail: …'ll\\norder by
  cs.sold_date.week_seq asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query02.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Bridge the two fact tables through their shared dat… and cs.ext_sales_price is not null

select
    cs.sold_date.week_seq,
    daily_sales_2001
order by cs.sold_date.week_seq asc nulls first
limit 100;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 15, column 44.
  Expected one of:
          * _TERMINATOR

  Location:
  ...02_mapped <- daily_sales_2002 ??? by cs.sold_date.week_seq, cs.s...

  Write stats: received 935 chars / 935 bytes; tail: …'01\\norder by
  cs.sold_date.week_seq asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query02.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;

merge cs.sold_date.date_sk into ~ws.sold_date.date_sk…o,
    fri_2002 / nullif(fri_2001, 0) as fri_ratio,
    sat_2002 / nullif(sat_2001, 0) as sat_ratio
order by week_seq_2001 asc nulls first
limit 100;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 10, column 113.
  Expected one of:
          * _TERMINATOR

  Location:
  ...0) by (cs.sold_date.week_seq) ??? where cs.sold_date.year = 2001...

  Write stats: received 2994 chars / 2994 bytes; tail: …' sat_ratio\\norder by
  week_seq_2001 asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws --import raw.date_dim:d merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; merge d.da…cs.order_number) as cs_cnt, count(ws.order_number) as ws_cnt where d.year in (2001,2002) and cs.ext_sales_price is not null group by d.year limit 10;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...s.ext_sales_price is not null ??? group by d.year limit 10;
  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; select cs.sold_date.week_seq, count(*) as cnt where cs.sold_date.year = 2002 and cs.ext_sales_price is not null limit 5;`

  ```text
  --> 3:92
    |
  3 | merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; select
  cs.sold_date.week_seq, count(*) as cnt where cs.sold_date.year = 2002 and
  cs.ext_sales_price is not null limit 5;
    |
  ^---
    |
    = expected access_chain
  Location:
   cs.sold_date.week_seq, count( ??? *) as cnt where cs.sold_date.y...
  ```
- `trilogy file write query04.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge customer keys a…omer.first_name asc nulls first,
         ss.customer.last_name asc nulls first,
         ss.customer.preferred_cust_flag asc nulls first

limit 100;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 13, column 122.
  Expected one of:
          * RPAR

  Location:
  ...t) + ss.ext_sales_price) / 2) ??? by ss.customer.customer_sk, ss...

  Write stats: received 2507 chars / 2507 bytes; tail: …'
  ss.customer.preferred_cust_flag asc nulls first\\n\\nlimit 100;'.
  ```
- `trilogy file write query04.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge customer keys a…omer.first_name asc nulls first,
         ss.customer.last_name asc nulls first,
         ss.customer.preferred_cust_flag asc nulls first

limit 100;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 31, column 1.
  Expected one of:
          * SELECT

  Location:
  ...001 > 0  # Ratios (2002/2001) ??? auto store_ratio <- ss_2002 /

  Write stats: received 2179 chars / 2179 bytes; tail: …'
  ss.customer.preferred_cust_flag asc nulls first\\n\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content # Query 5: Rolled-up summary across three sales channels
# For date range 2000-08-23 to 2000-09-06 inclusive

impo…' as channel,
    concat('store_', ss.store.store_id) as outlet,
    st_sales as sales,
    st_returns as returns,
    st_profit as profit
limit 100;`

  ```text
  …
  ate) -
  sum(wr.net_loss ? wr.date_dim.date between '2000-08-23'::date and
  '2000-09-06'::date);\n\n# Grand total\nauto gt_sales <- st_ch_sales +
  ca_ch_sales + we_ch_sales;\nauto gt_returns <- st_ch_returns + ca_ch_returns +
  we_ch_returns;\nauto gt_profit <- st_ch_profit + ca_ch_profit +
  we_ch_profit;\n\nselect\n    'store' as channel,\n    concat('store_',
  ss.store.store_id) as outlet,\n    st_sales as sales,\n    st_returns as
  returns,\n    st_profit as profit\nlimit 100;") at line 20, column 120.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'concat')]

  Location:
   '2000-09-06'::date) by concat ??? ('store_', ss.store.store_id);...

  Write stats: received 4200 chars / 4200 bytes; tail: …'   st_returns as
  returns,\\n    st_profit as profit\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content # Query 5: Store channel - rolled-up summary with rollup
# Date range: 2000-08-23 to 2000-09-06

import raw.store_…    @rollup_sum(sr.return_amt) as returns,
    @rollup_sum(ss.net_profit) - @rollup_sum(sr.net_loss) as profit
order by outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'def ') at line 12, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...:date and '2000-09-06'::date  ??? def rollup_sum(m) -> sum(m) by...

  Write stats: received 680 chars / 680 bytes; tail: …'r.net_loss) as
  profit\\norder by outlet nulls first\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content # Query 5: Store channel - rolled-up summary with rollup
# Date range: 2000-08-23 to 2000-09-06

import raw.store_…    @rollup_sum(sr.return_amt) as returns,
    @rollup_sum(ss.net_profit) - @rollup_sum(sr.net_loss) as profit
order by outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', "('store_',
  ss.store.store_id);\n\nwhere ss.date_dim.date between '2000-08-23'::date and
  '2000-09-06'::date\n\nselect\n    'store' as channel,\n    concat('store_',
  ss.store.store_id) as outlet,\n    @rollup_sum(ss.ext_sales_price) as sales,\n
  @rollup_sum(sr.return_amt) as returns,\n    @rollup_sum(ss.net_profit) -
  @rollup_sum(sr.net_loss) as profit\norder by outlet nulls first\nlimit 100;")
  at line 10, column 45.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'concat')]

  Location:
  ...(m) -> sum(m) by rollup concat ??? ('store_', ss.store.store_id);...

  Write stats: received 680 chars / 680 bytes; tail: …'r.net_loss) as
  profit\\norder by outlet nulls first\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content # Query 5: Store channel - rolled-up summary with rollup
# Date range: 2000-08-23 to 2000-09-06

import raw.store_…    @rollup_sum(sr.return_amt) as returns,
    @rollup_sum(ss.net_profit) - @rollup_sum(sr.net_loss) as profit
order by outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('CONCAT', 'concat') at line 10, column 40.
  Expected one of:
          * RPAR
  Previous tokens: [Token('LPAR', '(')]

  Location:
  ...p_sum(m) -> sum(m) by rollup ( ??? concat('store_', ss.store.stor...

  Write stats: received 682 chars / 682 bytes; tail: …'r.net_loss) as
  profit\\norder by outlet nulls first\\nlimit 100;'.
  ```
- `trilogy file write query08.preql --content parameter zips string;

# Set B: ZIP codes where more than 10 preferred customers live
auto preferred_cust_count <…    )
)
select
    store_sales.store.store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 5, column 57.
  Expected one of:
          * _TERMINATOR

  Location:
  ...customer.customer_address.zip ??? where preferred_cust_count > 1...

  Write stats: received 824 chars / 824 bytes; tail: …'_net_profit\\norder by
  store_sales.store.store_name\\nlimit 100;'.
  ```
- `trilogy file write query08.preql --content parameter zips string;

# Parameter ZIPs as an array
auto param_zips_arr <- split(zips, ',');

# Preferred custome…_zips
)
select
    store_sales.store.store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 10, column 54.
  Expected one of:
          * _TERMINATOR

  Location:
  ...customer.customer_address.zip ??? where cust_preferred_count > 1...

  Write stats: received 849 chars / 849 bytes; tail: …'_net_profit\\norder by
  store_sales.store.store_name\\nlimit 100;'.
  ```
- `trilogy file write query08.preql --content parameter zips string;

# Set (a): parameter ZIPs as array
auto param_zips_arr <- split(zips, ',');

# Set (b): ZI…qoy = 2
select
    store_sales.store.store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 8, column 54.
  Expected one of:
          * _TERMINATOR

  Location:
  ...customer.customer_address.zip ??? where pref_count > 10;  # Now:...

  Write stats: received 781 chars / 781 bytes; tail: …'_net_profit\\norder by
  store_sales.store.store_name\\nlimit 100;'.
  ```
- `trilogy file write query08.preql -e -c parameter zips string;

# Compute: for each ZIP prefix, count how many preferred customer ZIPs
# (with >10 pref custom…_prefix
select
    store_sales.store.store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 13, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...omer.customer_address.zip     ??? where pref_zip_count > 10

  Write stats: received 992 chars / 992 bytes; tail: …'_net_profit\\norder by
  store_sales.store.store_name\\nlimit 100;'.
  ```
- `trilogy run --param zips=31904 --import raw.store_sales:ss select ss.store.store_name, count(ss.ticket_number) as cnt where ss.date_dim.year=1998 and ss.date_dim.qoy=2 group by ss.store.store_name limit 5;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ar=1998 and ss.date_dim.qoy=2 ??? group by ss.store.store_name l...
  ```
- `trilogy file write query08.preql -e -c parameter zips string;

# Set B: preferred customer count per ZIP
auto pref_zip_count <- count(customer.customer_sk ? …,)'))
)
select
    store_sales.store.store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 14, column 5.
  Expected one of:
          * COMMA
          * RPAR

  Location:
  ...stomer_address.zip, 1, 2)     ??? where pref_zip_count > 10

  Write stats: received 783 chars / 783 bytes; tail: …'_net_profit\\norder by
  store_sales.store.store_name\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.store_name, ss.store.zip, count(ss.ticket_number) as cnt where ss.date_dim.year=1998 and ss.date_dim.qoy=2 group by ss.store.store_name, ss.store.zip order by ss.store.store_name limit 20;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ar=1998 and ss.date_dim.qoy=2 ??? group by ss.store.store_name,
  ```
- `trilogy file write query08.preql -e -c import raw.store_sales as store_sales;

parameter zips string;

# Preferred customer count per customer ZIP
auto pref_…_prefix
select
    store_sales.store.store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 10, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...omer.customer_address.zip     ??? where pref_cnt > 10     and re...

  Write stats: received 892 chars / 892 bytes; tail: …'_net_profit\\norder by
  store_sales.store.store_name\\nlimit 100;'.
  ```
- `trilogy file write query08.preql -e -c import raw.store_sales as store_sales;

parameter zips string;

# Set B: preferred customer count per ZIP
auto pref_co… > 10
)
select
    store_sales.store.store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 19, column 64.
  Expected one of:
          * COMMA
          * RPAR

  Location:
  ...r.customer_address.zip, 1, 2) ??? where valid_prefix_check > 10

  Write stats: received 1144 chars / 1144 bytes; tail: …'_net_profit\\norder by
  store_sales.store.store_name\\nlimit 100;'.
  ```
- `trilogy run --param zips=31387 --import raw.store_sales:ss select ss.store.store_name, ss.store.zip, sum(ss.net_profit) as profit, count(*) as row_count where ss.date_dim.year=1998 and ss.date_dim.qoy=2 and ss.store.zip is null group by ss.store.store_name, ss.store.zip;`

  ```text
  --> 2:79
    |
  2 | select ss.store.store_name, ss.store.zip, sum(ss.net_profit) as profit,
  count(*) as row_count where ss.date_dim.year=1998 and ss.date_dim.qoy=2 and
  ss.store.zip is null group by ss.store.store_name, ss.store.zip;
    |
  ^---
    |
    = expected access_chain
  Location:
  ....net_profit) as profit, count( ??? *) as row_count where ss.date_...
  ```
- `trilogy file write query10.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sale…dep_count,
  store_sales.customer.customer_demographics.dep_employed_count,
  store_sales.customer.customer_demographics.dep_college_count
limit 100;`

  ```text
  refused to write 'query10.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 10, column 5.
  Expected one of:
          * COMMA
          * RPAR

  Location:
  ...bill_customer.customer_sk     ??? where web_sales.sold_date.year...

  Write stats: received 1990 chars / 1990 bytes; tail:
  …'.customer.customer_demographics.dep_college_count\\nlimit 100;'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.catalog_sales:cs select cs.sold_date.year, count(cs.order_number) limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(cs.order_number) as
  order_number_count`
  Location:
  ....year, count(cs.order_number) ??? limit 10;
  ```
- `trilogy run --import raw.web_sales:ws select ws.sold_date.year, count(ws.order_number) limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(ws.order_number) as
  order_number_count`
  Location:
  ....year, count(ws.order_number) ??? limit 10;
  ```
- `trilogy file write query05.preql --content # Query 5: Store channel - rolled-up summary
# Date range: 2000-08-23 to 2000-09-06
# Combines store_sales and sto… by rollup concat('store_', ss.store.store_id) as returns,
    sum(ss.net_profit) - sum(sr.net_loss) as profit
order by outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `sum(ss.net_profit) - sum(sr.net_loss) as net_loss_sum`
  Location:
   outlet nulls first limit 100; ???

  Write stats: received 772 chars / 772 bytes; tail: …'r.net_loss) as
  profit\\norder by outlet nulls first\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content # Query 5: Store channel - rolled-up summary with rollup
# Date range: 2000-08-23 to 2000-09-06
# Combines store_s…ss) by rollup concat('store_', ss.store.store_id) as net_loss_sum,
    net_profit_sum - net_loss_sum as profit
order by outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `net_profit_sum - net_loss_sum as net_profit_sum_net_loss_sum`
  Location:
   outlet nulls first limit 100; ???

  Write stats: received 869 chars / 869 bytes; tail: …'et_loss_sum as
  profit\\norder by outlet nulls first\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content # Query 5: Store channel - rolled-up summary with rollup
# Date range: 2000-08-23 to 2000-09-06
# Combines store_s…et_loss_sum,
    --net_profit_sum - net_loss_sum as profit
    sum(ss.net_profit) - sum(sr.net_loss) as profit
order by outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `net_profit_sum - net_loss_sum as net_profit_sum_net_loss_sum`
  Location:
   outlet nulls first limit 100; ???

  Write stats: received 998 chars / 998 bytes; tail: …'r.net_loss) as
  profit\\norder by outlet nulls first\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content # Query 5: Store channel - rolled-up summary with rollup
# Date range: 2000-08-23 to 2000-09-06

import raw.store_… by rollup concat('store_', ss.store.store_id) as returns,
    sum(ss.net_profit) - sum(sr.net_loss) as profit
order by outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `sum(ss.net_profit) - sum(sr.net_loss) as net_loss_sum`
  Location:
   outlet nulls first limit 100; ???

  Write stats: received 664 chars / 664 bytes; tail: …'r.net_loss) as
  profit\\norder by outlet nulls first\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content # Query 5: Store channel - rolled-up summary with rollup
# Date range: 2000-08-23 to 2000-09-06

import raw.store_…ore_id) as np,
    sum(sr.net_loss) by rollup concat('store_', ss.store.store_id) as nl,
    np - nl as profit
order by outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `np - nl as np_nl`
  Location:
   outlet nulls first limit 100; ???

  Write stats: received 784 chars / 784 bytes; tail: …'    np - nl as
  profit\\norder by outlet nulls first\\nlimit 100;'.
  ```
- `trilogy run --param zips=31904 --import raw.store_sales:ss --import raw.customer_address:ca --import raw.customer:c select ss.store.store_name, count(ss.ticket_number) where ss.date_dim.year=1998 and ss.date_dim.qoy=2 limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(ss.ticket_number) as
  ticket_number_count`
  Location:
  ...name, count(ss.ticket_number) ??? where ss.date_dim.year=1998 an...
  ```

### `other`

- `trilogy run query01.preql`

  ```text
  HAVING references 'local.cust_store_total',
  'local.store_avg', which are not in the SELECT projection (line 6). Add them to
  SELECT, each prefixed with `--` so they stay out of the output rows — keep your
  HAVING as-is:
      select <your existing columns>, --local.cust_store_total, --local.store_avg
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query02.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 124:     INVALID_REFERENCE_BUG_<Missing source reference to
  cs.sold_date.year> = 2001
                                               ^
  [SQL:
  WITH
  highfalutin as (
  SELECT
      "cs_catalog_sales"."cs_ext_sales_price" as "cs_ext_sales_price",
      "cs_catalog_sales"."cs_sold_date_sk" as "ws_sold_date_date_sk"
  FROM
      "catalog_sales" as "cs_catalog_sales"
  WHERE
      "cs_catalog_sales"."cs_ext_sales_price" is not null

  GROUP BY
      1,
      2),
  questionable as (
  SELECT
      "cs_catalog_sales"."cs_ext_sales_price" as "cs_ex
  …
  i_2001" as "fri_2001",
      "concerned"."sat_2001" as "sat_2001",
      "concerned"."sun_2002" as "sun_2002",
      "concerned"."mon_2002" as "mon_2002",
      "concerned"."tue_2002" as "tue_2002",
      "concerned"."wed_2002" as "wed_2002",
      "concerned"."thu_2002" as "thu_2002",
      "concerned"."fri_2002" as "fri_2002",
      "concerned"."sat_2002" as "sat_2002"
  FROM
      "concerned"
  WHERE
      INVALID_REFERENCE_BUG_<Missing source reference to cs.sold_date.year> =
  2001

  ORDER BY
      "concerned"."week_seq_2001" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query02.preql`

  ```text
  Multiple where clauses are not supported
  ```
- `trilogy run --import raw.store:store --import raw.customer:customer --import raw.customer_address:ca select store.store_name, store.zip, count(customer.customer_sk ? customer.preferred_cust_flag = 'Y') by customer.customer_address.zip as pref_cnt, '31904' in split('24128,76232,31904', ',') as test;`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 14:     '31904' in (select INVALID_REFERENCE_BUG_<Missing source reference
  to local._virt_func_split_8165763218718135...
                                                                 ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "store_store"."s_store_name" as "store_store_name",
      "store_store"."s_zip" as "store_zip"
  FROM
      "store" as "store_store"
  GROUP BY
      1,
      2),
  wakeful as (
  SELECT
      '31904' in (select INVALID_REFERENCE_BUG_<Missing source reference to
  local._virt_func_split_8165763218718135>."_virt
  …
  E_BUG_<Missing source reference to
  local._virt_func_split_8165763218718135> where INVALID_REFERENCE_BUG_<Missing
  source reference to
  local._virt_func_split_8165763218718135>."_virt_func_split_8165763218718135" is
  not null) as "test"
  FROM
      "thoughtful")
  SELECT
      "questionable"."store_store_name" as "store_store_name",
      "questionable"."store_zip" as "store_zip",
      "wakeful"."pref_cnt" as "pref_cnt",
      "wakeful"."test" as "test"
  FROM
      "questionable"
      INNER JOIN "wakeful" on "questionable"."test" = "wakeful"."test"]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,28577,55565,17183,…910,16807,17871,35258,31387,35458,35576', concat('(^|,)', customer.customer_address.zip, '($|,)')) and pref_cnt > 10 order by pref_cnt desc limit 10;`

  ```text
  Cannot reference an aggregate derived in the select
  (local.pref_cnt) in the same statement where clause; move to the HAVING clause
  instead; Line: 3
  ```

### `join-resolution`

- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws select cs.sold_date.week_seq, cs.sold_date.dow, sum(cs.ext_sales_price) as cs_sales, sum(ws.ext_sales_price) as ws_sales where cs.sold_date.year = 2001 and cs.ext_sales_price is not null limit 20;`

  ```text
  Could not resolve connections for query with output
  ['cs.sold_date.week_seq<Purpose.PROPERTY>Derivation.ROOT>',
  'cs.sold_date.dow<Purpose.PROPERTY>Derivation.ROOT>',
  'local.cs_sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.ws_sales<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. The
  output draws on models that are not connected in the current graph: cs (needed
  by cs.sold_date.dow, cs.sold_date.week_seq, local.cs_sales, local.ws_sales); ws
  (needed by local.ws_sales). If these should be related, bridge their keys with
  a merge, e.g. `merge cs.<key> into ~ws.<key>;`.
  ```
- `trilogy run query05.preql`

  ```text
  Could not resolve connections for query with output
  ['local.channel<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.outlet<Purpose.PROPERTY>Derivation.BASIC>',
  'local.sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.returns<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.profit<Purpose.METRIC>Derivation.BASIC>'] from current model. The output
  draws on models that are not connected in the current graph: local (needed by
  local.channel); sr (needed by local.profit, local.returns); ss (needed by
  local.outlet, local.profit, local.returns, local.sales). If these should be
  related, bridge their keys with a merge, e.g. `merge local.<key> into
  ~sr.<key>;`.
  ```
- `trilogy run query05.preql`

  ```text
  Could not resolve connections for query with output
  ['local.channel<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.outlet<Purpose.PROPERTY>Derivation.BASIC>',
  'local.sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.returns<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.profit<Purpose.METRIC>Derivation.BASIC>'] from current model. The output
  draws on models that are not connected in the current graph: local (needed by
  local.channel); sr (needed by local.profit, local.returns); ss (needed by
  local.outlet, local.profit, local.returns, local.sales). If these should be
  related, bridge their keys with a merge, e.g. `merge local.<key> into
  ~sr.<key>;`.
  ```
- `trilogy run query05.preql`

  ```text
  Could not resolve connections for query with output
  ['local.channel<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.outlet<Purpose.PROPERTY>Derivation.BASIC>',
  'local.sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.returns<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.profit<Purpose.METRIC>Derivation.BASIC>'] from current model. The output
  draws on models that are not connected in the current graph: local (needed by
  local.channel); sr (needed by local.profit, local.returns); ss (needed by
  local.outlet, local.profit, local.returns, local.sales). If these should be
  related, bridge their keys with a merge, e.g. `merge local.<key> into
  ~sr.<key>;`.
  ```

### `undefined-concept`

- `trilogy run --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,28577,55565,17183,…,58048,56910,16807,17871,35258,31387,35458,35576', concat('(^|,)', customer.customer_address.zip, '($|,)')) and pref_cnt > 10 order by pref_cnt desc;`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  customer.customer_sk.')
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  customer.customer_sk.')
  ```

### `type-error`

- `trilogy run --param zips=31387,99999 select contains(split('31387,99999',','), '31387') as test1, contains(split('31387,99999',','), '00000') as test2;`

  ```text
  Invalid argument type ArrayType<STRING>' passed into CONTAINS
  function from function SPLIT in position 1. Valid: 'STRING'
  ```
