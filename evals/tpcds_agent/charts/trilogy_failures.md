# Trilogy failure analysis — 20260531-160146

- Run `20260531-160145_base` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 248 | failed: 26 (10%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 18 | 69% |
| `other` | 7 | 27% |
| `join-resolution` | 1 | 4% |

## Detail

### `syntax-parse`

- `trilogy file write query80.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Store channel metrics…nnel_tv = 'N'
        and cs.catalog_page.catalog_page_id is not null
)
order by channel_label nulls first, outlet_identifier nulls first
limit 100;
`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 12, column 33.
  Expected one of:
          * _TERMINATOR

  Location:
    by (concat('store channel')) ??? , (concat('store', ss.store.st...

  Write stats: received 2114 chars / 2114 bytes; tail: …'label nulls first,
  outlet_identifier nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query80.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

-- Store channel: compu…   and ss.item.current_price > 50
    and ss.promotion.channel_tv = 'N'
order by channel_label nulls first, outlet_identifier nulls first
limit 100;
`

  ```text
  …
  ON
          * PROPERTY
          * _DEF_TABLE
          * "merge"i
          * FROM
          * SHOW
          * RAW_SQL
          * SELF_IMPORT
          * IMPORT
          * AUTO
          * $END
          * CHART
          * DATASOURCE_ROOT
          * PERSIST_MODE
          * SELECT
          * MOCK
          * DEF
          * UNIQUE
          * SHORTHAND_MODIFIER
          * ROWSET
          * PARSE_COMMENT
          * COPY
          * _PROPERTIES
          * WITH
          * PARAMETER
          * DATASOURCE_PARTIAL
          * TYPE
          * PURPOSE
          * VALIDATE
          * CREATE
  Previous tokens: [Token('LINE_SEPARATOR', '\n\n')]

  Location:
   import raw.web_sales as ws;  ??? -- Store channel: compute per-...

  Write stats: received 2118 chars / 2118 bytes; tail: …'label nulls first,
  outlet_identifier nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query80_test.preql --content import raw.store_sales as ss;

select 'store channel' as channel_label,
    concat('store', ss.store.store_id…0-08-23'::date and '2000-09-22'::date
    and cs.item.current_price > 50
    and cs.promotion.channel_tv = 'N'
order by outlet_identifier;
limit 100;`

  ```text
  refused to write 'query80_test.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LIMIT', 'limit') at line 22, column 1.
  Expected one of:
          * _PROPERTIES
          * DEF
          * UNIQUE
          * CREATE
          * CHART
          * PUBLISH_ACTION
          * SELECT
          * WITH
          * _DEF_TABLE
          * ROWSET
          * SHORTHAND_MODIFIER
          * PARAM
          * DATASOURCE_ROOT
          * PROPERTY
          * $END
          * PERSIST_MODE
          * MOCK
          * SELF_IMPORT
          * VALIDATE
          * FROM
          * PARAMETER
          * PARSE_COMMENT
          * WHERE
          * PURPOSE
          * RAW_SQL
          * IMPORT
          * DATASOURCE_PARTIAL
          * SHOW
          * AUTO
          * DATASOURCE
          * "merge"i
          * TYPE
          * COPY
  Previous tokens: [Token('LINE_SEPARATOR', '\n')]

  Location:
  ...' order by outlet_identifier; ??? limit 100;

  Write stats: received 836 chars / 836 bytes; tail: …"tion.channel_tv =
  'N'\\norder by outlet_identifier;\\nlimit 100;".
  ```
- `trilogy file write query80.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge all fact tables…and ss.promotion.channel_tv = 'N'
    and ss.store.store_id is not null
order by channel_label nulls first, outlet_identifier nulls first
limit 100;
`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 25, column 57.
  Expected one of:
          * _TERMINATOR

  Location:
  ...t('store', ss.store.store_id) ??? by ss.store.store_id;  # Catal...

  Write stats: received 2764 chars / 2764 bytes; tail: …'label nulls first,
  outlet_identifier nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query80.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

# Link returns to sales via item_sk and ticket_numb…and ss.promotion.channel_tv = 'N'
    and ss.store.store_id is not null
order by channel_label nulls first, outlet_identifier nulls first
limit 100;
`

  ```text
  …
  ce > 50 and ss.promotion.channel_tv =
  'N' and ss.store.store_id is not null) - coalesce(@rollup_profit(sr.net_loss ?
  sr.date_dim.date between '2000-08-23'::date and '2000-09-22'::date), 0) as
  profit\nwhere ss.date_dim.date between '2000-08-23'::date and
  '2000-09-22'::date\n    and ss.item.current_price > 50\n    and
  ss.promotion.channel_tv = 'N'\n    and ss.store.store_id is not null\norder by
  channel_label nulls first, outlet_identifier nulls first\nlimit 100;\n") at
  line 12, column 51.
  Expected one of:
          * LPAR
          * IDENTIFIER
  Previous tokens: [Token('ROLLUP', 'rollup')]

  Location:
  ...ric) -> sum(metric) by rollup ??? 'store channel', concat('store...

  Write stats: received 1734 chars / 1734 bytes; tail: …'label nulls first,
  outlet_identifier nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query80_rollup_test.preql --content import raw.store_sales as ss;

select
    'store channel' as channel_label,
    concat('store', ss.sto…and ss.promotion.channel_tv = 'N'
    and ss.store.store_id is not null
order by channel_label nulls first, outlet_identifier nulls first
limit 100;
`

  ```text
  refused to write 'query80_rollup_test.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WILDCARD_IDENTIFIER', 'channel_label') at line 6,
  column 40.
  Expected one of:
          * RPAR
  Previous tokens: [Token('LPAR', '(')]

  Location:
  ...s.ext_sales_price) by rollup ( ??? channel_label), (outlet_identi...

  Write stats: received 482 chars / 482 bytes; tail: …'label nulls first,
  outlet_identifier nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query80_rollup_test.preql --content import raw.store_sales as ss;

auto ch_lbl <- 'store channel';
auto out_id <- concat('store', ss.store…and ss.promotion.channel_tv = 'N'
    and ss.store.store_id is not null
order by channel_label nulls first, outlet_identifier nulls first
limit 100;
`

  ```text
  refused to write 'query80_rollup_test.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 4, column 51.
  Expected one of:
          * _TERMINATOR

  Location:
  ...t('store', ss.store.store_id) ??? by ss.store.store_id;  select

  Write stats: received 531 chars / 531 bytes; tail: …'label nulls first,
  outlet_identifier nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query80.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…N'
    and ss.store.store_id is not null
having outlet_identifier is not null
order by channel nulls first, outlet_identifier nulls first
limit 100;
`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 36, column 55.
  Expected one of:
          * _TERMINATOR

  Location:
  ...alesce(sum(sr.return_amt), 0) ??? by sr.store.store_id; auto sto...

  Write stats: received 2983 chars / 2983 bytes; tail: …'annel nulls first,
  outlet_identifier nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run - --import raw.store_returns:store_returns`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ek_seq in (5244, 5257, 5264)  ??? group by store_returns.item.it...
  ```
- `trilogy file write query83.preql`

  ```text
  refused to write 'query83.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 13, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ..._seq in (5244, 5257, 5264)  ??? auto store_qty <- sum(sr.retur...

  Write stats: received 1108 chars / 1108 bytes; tail: …'tem.item_id nulls first,
  store_qty nulls first\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query83.preql`

  ```text
  refused to write 'query83.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'alias ') at line 13, column 1.
  Expected one of:
          * PARSE_COMMENT
          * ROWSET
          * PARAM
          * PERSIST_MODE
          * TYPE
          * UNIQUE
          * COPY
          * _PROPERTIES
          * "merge"i
          * DATASOURCE_PARTIAL
          * RAW_SQL
          * CREATE
          * DATASOURCE
          * WHERE
          * SELF_IMPORT
          * CHART
          * MOCK
          * PURPOSE
          * $END
          * SHOW
          * PROPERTY
          * SELECT
          * IMPORT
          * _DEF_TABLE
          * SHORTHAND_MODIFIER
          * PARAMETER
          * DEF
          * WITH
          * VALIDATE
          * FROM
          * DATASOURCE_ROOT
          * AUTO
          * PUBLISH_ACTION
  Previous tokens: [Token('LINE_SEPARATOR', '\r\n')]

  Location:
  ...eq in (5244, 5257, 5264));  ??? alias store_qty = coalesce(raw...

  Write stats: received 1556 chars / 1556 bytes; tail: …'tem.item_id nulls first,
  store_qty nulls first\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query85.preql -e -c import raw.web_sales as ws;
import raw.web_returns as wr;

# Merge the return order/item key into the sales order/item…ded_cash) as avg_refunded_cash,
    avg(wr.fee) as avg_return_fee
order by reason_desc_20, avg_sold_qty, avg_refunded_cash, avg_return_fee
limit 100;`

  ```text
  refused to write 'query85.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 15, column 45.
  Expected one of:
          * RPAR
          * COMMA

  Location:
  ...ed_cdemo.marital_status = 'M' ??? and wr.returning_cdemo.marital...

  Write stats: received 2205 chars / 2205 bytes; tail: …', avg_sold_qty,
  avg_refunded_cash, avg_return_fee\\nlimit 100;'.
  ```
- `trilogy file write query85.preql -e -c import raw.web_sales as ws;
import raw.web_returns as wr;

merge wr.order_number into ~ws.order_number;

auto reason_d…ded_cash) as avg_refunded_cash,
    avg(wr.fee) as avg_return_fee
order by reason_desc_20, avg_sold_qty, avg_refunded_cash, avg_return_fee
limit 100;`

  ```text
  refused to write 'query85.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 9, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...demo.marital_status = 'M'     ??? and wr.returning_cdemo.marital...

  Write stats: received 1836 chars / 1836 bytes; tail: …', avg_sold_qty,
  avg_refunded_cash, avg_return_fee\\nlimit 100;'.
  ```
- `trilogy file write query85.preql -e -c import raw.web_sales as ws;
import raw.web_returns as wr;
merge wr.order_number into ~ws.order_number;
auto reason_des…efunded_cash) as avg_refunded_cash, avg(wr.fee) as avg_return_fee
order by reason_desc_20, avg_sold_qty, avg_refunded_cash, avg_return_fee
limit 100;`

  ```text
  refused to write 'query85.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 5, column 55.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ed_cdemo.marital_status = 'M' ??? and wr.returning_cdemo.marital...

  Write stats: received 1731 chars / 1731 bytes; tail: …', avg_sold_qty,
  avg_refunded_cash, avg_return_fee\\nlimit 100;'.
  ```
- `trilogy file write query86.preql --content import raw.web_sales as web_sales;

where web_sales.sold_date.year = 2000

# define the rollup aggregate once
auto…der by rollup_net_paid desc
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
  ...ine the rollup aggregate once ??? auto rollup_net_paid <- sum(we...

  Write stats: received 885 chars / 885 bytes; tail: …'es.item.category nulls
  first,\\n    rnk nulls first\\nlimit 100;'.
  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Build a concatenated …00
  and ss_name_date not in cs_name_date
  and ss_name_date not in ws_name_date

select count_distinct(ss_name_date) as unique_customer_date_combos;`

  ```text
  refused to write 'query87.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 6, column 112.
  Expected one of:
          * _TERMINATOR

  Location:
  ...|', ss.date_dim.date::string) ??? where ss.date_dim.year = 2000;...

  Write stats: received 866 chars / 866 bytes; tail:
  …'count_distinct(ss_name_date) as unique_customer_date_combos;'.
  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Build a concatenated …00
  and ss_name_date not in cs_name_date
  and ss_name_date not in ws_name_date

select count_distinct(ss_name_date) as unique_customer_date_combos;`

  ```text
  refused to write 'query87.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 9, column 123.
  Expected one of:
          * _TERMINATOR

  Location:
  ...', cs.sold_date.date::string) ??? where cs.sold_date.year = 2000...

  Write stats: received 836 chars / 836 bytes; tail:
  …'count_distinct(ss_name_date) as unique_customer_date_combos;'.
  ```
- `trilogy file write query89.preql --content import raw.store_sales as store_sales;

# Filter: year 1999 and item category/class restrictions
where store_sales…ame asc,
    category asc,
    class asc,
    brand asc,
    store_company_name asc,
    month_of_year asc,
    total asc,
    average asc
limit 100;`

  ```text
  refused to write 'query89.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 11, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...per-unit price) by full grain ??? auto monthly_sales <- sum(stor...

  Write stats: received 1643 chars / 1643 bytes; tail: …'month_of_year asc,\\n
  total asc,\\n    average asc\\nlimit 100;'.
  ```

### `other`

- `trilogy run query80.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.store_return_amt', 'local.store_net_loss',
  'local.store_net_profit'} out of  with found {'ss.store.store_id',
  'local.store_sales_amt', 'local.store_net_profit'}
  ```
- `trilogy run query80.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.store_net_loss', 'local.store_net_profit',
  'local.store_return_amt'} out of  with found {'local.store_sales_amt',
  'local.store_net_profit', 'ss.store.store_id'}
  ```
- `trilogy run query80.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local._virt_agg_sum_6174694630167103', 'local.sales',
  'local._virt_agg_sum_4582371212002110', 'local._virt_agg_sum_2856259540536641'}
  out of  with found {'local.sales', 'local.outlet_identifier'}
  ```
- `trilogy run query81.preql`

  ```text
  HAVING references 'local.cust_state_total',
  'local.state_avg', which are not in the SELECT projection (line 7). Add them to
  SELECT, each prefixed with `--` so they stay out of the output rows — keep your
  HAVING as-is:
      select <your existing columns>, --local.cust_state_total, --local.state_avg
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run - --import raw.store_returns:sr --import raw.catalog_returns:cr --import raw.web_returns:wr`

  ```text
  Cannot reference an aggregate derived in the select
  (local.sr_qty) in the same statement where clause; move to the HAVING clause
  instead; Line: 7
  ```
- `trilogy run query83.preql`

  ```text
  HAVING references 'local.has_store_return',
  'local.has_catalog_return', 'local.has_web_return', which are not in the SELECT
  projection (line 18). Add them to SELECT, each prefixed with `--` so they stay
  out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.has_store_return,
  --local.has_catalog_return, --local.has_web_return
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query88.preql`

  ```text
  Value 6 is not valid for enum field
  'store_sales.customer.household_demographics.vehicle_count'. Allowed values:
  -1, 0, 1, 2, 3, 4.
  ```

### `join-resolution`

- `trilogy run - --import raw.item:item --import raw.store_returns:store_returns`

  ```text
  Could not resolve connections for query with output
  ['item.item_id<Purpose.PROPERTY>Derivation.ROOT>',
  'local.sq<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
