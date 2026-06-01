# Trilogy failure analysis — 20260601-025818

- Run `20260601-025817_base` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 205 | failed: 24 (12%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 10 | 42% |
| `syntax-parse` | 7 | 29% |
| `join-resolution` | 3 | 12% |
| `syntax-missing-alias` | 2 | 8% |
| `cli-misuse` | 1 | 4% |
| `undefined-concept` | 1 | 4% |

## Detail

### `other`

- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 58 column 12 (char 2682). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query23.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "aggregate"

  LINE 57:     INVALID_REFERENCE_BUG_<Cannot render aggregate
  local._virt_agg_sum_1078571684761268 in CTE uneven...
                                                    ^
  [SQL:
  WITH
  young as (
  SELECT
      "web_sales_web_sales"."ws_bill_customer_sk" as
  "web_sales_bill_customer_customer_sk",
      "web_sales_web_sales"."ws_item_sk" as "web_sales_item_item_sk"
  FROM
      "web_sales" as "web_sales_web_sales"
  GROUP BY
      1,
      2),
  late as (
  SELECT
      "store_sales_store_sales"."ss_customer_sk" as
  "web_sales_bill_customer_cu
  …
  es_catalog_sales"."cs_item_sk","web_sales_item_item"."
  i_item_sk","young"."web_sales_item_item_sk"))
  SELECT
      $1 as "channel",
      "rambunctious"."catalog_sales_bill_customer_last_name" as "last_name",
      "rambunctious"."catalog_sales_bill_customer_first_name" as "first_name",
      sum("rambunctious"."catalog_sales_quantity" *
  "rambunctious"."catalog_sales_list_price") as "total_sales"
  FROM
      "rambunctious"
  GROUP BY
      2,
      3
  ORDER BY
      "last_name" asc,
      "first_name" asc,
      "total_sales" asc nulls first
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query23.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "aggregate"

  LINE 63:     INVALID_REFERENCE_BUG_<Cannot render aggregate
  local._virt_agg_sum_1078571684761268 in CTE yummy...
                                                    ^
  [SQL:
  WITH
  friendly as (
  SELECT
      "web_sales_web_sales"."ws_bill_customer_sk" as
  "web_sales_bill_customer_customer_sk",
      "web_sales_web_sales"."ws_item_sk" as "web_sales_item_item_sk"
  FROM
      "web_sales" as "web_sales_web_sales"
  GROUP BY
      1,
      2),
  late as (
  SELECT
      "store_sales_store_sales"."ss_customer_sk" as
  "web_sales_bill_customer_
  …
  log_sales"."cs_item_sk","catalog_sales_item_ite
  m"."i_item_sk","friendly"."web_sales_item_item_sk","web_sales_item_item"."i_ite
  m_sk"))
  SELECT
      $1 as "channel",
      "puffy"."catalog_sales_bill_customer_last_name" as "last_name",
      "puffy"."catalog_sales_bill_customer_first_name" as "first_name",
      sum("puffy"."catalog_sales_quantity" * "puffy"."catalog_sales_list_price")
  as "total_sales"
  FROM
      "puffy"
  GROUP BY
      2,
      3
  ORDER BY
      "last_name" asc,
      "first_name" asc,
      "total_sales" asc nulls first
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query23.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "aggregate"

  LINE 63:     INVALID_REFERENCE_BUG_<Cannot render aggregate
  local._virt_agg_sum_4430872772101693 in CTE yummy...
                                                    ^
  [SQL:
  WITH
  friendly as (
  SELECT
      "ws_web_sales"."ws_bill_customer_sk" as "ws_bill_customer_customer_sk",
      "ws_web_sales"."ws_item_sk" as "ws_item_item_sk"
  FROM
      "web_sales" as "ws_web_sales"
  GROUP BY
      1,
      2),
  late as (
  SELECT
      "ss_store_sales"."ss_customer_sk" as "ws_bill_customer_customer_sk",
      "ss_store_sales"."ss_sold_date_sk"
  …

      4,
      "cs_catalog_sales"."cs_order_number",
      coalesce("cs_catalog_sales"."cs_item_sk","cs_item_item"."i_item_sk","friend
  ly"."ws_item_item_sk","ws_item_item"."i_item_sk"))
  SELECT
      $1 as "channel",
      "puffy"."cs_bill_customer_last_name" as "last_name",
      "puffy"."cs_bill_customer_first_name" as "first_name",
      sum("puffy"."cs_quantity" * "puffy"."cs_list_price") as "total_sales"
  FROM
      "puffy"
  GROUP BY
      2,
      3
  ORDER BY
      "last_name" asc,
      "first_name" asc,
      "total_sales" asc nulls first
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query23.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "aggregate"

  LINE 48:     INVALID_REFERENCE_BUG_<Cannot render aggregate
  local._virt_agg_sum_4430872772101693 in CTE macho...
                                                    ^
  [SQL:
  WITH
  late as (
  SELECT
      "ss_store_sales"."ss_item_sk" as "cs_item_item_sk",
      "ss_store_sales"."ss_sold_date_sk" as "ss_date_dim_date_sk"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1,
      2),
  uneven as (
  SELECT
      "ss_store_sales"."ss_customer_sk" as "cs_bill_customer_customer_sk",
      "ss_store_sales"."ss_sold_date_sk" as "ss_
  …
  ing where sparkling."bc" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "cs_catalog_sales"."cs_item_sk",
      "cs_catalog_sales"."cs_order_number")
  SELECT
      $1 as "channel",
      "protective"."cs_bill_customer_last_name" as "last_name",
      "protective"."cs_bill_customer_first_name" as "first_name",
      sum("protective"."cs_quantity" * "protective"."cs_list_price") as
  "total_sales"
  FROM
      "protective"
  GROUP BY
      2,
      3
  ORDER BY
      "last_name" asc,
      "first_name" asc,
      "total_sales" asc nulls first
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query23.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "aggregate"

  LINE 37:     INVALID_REFERENCE_BUG_<Cannot render aggregate
  local._virt_agg_sum_4430872772101693 in CTE scrawny...
                                                    ^
  [SQL:
  WITH
  macho as (
  SELECT
      "ss_store_sales"."ss_item_sk" as "ss_item_item_sk",
      "ss_store_sales"."ss_sold_date_sk" as "ss_date_dim_date_sk"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1,
      2),
  abundant as (
  SELECT
      "ss_store_sales"."ss_customer_sk" as "ss_customer_customer_sk",
      "ss_store_sales"."ss_sold_date_sk" as "ss_
  …
  bc" from
  abhorrent where abhorrent."bc" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "cs_catalog_sales"."cs_item_sk",
      "cs_catalog_sales"."cs_order_number")
  SELECT
      $1 as "channel",
      "premium"."cs_bill_customer_last_name" as "last_name",
      "premium"."cs_bill_customer_first_name" as "first_name",
      sum("premium"."cs_quantity" * "premium"."cs_list_price") as "total_sales"
  FROM
      "premium"
  GROUP BY
      2,
      3
  ORDER BY
      "last_name" asc,
      "first_name" asc,
      "total_sales" asc nulls first
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query23.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "aggregate"

  LINE 37:     INVALID_REFERENCE_BUG_<Cannot render aggregate
  local._virt_agg_count_2956982007116247 in CTE...
                                                    ^
  [SQL:
  WITH
  macho as (
  SELECT
      "ss_store_sales"."ss_item_sk" as "ss_item_item_sk",
      "ss_store_sales"."ss_sold_date_sk" as "ss_date_dim_date_sk"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1,
      2),
  abundant as (
  SELECT
      "ss_store_sales"."ss_customer_sk" as "ss_customer_customer_sk",
      "ss_store_sales"."ss_sold_date_sk" as "ss_date_d
  …
  bc" from
  abhorrent where abhorrent."bc" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "cs_catalog_sales"."cs_item_sk",
      "cs_catalog_sales"."cs_order_number")
  SELECT
      $1 as "channel",
      "premium"."cs_bill_customer_last_name" as "last_name",
      "premium"."cs_bill_customer_first_name" as "first_name",
      sum("premium"."cs_quantity" * "premium"."cs_list_price") as "total_sales"
  FROM
      "premium"
  GROUP BY
      2,
      3
  ORDER BY
      "last_name" asc,
      "first_name" asc,
      "total_sales" asc nulls first
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query23.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "aggregate"

  LINE 37:     INVALID_REFERENCE_BUG_<Cannot render aggregate
  local._virt_agg_count_8576311613180931 in CTE...
                                                    ^
  [SQL:
  WITH
  macho as (
  SELECT
      "ss_store_sales"."ss_item_sk" as "ss_item_item_sk",
      "ss_store_sales"."ss_sold_date_sk" as "ss_date_dim_date_sk"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1,
      2),
  abundant as (
  SELECT
      "ss_store_sales"."ss_customer_sk" as "ss_customer_customer_sk",
      "ss_store_sales"."ss_sold_date_sk" as "ss_date_d
  …
  bc" from
  abhorrent where abhorrent."bc" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "cs_catalog_sales"."cs_item_sk",
      "cs_catalog_sales"."cs_order_number")
  SELECT
      $1 as "channel",
      "premium"."cs_bill_customer_last_name" as "last_name",
      "premium"."cs_bill_customer_first_name" as "first_name",
      sum("premium"."cs_quantity" * "premium"."cs_list_price") as "total_sales"
  FROM
      "premium"
  GROUP BY
      2,
      3
  ORDER BY
      "last_name" asc,
      "first_name" asc,
      "total_sales" asc nulls first
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query24.preql`

  ```text
  HAVING references 'local.reagg_sum', 'local.avg_sub_total',
  which are not in the SELECT projection (line 45). Add them to SELECT, each
  prefixed with `--` so they stay out of the output rows — keep your HAVING
  as-is:
      select <your existing columns>, --local.reagg_sum, --local.avg_sub_total
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query24.preql`

  ```text
  (_duckdb.BinderException) Binder Error: Referenced table
  "questionable" not found!
  Candidate tables: "cooperative"

  LINE 39: ..."."store_sales_customer_customer_address_country")  and
  "questionable"."store_sales_ticket_number" in (select quest...
                                                                      ^
  [SQL:
  WITH
  cooperative as (
  SELECT
      "store_sales_customer_customer"."c_birth_country" as
  "store_sales_customer_birth_country",
      "store_sales_customer_customer"."c_first_name" as
  "store_sales_customer_first_name",
      "store_sales_customer_customer"."c_last_name" as
  "st
  …
  _last_name" as
  "store_sales_customer_last_name",
      "abundant"."store_sales_customer_first_name" as
  "store_sales_customer_first_name",
      "abundant"."store_sales_store_store_name" as "store_sales_store_store_name"
  FROM
      "abundant"
  GROUP BY
      1,
      2,
      3
  HAVING
      sum("abundant"."stage1_sub_total") > 0.05 *
  avg("abundant"."stage1_sub_total")

  ORDER BY
      "abundant"."store_sales_customer_last_name" asc,
      "abundant"."store_sales_customer_first_name" asc,
      "abundant"."store_sales_store_store_name" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```

### `syntax-parse`

- `trilogy file write query20.preql --content import raw.catalog_sales as cs;

# Filter to the date range and categories of interest
where cs.sold_date.date bet…s first,
    class asc nulls first,
    item_code asc nulls first,
    description asc nulls first,
    pct_of_class_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query20.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...ded sales price for this item ??? auto item_ext_sales_price <- s...

  Write stats: received 1103 chars / 1103 bytes; tail: …'lls first,\\n
  pct_of_class_sales asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…er total
auto max_cust_total <- max(cust_total);

# Customers whose total > 50% of max
auto best_customer_flag <- cust_total > 0.5 * max_cust_total;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 16, column 67.
  Expected one of:
          * RPAR

  Location:
  ...tinct(store_sales.item.item_sk ??? , store_sales.date_dim.date_sk...

  Write stats: received 1469 chars / 1469 bytes; tail: …'to best_customer_flag <-
  cust_total > 0.5 * max_cust_total;\\n'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…sc_prefix
    and web_sales.bill_customer.customer_sk in best_cust_sk
order by last_name asc, first_name asc, total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 10, column 23.
  Expected one of:
          * _TERMINATOR

  Location:
   prefix auto unique_pair <- 1 ??? by (item_desc_prefix, store_sa...

  Write stats: received 2328 chars / 2328 bytes; tail: …'asc, first_name asc,
  total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge item keys acros…esc, 1, 30) in freq_prefix
    and cs.bill_customer.customer_sk in bc
order by last_name asc, first_name asc, total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 15, column 16.
  Expected one of:
          * _TERMINATOR

  Location:
  ..._desc, 1, 30); auto pair <- 1 ??? by (prefix, ss.item.item_sk, s...

  Write stats: received 1463 chars / 1463 bytes; tail: …'asc, first_name asc,
  total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Frequent items: desc …ue

select 'test' as x, count_distinct(ss.date_dim.date_sk) as y
    by ss.item.item_sk
    ? ss.date_dim.year in (2000, 2001, 2002, 2003)
limit 10;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'by ') at line 36, column 5.
  Expected one of:
          * WHERE
          * ORDER
          * _TERMINATOR
          * MERGE
          * LIMIT
          * COMMA
          * HAVING
          * METADATA
  Previous tokens: [Token('IDENTIFIER', 'y')]

  Location:
  ...ss.date_dim.date_sk) as y     ??? by ss.item.item_sk     ? ss.da...

  Write stats: received 1706 chars / 1706 bytes; tail: …'   ? ss.date_dim.year in
  (2000, 2001, 2002, 2003)\\nlimit 10;\\n'.
  ```
- `trilogy run --import raw.store_returns:sr select count(sr.item.item_sk) as cnt, sr.date_dim.year, sr.date_dim.moy where sr.date_dim.moy between 3 and 11 group by sr.date_dim.year, sr.date_dim.moy order by sr.date_dim.year, sr.date_dim.moy limit 20;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...date_dim.moy between 3 and 11 ??? group by sr.date_dim.year, sr....
  ```
- `trilogy file write query27.preql --content import raw.store_sales as store_sales;

# Filters: Year 2002, TN stores, single male with College education
where …e,
    case when g_item = 0 and g_state = 0 then 0 else 1 end as group_indicator
order by
    item_code nulls first,
    state nulls first
limit 100;`

  ```text
  refused to write 'query27.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'def ') at line 11, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...'  # Define rollup aggregates ??? def rollup_avg_ticket_qty(metr...

  Write stats: received 1569 chars / 1569 bytes; tail: …'\\n    item_code nulls
  first,\\n    state nulls first\\nlimit 100;'.
  ```

### `join-resolution`

- `trilogy run query23.preql`

  ```text
  Could not resolve connections for query with output
  ['local.channel<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.last_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.first_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.total_sales<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query23.preql`

  ```text
  Could not resolve connections for query with output
  ['local.channel<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.last_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.first_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.total_sales<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query23.preql`

  ```text
  Could not resolve connections for query with output
  ['local.channel<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.last_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.first_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.total_sales<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.store_returns:sr select sr.date_dim.year, count(sr.item.item_sk) where sr.date_dim.moy between 4 and 10 group by sr.date_dim.year order by sr.date_dim.year limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(sr.item.item_sk) as item_sk_count`
  Location:
  ....year, count(sr.item.item_sk) ??? where sr.date_dim.moy between
  ```
- `trilogy file write query27.preql --content import raw.store_sales as store_sales;

# Filters: Year 2002, TN stores, single male with College education
where …e,
    case when g_item = 0 and g_state = 0 then 0 else 1 end as group_indicator
order by
    item_code nulls first,
    state nulls first
limit 100;`

  ```text
  refused to write 'query27.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `# Define rollup aggregates
      def rollup_avg(metric) -> avg(metric) by rollup store_sales.item.item_id as
  define_rollup_aggregates_def_rollup_avg_`
  Location:
   aggregates     def rollup_avg ??? (metric) -> avg(metric) by rol...

  Write stats: received 1196 chars / 1196 bytes; tail: …'\\n    item_code nulls
  first,\\n    state nulls first\\nlimit 100;'.
  ```

### `cli-misuse`

- `trilogy file write --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as catalog_sales;

# …sales.net_profit) as catalog_sales_net_profit
order by
    item_code asc,
    item_description asc,
    store_code asc,
    store_name asc
limit 100;`

  ```text
  Missing argument 'PATH'.
  ```

### `undefined-concept`

- `trilogy run --import raw.store_sales:store_sales select store_sales.item.item_id, store_sales.item.item_desc, store_sales.store.store_id, store_sales.store.store_name, sum(store_sales.net_profit) as profit where store_sales.date_dim.year=2001 and store_sales.date_dim.moy=4 order by item_id asc limit 10;`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  item_id.')
  ```
