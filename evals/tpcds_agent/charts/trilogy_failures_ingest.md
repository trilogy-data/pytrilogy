# Trilogy failure analysis — 20260603-005054

- Run `20260603-005045_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 97 | failed: 12 (12%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 6 | 50% |
| `undefined-concept` | 4 | 33% |
| `syntax-parse` | 2 | 17% |

## Detail

### `other`

- `trilogy run query05.preql`

  ```text
  Datatypes do not align for merged statements returns, have
  {<DataType.NUMERIC: 'numeric'>, <DataType.UNKNOWN: 'unknown'>}
  ```
- `trilogy run query05.preql`

  ```text
  Datatypes do not align for merged statements returns, have
  {<DataType.NUMERIC: 'numeric'>, <DataType.FLOAT: 'float'>}
  ```
- `trilogy run query05.preql`

  ```text
  Invalid derive expression ref:local.ch, must be a function or
  conditional
  ```
- `trilogy run query05.preql`

  ```text
  (_duckdb.BinderException) Binder Error: GROUP BY clause
  cannot contain aggregates!

  LINE 58:     coalesce(sum("ws_web_sales"."ws_net_paid"),0) as "gsales",
                        ^
  [SQL:
  WITH
  abhorrent as (
  SELECT
      "ss_store_sales"."ss_sold_date_sk" as "ss_date_dim_date_sk",
      "ss_store_sales"."ss_store_sk" as "ss_store_store_sk"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1,
      2),
  thoughtful as (
  SELECT
      "cs_catalog_sales"."cs_catalog_page_sk" as
  "cs_catalog_page_catalog_page_sk",
      "cs_catalog_sales"."cs_sold_date_sk" as "cs_sold_date_date_sk"
  FROM
      "catalog_sa
  …
  riendly"."profit" is not distinct from "juicy"."profit" AND
  "friendly"."returns" is not distinct from "juicy"."returns"
      FULL JOIN "charming" on coalesce("friendly"."channel", "juicy"."channel") =
  "charming"."channel" AND coalesce("friendly"."entity", "juicy"."entity") =
  "charming"."entity" AND coalesce("friendly"."gsales", "juicy"."gsales") =
  "charming"."gsales" AND coalesce("friendly"."profit", "juicy"."profit") =
  "charming"."profit" AND coalesce("friendly"."returns", "juicy"."returns") =
  "charming"."returns"
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query05.preql`

  ```text
  Invalid derive expression ref:local.channel, must be a
  function or conditional
  ```
- `trilogy run query05.preql`

  ```text
  (_duckdb.BinderException) Binder Error: GROUP BY clause
  cannot contain aggregates!

  LINE 58:     coalesce(sum("ws_web_sales"."ws_net_paid"),0) as "gsales",
                        ^
  [SQL:
  WITH
  abhorrent as (
  SELECT
      "ss_store_sales"."ss_sold_date_sk" as "ss_date_dim_date_sk",
      "ss_store_sales"."ss_store_sk" as "ss_store_store_sk"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1,
      2),
  thoughtful as (
  SELECT
      "cs_catalog_sales"."cs_catalog_page_sk" as
  "cs_catalog_page_catalog_page_sk",
      "cs_catalog_sales"."cs_sold_date_sk" as "cs_sold_date_date_sk"
  FROM
      "catalog_sa
  …
  riendly"."profit" is not distinct from "juicy"."profit" AND
  "friendly"."returns" is not distinct from "juicy"."returns"
      FULL JOIN "charming" on coalesce("friendly"."channel", "juicy"."channel") =
  "charming"."channel" AND coalesce("friendly"."entity", "juicy"."entity") =
  "charming"."entity" AND coalesce("friendly"."gsales", "juicy"."gsales") =
  "charming"."gsales" AND coalesce("friendly"."profit", "juicy"."profit") =
  "charming"."profit" AND coalesce("friendly"."returns", "juicy"."returns") =
  "charming"."returns"
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```

### `undefined-concept`

- `trilogy run query05.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  channel_type.')
  ```
- `trilogy run query05.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  channel_type.')
  ```
- `trilogy run query05.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  channel_type.')
  ```
- `trilogy run query05.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  channel_type.')
  ```

### `syntax-parse`

- `trilogy file write query05.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r… -> entity_id,
    gsales -> gross_sales,
    returns -> total_returns,
    profit -> net_profit
order by channel_type asc, entity_id asc
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', ': eid_s, eid_c, eid_w,\n
  gsales: gs_s, gs_c, gs_w,\n    returns: ret_s, ret_c, ret_w,\n    profit: np_s,
  np_c, np_w\nderive\n    channel -> channel_type,\n    entity -> entity_id,\n
  gsales -> gross_sales,\n    returns -> total_returns,\n    profit ->
  net_profit\norder by channel_type asc, entity_id asc\nlimit 100;\n') at line
  42, column 11.
  Expected one of:
          * LIMIT
          * DERIVE
          * COMMA
          * WHERE
          * ORDER
          * AND
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'entity')]

  Location:
  ...: ch_s, ch_c, ch_w,     entity ??? : eid_s, eid_c, eid_w,     gsa...

  Write stats: received 1811 chars / 1811 bytes; tail: …'_profit\\norder by
  channel_type asc, entity_id asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r….web_site.site_id;
auto w_profit <- w_sales;  # no web returns with site_id

select coalesce(s_returns, c_returns, 0::numeric) as x from ss limit 1;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 13, column 150.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ore.store_id is not null), 0) ??? by ss.store.store_id; auto s_r...

  Write stats: received 1572 chars / 1572 bytes; tail: …'sce(s_returns,
  c_returns, 0::numeric) as x from ss limit 1;\\n'.
  ```
