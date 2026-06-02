# Trilogy failure analysis — 20260602-131603

- Run `20260602-131553_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 86 | failed: 12 (14%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-missing-alias` | 4 | 33% |
| `syntax-parse` | 3 | 25% |
| `other` | 3 | 25% |
| `join-resolution` | 2 | 17% |

## Detail

### `syntax-missing-alias`

- `trilogy file write query05.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…m(store_returns.net_loss ? store_returns.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) by store_returns.store.store_id;

select 1;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `1 as 1`
  Location:
  ...rns.store.store_id;  select 1; ???

  Write stats: received 1153 chars / 1153 bytes; tail: …"00-09-06'::date) by
  store_returns.store.store_id;\\n\\nselect 1;".
  ```
- `trilogy file write query05.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…net_loss ? catalog_returns.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) by catalog_returns.call_center.call_center_sk;

select 1;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `1 as 1`
  Location:
  ...ter.call_center_sk;  select 1; ???

  Write stats: received 1872 chars / 1872 bytes; tail: …'e) by
  catalog_returns.call_center.call_center_sk;\\n\\nselect 1;'.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…llup store_sales.store.store_id as total_returns,
    coalesce(store_np, 0) - coalesce(store_nl, 0) as net_profit
order by channel, entity
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `coalesce(store_gross_sales, 0) by rollup
  store_sales.store.store_id as coalesce_store_gross_sales_0_by_rollup_s`
  Location:
  ...lesce(store_gross_sales, 0) by ??? rollup store_sales.store.stor...

  Write stats: received 1377 chars / 1377 bytes; tail: …'ore_nl, 0) as
  net_profit\\norder by channel, entity\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…e(wb_tr, 0) by rollup ws.web_site.site_id as tr_wb,
    coalesce(wb_np, 0) - coalesce(wb_nl, 0) as np_wb
order by wb_ch, wb_en nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `coalesce(st_gs, 0) by rollup ss.store.store_id as
  coalesce_st_gs_0_by_rollup_ss_store_stor`
  Location:
  ..._en,     coalesce(st_gs, 0) by ??? rollup ss.store.store_id as g...

  Write stats: received 2906 chars / 2906 bytes; tail: …'nl, 0) as np_wb\\norder
  by wb_ch, wb_en nulls first\\nlimit 100;'.
  ```

### `syntax-parse`

- `trilogy file write query05.preql --content import raw.store_sales as store_sales
import raw.store_returns as store_returns
import raw.catalog_sales as catalo…w.web_sales as web_sales
import raw.web_returns as web_returns

merge store_sales.store.store_sk into ~store_returns.store.store_sk

select 1 as test`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'import ') at line 2, column 1.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'store_sales')]

  Location:
  ...aw.store_sales as store_sales ??? import raw.store_returns as st...

  Write stats: received 326 chars / 326 bytes; tail: …'tore_sk into
  ~store_returns.store.store_sk\\n\\nselect 1 as test'.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…oss ? store_returns.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) as net_profit
    by rollup store_sales.store.store_id
limit 10;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'by ') at line 18, column 5.
  Expected one of:
          * MERGE
          * WHERE
          * HAVING
          * LIMIT
          * METADATA
          * _TERMINATOR
          * COMMA
          * ORDER
  Previous tokens: [Token('IDENTIFIER', 'net_profit')]

  Location:
  ...-06'::date) as net_profit     ??? by rollup store_sales.store.st...

  Write stats: received 1033 chars / 1033 bytes; tail: …'et_profit\\n    by
  rollup store_sales.store.store_id\\nlimit 10;'.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…_returns.net_loss ? catalog_returns.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) as net_profit
order by channel, entity
limit 100`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 28, column 1.
  Expected one of:
          * WHERE
          * SELECT
          * FROM
  Previous tokens: [Token('MERGE', 'merge')]

  Location:
  ...100  merge  # Catalog channel ??? auto ct_sales <- sum(catalog_s...

  Write stats: received 2958 chars / 2958 bytes; tail: …"-06'::date) as
  net_profit\\norder by channel, entity\\nlimit 100".
  ```

### `other`

- `trilogy run query05.preql duckdb`

  ```text
  (_duckdb.BinderException) Binder Error: column "entity" must
  appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(entity)" if the exact
  value of "entity" is not important.

  LINE 71:     "concerned"."entity" as "entity",
               ^
  [SQL:
  WITH
  questionable as (
  SELECT
      "store_returns_store_returns"."sr_return_amt" as
  "_virt_filter_return_amt_6672716593962401",
      "store_sales_store_sales"."ss_net_paid" as
  "_virt_filter_net_paid_3008268891857210",
      "store_sales_store_store"."s_store_id" as "store_sales_stor
  …
  es") as "gross_sales",
      coalesce("juicy"."total_returns","young"."total_returns") as
  "total_returns",
      "young"."store_profit" as "store_profit",
      "juicy"."store_loss" as "store_loss"
  FROM
      "young"
      INNER JOIN "juicy" on "young"."entity" is not distinct from
  "juicy"."entity" AND "young"."gross_sales" is not distinct from
  "juicy"."gross_sales" AND "young"."total_returns" is not distinct from
  "juicy"."total_returns"
  ORDER BY
      "juicy"."channel" asc,
      coalesce("juicy"."entity","young"."entity") asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query05.preql duckdb`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.entity', 'local._virt_filter_net_loss_4984329924949182'} out
  of  with found {'catalog_returns.order_number', 'catalog_returns.item.item_sk',
  'local._virt_filter_return_amount_5708143592307031',
  'local._virt_filter_net_loss_4984329924949182'}
  ```
- `trilogy run query05.preql duckdb`

  ```text
  SELECT output 'local.st_gs' is defined by an expression that
  references 'local.st_gs' itself (line 18). This is a recursive self-reference:
  an alias cannot redefine a name its own calculation reads. Rename the output to
  a distinct name (e.g. `... as st_gs_out`).
  ```

### `join-resolution`

- `trilogy run query05.preql duckdb`

  ```text
  Could not resolve connections for query with output
  ['local.store_gross_sales<Purpose.METRIC>Derivation.BASIC>',
  'local.catalog_gross_sales<Purpose.METRIC>Derivation.BASIC>',
  'local.web_gross_sales<Purpose.METRIC>Derivation.BASIC>'] from current model.
  The output draws on models that are not connected in the current graph:
  catalog_sales (needed by local.catalog_gross_sales); store_sales (needed by
  local.store_gross_sales); web_sales (needed by local.web_gross_sales). If these
  should be related, bridge their keys with a merge, e.g. `merge
  catalog_sales.<key> into ~store_sales.<key>;`.
  ```
- `trilogy run query05.preql duckdb`

  ```text
  Could not resolve connections for query with output
  ['local.ch_wb<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.ent_wb<Purpose.PROPERTY>Derivation.BASIC>',
  'local.gs_wb<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.tr_wb<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.np_wb<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.nl_wb<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. The
  output draws on models that are not connected in the current graph: local
  (needed by local.ch_wb); wr (needed by local.nl_wb, local.tr_wb); ws (needed by
  local.ent_wb, local.gs_wb, local.nl_wb, local.np_wb, local.tr_wb). If these
  should be related, bridge their keys with a merge, e.g. `merge local.<key> into
  ~wr.<key>;`.
  ```
