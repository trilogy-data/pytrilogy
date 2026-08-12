# Trilogy failure analysis — 20260811-133917

- Run `20260811-133909_enriched_aggregates` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 418 | failed: 21 (5%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `disabled-tool` | 9 | 43% |
| `syntax-parse` | 4 | 19% |
| `other` | 3 | 14% |
| `undefined-concept` | 2 | 10% |
| `type-error` | 1 | 5% |
| `no-output` | 1 | 5% |
| `file-not-found` | 1 | 5% |

## Detail

### `disabled-tool`

- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_3697706765.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe_1455459008.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read probe_1455459008.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write _probe4.preql --run-and-delete`

  ```text
  refused to write '_probe4.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:1
    |
  8 | select2:
    | ^---
    |
    = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...here it.category is not null
   ??? select2:
  ```
- `trilogy file write _probe8.preql --run-and-delete`

  ```text
  refused to write '_probe8.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...d in (
           select it2.id ??? from raw.item as it2 where it2...
  ```
- `trilogy file write diag_3697706765.preql`

  ```text
  refused to write 'diag_3697706765.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:1
     |
  14 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...) as num_qualifying_prefixes
   ??? by *;
  ```
- `trilogy file write _diag_4199102535.preql`

  ```text
  refused to write '_diag_4199102535.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...ct count(c.sk) as n)
   ) -> (n) ???

   select diag.n;
  ```

### `other`

- `trilogy file write _probe5.preql --run-and-delete`

  ```text
  Unexpected error in _probe5.preql: name 'is_grouping_identity' is not defined
  ```
- `trilogy agent-info syntax example intersect-setops`

  ```text
  Unknown syntax example: 'intersect-setops'

  Available Trilogy syntax examples - print one with `trilogy agent-info syntax example <name>`:

  - `python-datasource` - run a local Python script as a datasource: wrap a function in `trilogy.io.run`, which writes the Arrow IPC stream to stdout for you from a table, dataframe, or list of dicts; declare concepts, map script columns in `datasource (...)`, use `grain (...) file `path.py`;`, then reference locally declared concepts WITHOUT the datasource name as a prefix
  - `query-structure` - the clause order of a query (`where` -> `select` <cols> -> joi
  …
  subtotal/total rows and to sort by level
  - `rank-over-rollup` - rank rollup subtotals/leaves with a SINGLE `rank(a,b) over (partition by level, parent ...)` - not separate ranks per level
  - `staged-membership` - compute a membership set in a `rowset` (keys meeting a count/HAVING), then filter the main query with `<key> in <rowset>.<col>`
  - `correlated-exists-via-grouped-counts` - translate `EXISTS other` / `NOT EXISTS other matching` over the same model into two `count(...) by <grain>` compared in `where` (`> 1` = another exists, `= 1` = no other matches) - pin the correlation grain with `by`
  ```
- `trilogy run probe3.preql`

  ```text
  Unexpected error in probe3.preql: (_duckdb.NotImplementedException) Not implemented Error: Unable to transform python value of type '<class 'trilogy.core.models.author.SubqueryItem'>' to DuckDB LogicalType
  [SQL:
  WITH
  macho as (
  SELECT
      "s_web_sales_unified"."WS_ITEM_SK" as "___tvf_arm_2_item_sk"
  FROM
      "fact_web_sales" as "s_web_sales_unified"
      INNER JOIN "dim_date_dim" as "s_sale_date_date" on "s_web_sales_unified"."WS_SOLD_DATE_SK" = "s_sale_date_date"."D_DATE_SK"
  WHERE
      "s_sale_date_date"."D_YEAR" BETWEEN 1999 AND 2001 and  'WEB'  = 'WEB'

  GROUP BY
      1),
  young as (
  SELECT
      "s_catalog_sales_unified"."CS_ITEM_SK" as "___tvf_arm_1_item_sk"
  FROM
      "fact_catalog_sales" as "s_catalog_sales_unified"
      INNER JOIN "dim_date_dim" as "s_sale_date_date" on "s_catalog_sales_unified"."CS_SOLD_DATE_SK" = "s_sale_date_date"."D_DATE_SK"
  WHERE
      "s_sale_date_date"."D_YEAR" BETWEEN 1999 AND 2001 and  'CATALOG'  = 'CATALOG'

  GROUP BY
      1),
  uneven as (
  SELECT
      "s_store_sales_unified"."SS_ITEM_SK" as "___tvf_arm_0_item_sk"
  FROM
      "fact_store_sales" as "s_store_sales_unified"
      INNER JOIN "dim_date_dim" as "s_sale_date_date" on "s_store_sales_unified"."SS_SOLD_DATE_SK" = "s_sale_date_date"."D_DATE_SK"
  WHERE
      "s_sale_date_date"."D_YEAR" BETWEEN 1999 AND 2001 and  'STORE'  = 'STORE'

  GROUP BY
      1),
  busy as (
  SELECT
      "uneven"."___tvf_arm_0_item_sk" as "_channel_common_item_sk"
  FROM
      "uneven"
  INTERSECT
  SELECT
      "young"."___tvf_arm_1_item_sk" as "_channel_common_item_sk"
  FROM
      "young"
  INTERSECT
  SELECT
      "macho"."___tvf_arm_2_item_sk" as "_channel_common_item_sk"
  FROM
      "macho"),
  charming as (
  SELECT
      "busy"."_channel_common_item_sk" as "channel_common_item_sk"
  FROM
      "busy"),
  puzzled as (
  SELECT
      $1 as "overall_avg",
      count(distinct "charming"."channel_common_item_sk") as "common_items"
  FROM
      "charming"),
  cheerful as (
  SELECT
      "s_catalog_sales_unified"."CS_ITEM_SK" as "s_item_sk",
      "s_catalog_sales_unified"."CS_ORDER_NUMBER" as "s_order_id",
      "s_catalog_sales_unified"."CS_SOLD_DATE_SK" as "s_sale_date_sk"
  FROM
      "fact_catalog_sales" as "s_catalog_sales_unified"
  UNION ALL
  SELECT
      "s_store_sales_unified"."SS_ITEM_SK" as "s_item_sk",
      "s_store_sales_unified"."SS_TICKET_NUMBER" as "s_order_id",
      "s_store_sales_unified"."SS_SOLD_DATE_SK" as "s_sale_date_sk"
  FROM
      "fact_store_sales" as "s_store_sales_unified"
  UNION ALL
  SELECT
      "s_web_sales_unified"."WS_ITEM_SK" as "s_item_sk",
      "s_web_sales_unified"."WS_ORDER_NUMBER" as "s_order_id",
      "s_web_sales_unified"."WS_SOLD_DATE_SK" as "s_sale_date_sk"
  FROM
      "fact_web_sales" as "s_web_sales_unified"),
  questionable as (
  SELECT
      "cheerful"."s_item_sk" as "s_item_sk",
      "s_item_items"."I_BRAND_ID" as "s_item_brand_id",
      "s_item_items"."I_CATEGORY_ID" as "s_item_category_id",
      "s_item_items"."I_CLASS_ID" as "s_item_class_id",
      "s_sale_date_date"."D_MOY" as "s_sale_date_month_of_year",
      "s_sale_date_date"."D_YEAR" as "s_sale_date_year",
      md5(CONCAT_WS('', coalesce(cast("cheerful"."s_item_sk" as string),'
  '), coalesce(cast("cheerful"."s_order_id" as string),'
  '))) as "_virt_func_hash_1026305025373187"
  FROM
      "cheerful"
      INNER JOIN "dim_item" as "s_item_items" on "cheerful"."s_item_sk" = "s_item_items"."I_ITEM_SK"
      LEFT OUTER JOIN "dim_date_dim" as "s_sale_date_date" on "cheerful"."s_sale_date_sk" = "s_sale_date_date"."D_DATE_SK"),
  protective as (
  SELECT
      count(CASE WHEN "questionable"."s_sale_date_year" = 2001 and "questionable"."s_sale_date_month_of_year" = 11 and exists (select 1 from charming where charming."channel_common_item_sk" is not distinct from "questionable"."s_item_sk") and "questionable"."s_item_brand_id" is not null and "questionable"."s_item_class_id" is not null and "questionable"."s_item_category_id" is not null THEN "questionable"."_virt_func_hash_1026305025373187" ELSE NULL END) as "nov01_common_lines"
  FROM
      "questionable")
  SELECT
      "puzzled"."overall_avg" as "overall_avg",
      "puzzled"."common_items" as "common_items",
      "protective"."nov01_common_lines" as "nov01_common_lines"
  FROM
      "protective"
      INNER JOIN "puzzled" on 1=1
  GROUP BY
      1,
      2,
      3]
  [parameters: (<Subquery: ref:_subquery_13_5.a>,)]
  (Background on this error at: https://sqlalche.me/e/20/tw8g)
  ```

### `undefined-concept`

- `trilogy run probe2_3697440276.preql`

  ```text
  Syntax error in probe2_3697440276.preql: 3 undefined concept references; fix all before re-running:
    - c.row_counter (line 3, in SELECT); did you mean: c.birth_country?
    - c.row_counter (line 4, in SELECT); did you mean: c.birth_country?
    - c.row_counter (line 5, in SELECT); did you mean: c.birth_country?
  ```
- `trilogy run answer_2524943990.preql`

  ```text
  Syntax error in answer_2524943990.preql: 2 undefined concept references; fix all before re-running:
    - catalog.multi_warehouse_order (line 16, col 9, in WHERE); did you mean: catalog.warehouse.id, catalog.return_warehouse.id, catalog.warehouse.name, multi_warehouse_order?
    - catalog.no_return_order (line 17, col 9, in WHERE); did you mean: catalog.return_fee, catalog.return_date.year, catalog.is_returned, no_return_order?
  ```

### `type-error`

- `trilogy file write answer_3705756794.preql --run`

  ```text
  Type error in answer_3705756794.preql: Invalid argument type 'NULL' passed into SUM function in position 1 from concept: arms.returns_amt. Valid: 'BIGINT', 'BOOL', 'DOUBLE', 'FLOAT', 'INTEGER', 'NUMBER', 'NUMERIC'.
  ```

### `no-output`

- `trilogy run _diag_4199102535.preql`

  ```text
  Nothing was executed: parsed 12 definition statement(s) (5 rowsets, 4 imports, 3 concepts) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```

### `file-not-found`

- `trilogy run probe_1455459008.preql`

  ```text
  Input 'probe_1455459008.preql' does not exist.
  ```
