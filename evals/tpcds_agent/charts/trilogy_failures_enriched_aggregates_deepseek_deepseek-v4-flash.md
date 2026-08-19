# Trilogy failure analysis — 20260813-030820

- Run `20260813-030820_enriched_aggregates` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 295 | failed: 18 (6%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `disabled-tool` | 10 | 56% |
| `other` | 4 | 22% |
| `syntax-parse` | 3 | 17% |
| `undefined-concept` | 1 | 6% |

## Detail

### `disabled-tool`

- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
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
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `other`

- `trilogy file write probe4.preql --run-and-delete`

  ```text
  Unexpected error in probe4.preql: Could not render the query: Missing source reference to a.sale_date.week_seq. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "a_channel",
      "a_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "a_ext_sales_price",
      "a_catalog_sales_unified"."CS_SOLD_DATE_SK" as "a_sale_date_sk"
  FROM
      "fact_catalog_sales" as "a_catalog_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "a_channel",
      "a_web_sales_unified"."WS_EXT_SALES_PRICE" as "a_ext_sales_price",
      "a_web_sales_unified"."WS_SOLD_DATE_SK" as "a_sale_date_sk"
  FROM
      "fact_web_sales" as "a_web_sales_unified"),
  cooperative as (
  SELECT
      "a_sale_date_date"."D_DOW" as "_dow_sales_dow",
      "a_sale_date_date"."D_WEEK_SEQ" as "_dow_sales_wk",
      sum("cheerful"."a_ext_sales_price") as "_dow_sales_sales"
  FROM
      "cheerful"
      LEFT OUTER JOIN "dim_date_dim" as "a_sale_date_date" on "cheerful"."a_sale_date_sk" = "a_sale_date_date"."D_DATE_SK"
  WHERE
      ("cheerful"."a_channel" is not null and "cheerful"."a_channel" in ('WEB','CATALOG'))

  GROUP BY
      1,
      2),
  uneven as (
  SELECT
      "cooperative"."_dow_sales_dow" as "dow_sales_dow",
      "cooperative"."_dow_sales_dow" as "fut_dow",
      "cooperative"."_dow_sales_sales" as "dow_sales_sales",
      "cooperative"."_dow_sales_sales" as "fut_sales",
      "cooperative"."_dow_sales_wk" + 53 as "_virt_func_add_2282303569819927",
      "cooperative"."_dow_sales_wk" as "dow_sales_wk",
      "cooperative"."_dow_sales_wk" as "fut_wk"
  FROM
      "cooperative"),
  yummy as (
  SELECT
      "uneven"."dow_sales_sales" as "dow_sales_sales",
      "uneven"."dow_sales_wk" as "dow_sales_wk",
      CASE WHEN "uneven"."fut_sales" is not null THEN "uneven"."dow_sales_sales" ELSE NULL END as "_virt_filter_sales_5458029491157636"
  FROM
      "uneven"),
  juicy as (
  SELECT
      "yummy"."_virt_filter_sales_5458029491157636" as "_virt_filter_sales_5458029491157636",
      "yummy"."dow_sales_wk" as "dow_sales_wk"
  FROM
      "yummy"
  GROUP BY
      1,
      2,
      "yummy"."dow_sales_sales"),
  vacuous as (
  SELECT
      "juicy"."dow_sales_wk" as "dow_sales_wk",
      max("juicy"."_virt_filter_sales_5458029491157636") as "has_future"
  FROM
      "juicy"
  GROUP BY
      1),
  concerned as (
  SELECT
      "uneven"."_virt_func_add_2282303569819927" as "_virt_func_add_2282303569819927",
      "uneven"."dow_sales_dow" as "dow_sales_dow",
      "uneven"."dow_sales_wk" as "dow_sales_wk",
      "vacuous"."has_future" as "has_future"
  FROM
      "uneven"
      INNER JOIN "vacuous" on "uneven"."dow_sales_wk" is not distinct from "vacuous"."dow_sales_wk"),
  young as (
  SELECT
      "concerned"."_virt_func_add_2282303569819927" as "_virt_func_add_2282303569819927",
      "concerned"."dow_sales_dow" as "dow_sales_dow",
      "concerned"."dow_sales_dow" as "fut_dow",
      "concerned"."dow_sales_wk" as "wk",
      "concerned"."has_future" as "has_future"
  FROM
      "concerned"),
  sparkling as (
  SELECT
      "young"."_virt_func_add_2282303569819927" as "_virt_func_add_2282303569819927",
      "young"."dow_sales_dow" as "dow_sales_dow",
      "young"."fut_dow" as "fut_dow",
      "young"."has_future" as "has_future",
      "young"."wk" as "wk"
  FROM
      "young")
  SELECT
      "young"."wk" as "wk",
      "young"."has_future" as "has_future"
  FROM
      "sparkling"
      INNER JOIN "young" on "sparkling"."_virt_func_add_2282303569819927" = "young"."_virt_func_add_2282303569819927" AND "sparkling"."dow_sales_dow" is not distinct from "young"."dow_sales_dow" AND "sparkling"."has_future" is not distinct from "young"."has_future" AND "sparkling"."wk" = "young"."wk"
  WHERE
      INVALID_REFERENCE_BUG<Missing source reference to a.sale_date.week_seq> >= 5320

  GROUP BY
      1,
      2
  ORDER BY
      "young"."wk" asc
  ```
- `trilogy file write answer_1858999935.preql --run`

  ```text
  Unexpected error in answer_1858999935.preql: Could not render the query: Missing source reference to a.sale_date.week_seq. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  sparkling as (
  SELECT
      "d_date"."D_WEEK_SEQ" as "weeks2001_wk"
  FROM
      "dim_date_dim" as "d_date"
  WHERE
      "d_date"."D_YEAR" = 2001

  GROUP BY
      1),
  cheerful as (
  SELECT
       'CATALOG'  as "a_channel",
      "a_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "a_ext_sales_price",
      "a_catalog_sales_unified"."CS_SOLD_DATE_SK" as "a_sale_date_sk"
  FROM
      "fact_catalog_sales" as "a_catalog_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "a_channel",
      "a_web_sales_unified"."WS_EXT_SALES_PRICE" as "a_ext_sales_price",
      "a_web_sales_unified"."WS_SOLD_DATE_SK" as "a_sale_date_sk"
  FROM
      "fact_web_sales" as "a_web_sales_unified"),
  cooperative as (
  SELECT
      "a_sale_date_date"."D_DOW" as "_dow_sales_dow",
      "a_sale_date_date"."D_WEEK_SEQ" as "_dow_sales_wk",
      sum("cheerful"."a_ext_sales_price") as "_dow_sales_sales"
  FROM
      "cheerful"
      LEFT OUTER JOIN "dim_date_dim" as "a_sale_date_date" on "cheerful"."a_sale_date_sk" = "a_sale_date_date"."D_DATE_SK"
  WHERE
      ("cheerful"."a_channel" is not null and "cheerful"."a_channel" in ('WEB','CATALOG'))

  GROUP BY
      1,
      2),
  uneven as (
  SELECT
      "cooperative"."_dow_sales_dow" as "dow_sales_dow",
      "cooperative"."_dow_sales_dow" as "fut_dow",
      "cooperative"."_dow_sales_sales" as "dow_sales_sales",
      "cooperative"."_dow_sales_sales" as "fut_sales",
      "cooperative"."_dow_sales_wk" + 53 as "_virt_func_add_2282303569819927",
      "cooperative"."_dow_sales_wk" as "dow_sales_wk",
      "cooperative"."_dow_sales_wk" as "fut_wk"
  FROM
      "cooperative"),
  yummy as (
  SELECT
      "uneven"."dow_sales_dow" as "dow_sales_dow",
      "uneven"."dow_sales_sales" / "uneven"."fut_sales" as "_virt_func_divide_4096338924150021",
      "uneven"."dow_sales_wk" as "dow_sales_wk",
      "uneven"."fut_dow" as "fut_dow",
      "uneven"."fut_wk" as "fut_wk"
  FROM
      "uneven"),
  juicy as (
  SELECT
      "yummy"."dow_sales_dow" as "dow_sales_dow",
      "yummy"."dow_sales_wk" as "dow_sales_wk",
      "yummy"."fut_dow" as "fut_dow",
      "yummy"."fut_wk" as "fut_wk",
      CASE WHEN "yummy"."dow_sales_dow" = 0 THEN "yummy"."_virt_func_divide_4096338924150021" ELSE NULL END as "_virt_filter_5958033065751971",
      CASE WHEN "yummy"."dow_sales_dow" = 1 THEN "yummy"."_virt_func_divide_4096338924150021" ELSE NULL END as "_virt_filter_601564596795895",
      CASE WHEN "yummy"."dow_sales_dow" = 2 THEN "yummy"."_virt_func_divide_4096338924150021" ELSE NULL END as "_virt_filter_2946388091590669",
      CASE WHEN "yummy"."dow_sales_dow" = 3 THEN "yummy"."_virt_func_divide_4096338924150021" ELSE NULL END as "_virt_filter_8565183181289158",
      CASE WHEN "yummy"."dow_sales_dow" = 4 THEN "yummy"."_virt_func_divide_4096338924150021" ELSE NULL END as "_virt_filter_3283913797702838",
      CASE WHEN "yummy"."dow_sales_dow" = 5 THEN "yummy"."_virt_func_divide_4096338924150021" ELSE NULL END as "_virt_filter_2437681644627202",
      CASE WHEN "yummy"."dow_sales_dow" = 6 THEN "yummy"."_virt_func_divide_4096338924150021" ELSE NULL END as "_virt_filter_9399393513655732"
  FROM
      "yummy"),
  vacuous as (
  SELECT
      "juicy"."_virt_filter_2437681644627202" as "_virt_agg_max_1235202847133100",
      "juicy"."_virt_filter_2946388091590669" as "_virt_agg_max_7392672016673230",
      "juicy"."_virt_filter_3283913797702838" as "_virt_agg_max_5114579565971939",
      "juicy"."_virt_filter_5958033065751971" as "_virt_agg_max_4804741707063960",
      "juicy"."_virt_filter_601564596795895" as "_virt_agg_max_9131047644678936",
      "juicy"."_virt_filter_8565183181289158" as "_virt_agg_max_4064030836947737",
      "juicy"."_virt_filter_9399393513655732" as "_virt_agg_max_9565596272094933",
      "juicy"."dow_sales_wk" as "week_seq",
      "juicy"."fut_dow" as "fut_dow",
      "juicy"."fut_wk" as "fut_wk"
  FROM
      "juicy"),
  concerned as (
  SELECT
      "vacuous"."fut_dow" as "dow_sales_dow",
      "vacuous"."fut_dow" as "fut_dow",
      "vacuous"."fut_wk" as "_virt_func_add_2282303569819927",
      "vacuous"."fut_wk" as "fut_wk",
      "vacuous"."week_seq" as "week_seq",
      round("vacuous"."_virt_agg_max_1235202847133100",2) as "fri_ratio",
      round("vacuous"."_virt_agg_max_4064030836947737",2) as "wed_ratio",
      round("vacuous"."_virt_agg_max_4804741707063960",2) as "sun_ratio",
      round("vacuous"."_virt_agg_max_5114579565971939",2) as "thu_ratio",
      round("vacuous"."_virt_agg_max_7392672016673230",2) as "tue_ratio",
      round("vacuous"."_virt_agg_max_9131047644678936",2) as "mon_ratio",
      round("vacuous"."_virt_agg_max_9565596272094933",2) as "sat_ratio"
  FROM
      "vacuous"),
  young as (
  SELECT
      "concerned"."_virt_func_add_2282303569819927" as "_virt_func_add_2282303569819927",
      "concerned"."dow_sales_dow" as "dow_sales_dow",
      "concerned"."fri_ratio" as "fri_ratio",
      "concerned"."fut_dow" as "fut_dow",
      "concerned"."fut_wk" as "fut_wk",
      "concerned"."mon_ratio" as "mon_ratio",
      "concerned"."sat_ratio" as "sat_ratio",
      "concerned"."sun_ratio" as "sun_ratio",
      "concerned"."thu_ratio" as "thu_ratio",
      "concerned"."tue_ratio" as "tue_ratio",
      "concerned"."wed_ratio" as "wed_ratio",
      "concerned"."week_seq" as "week_seq"
  FROM
      "concerned")
  SELECT
      "concerned"."week_seq" as "week_seq",
      "concerned"."sun_ratio" as "sun_ratio",
      "concerned"."mon_ratio" as "mon_ratio",
      "concerned"."tue_ratio" as "tue_ratio",
      "concerned"."wed_ratio" as "wed_ratio",
      "concerned"."thu_ratio" as "thu_ratio",
      "concerned"."fri_ratio" as "fri_ratio",
      "concerned"."sat_ratio" as "sat_ratio"
  FROM
      "young"
      INNER JOIN "concerned" on "young"."dow_sales_dow" is not distinct from "concerned"."fut_dow" AND "young"."fri_ratio" is not distinct from "concerned"."fri_ratio" AND "young"."fut_wk" = "concerned"."fut_wk" AND "young"."mon_ratio" is not distinct from "concerned"."mon_ratio" AND "young"."sat_ratio" is not distinct from "concerned"."sat_ratio" AND "young"."sun_ratio" is not distinct from "concerned"."sun_ratio" AND "young"."thu_ratio" is not distinct from "concerned"."thu_ratio" AND "young"."tue_ratio" is not distinct from "concerned"."tue_ratio" AND "young"."wed_ratio" is not distinct from "concerned"."wed_ratio" AND "young"."week_seq" = "concerned"."week_seq"
  WHERE
      exists (select 1 from sparkling where sparkling."weeks2001_wk" is not distinct from INVALID_REFERENCE_BUG<Missing source reference to a.sale_date.week_seq>)

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8
  ORDER BY
      "concerned"."week_seq" asc
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Syntax error in probe2.preql: a `(select ...)` subquery used as a scalar value or membership set must select exactly one column; project only the key/value consumed by the outer expression (line 7, column 7)
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

### `syntax-parse`

- `trilogy file write probe5.preql --run-and-delete`

  ```text
  refused to write 'probe5.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:1
     |
  16 | ;
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...ales.dow = fut.dow
   limit 5;
   ??? ;
  ```
- `trilogy file write probe6.preql --run-and-delete`

  ```text
  refused to write 'probe6.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:1
     |
  21 | ;
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...atio_pairs.dow asc
   limit 5;
   ??? ;
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  refused to write 'probe2.preql': not syntactically valid Trilogy.

  Parse error:
    --> 25:1
     |
  25 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...year = 11)) as nov2001_lines
   ??? by *;
  ```

### `undefined-concept`

- `trilogy file write probe4.preql --run-and-delete --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126`

  ```text
  Syntax error in probe4.preql: Undefined concept: local.zip (line 17, col 10, in ORDER BY). Suggestions: ['ss.store.zip', 'ss.pos_address.zip', 'ss.return_store.zip', 'qualifying_zips.zip', 'ss.return_address.zip', 'ss.customer.current_address.zip']
  ```
