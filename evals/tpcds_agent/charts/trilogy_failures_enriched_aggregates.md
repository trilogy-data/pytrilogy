# Trilogy failure analysis — 20260811-145004

- Run `20260811-145002_enriched_aggregates` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 376 | failed: 25 (7%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `disabled-tool` | 16 | 64% |
| `other` | 4 | 16% |
| `syntax-parse` | 3 | 12% |
| `no-output` | 2 | 8% |

## Detail

### `disabled-tool`

- `trilogy file read raw/all_sales.preql`

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
- `trilogy file read raw/_q04_agent_rowset_union_join.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read scratch_cust.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read scratch_verify.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read scratch_verify.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read scratch_probe2.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read scratch_chk_store.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read probe_avg.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe_kept.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe_avg.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `other`

- `trilogy run scratch1.preql`

  ```text
  Syntax error in scratch1.preql: This script requires parameter "zips" to be set in environment.
  ```
- `trilogy run scratch2.preql`

  ```text
  Syntax error in scratch2.preql: This script requires parameter "zips" to be set in environment.
  ```
- `trilogy run scratch_verify.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,840…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Unexpected error in scratch_verify.preql: Could not render the query: Missing source reference to qualifying_prefixes.prefix. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  abhorrent as (
  SELECT
      "ss_store_sales"."SS_NET_PROFIT" as "ss_net_profit",
      "ss_store_store"."S_STORE_NAME" as "ss_store_name",
      "ss_store_store"."S_ZIP" as "ss_store_zip"
  FROM
      "fact_store_sales" as "ss_store_sales"
      INNER JOIN "dim_date_dim" as "ss_sale_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_sale_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "dim_store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
  WHERE
      "ss_sale_date_date"."D_YEAR" = 1998 and "ss_sale_date_date"."D_QOY" = 2
  ),
  quizzical as (
  SELECT
      STRING_SPLIT( :zips , ',' ) as "_virt_func_split_4785012549328100"
  ),
  cheerful as (
  SELECT
      "ss_customer_current_address_customer_address"."CA_ZIP" as "ss_customer_current_address_zip",
      "ss_customer_customers"."C_CUSTOMER_SK" as "ss_customer_sk"
  FROM
      "dim_customer" as "ss_customer_customers"
      INNER JOIN "dim_customer_address" as "ss_customer_current_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_current_address_customer_address"."CA_ADDRESS_SK"
  WHERE
      "ss_customer_customers"."C_PREFERRED_CUST_FLAG" = 'Y' and exists (select 1 from (select unnest(quizzical."_virt_func_split_4785012549328100") as unnest_member from quizzical) as unnest_members where unnest_member is not distinct from "ss_customer_current_address_customer_address"."CA_ZIP")
  ),
  thoughtful as (
  SELECT
      "cheerful"."ss_customer_current_address_zip" as "_qualifying_zips_zip"
  FROM
      "cheerful"
  GROUP BY
      1
  HAVING
      count(distinct "cheerful"."ss_customer_sk") > 10
  ),
  abundant as (
  SELECT
      SUBSTRING("thoughtful"."_qualifying_zips_zip",1,2) as "qualifying_prefixes_prefix"
  FROM
      "thoughtful"
  GROUP BY
      1),
  macho as (
  SELECT
      "abhorrent"."ss_store_name" as "store_name",
      "abhorrent"."ss_store_zip" as "store_zip",
      (exists (select 1 from abundant where abundant."qualifying_prefixes_prefix" is not distinct from SUBSTRING("abhorrent"."ss_store_zip",1,2))) as "qualifies",
      SUBSTRING("abhorrent"."ss_store_zip",1,2) as "store_prefix"
  FROM
      "abhorrent"),
  sweltering as (
  SELECT
      "abhorrent"."ss_net_profit" as "ss_net_profit",
      "abhorrent"."ss_store_name" as "ss_store_name",
      "abhorrent"."ss_store_name" as "store_name",
      "abhorrent"."ss_store_zip" as "ss_store_zip",
      "abhorrent"."ss_store_zip" as "store_zip",
      (exists (select 1 from INVALID_REFERENCE_BUG<Missing source reference to qualifying_prefixes.prefix> where INVALID_REFERENCE_BUG<Missing source reference to qualifying_prefixes.prefix>."qualifying_prefixes_prefix" is not distinct from SUBSTRING("abhorrent"."ss_store_zip",1,2))) as "qualifies"
  FROM
      "abhorrent"),
  late as (
  SELECT
      "sweltering"."qualifies" as "qualifies",
      "sweltering"."ss_store_name" as "ss_store_name",
      "sweltering"."ss_store_zip" as "ss_store_zip",
      sum("sweltering"."ss_net_profit") as "total_net_profit"
  FROM
      "sweltering"
  GROUP BY
      1,
      2,
      3)
  SELECT
      "macho"."store_name" as "store_name",
      "macho"."store_zip" as "store_zip",
      "macho"."store_prefix" as "store_prefix",
      "late"."total_net_profit" as "total_net_profit"
  FROM
      "late"
      INNER JOIN "macho" on "late"."qualifies" = "macho"."qualifies" AND "late"."ss_store_name" is not distinct from "macho"."store_name" AND "late"."ss_store_zip" is not distinct from "macho"."store_zip"
  GROUP BY
      1,
      2,
      3,
      4,
      "late"."qualifies"
  ORDER BY
      "macho"."store_name" asc,
      "macho"."store_zip" asc
  LIMIT (100)
  ```
- `trilogy run probe8.preql`

  ```text
  Syntax error in probe8.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 29). Move the filter to WHERE to filter before aggregation.
  ```

### `syntax-parse`

- `trilogy file write scratch_explore.preql`

  ```text
  refused to write 'scratch_explore.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
   not null) as with_category,
   ???
  ```
- `trilogy file write scratch_probe.preql`

  ```text
  refused to write 'scratch_probe.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:1
     |
  15 | order by qualifying_line_items asc nulls first, ss.customer.current_address.state asc nulls first
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   qualifying_line_items >= 10
   ??? order by qualifying_line_items...
  ```
- `trilogy file write scratch_probe_q25b.preql`

  ```text
  refused to write 'scratch_probe_q25b.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:30
     |
  10 |     --(max(cs.warehouse.sk) <> min(cs.warehouse.sk)) as differs,
     |                              ^---
     |
     = expected sum_operator
  Location:
       --(max(cs.warehouse.sk) < ??? > min(cs.warehouse.sk)) as dif...
  ```

### `no-output`

- `trilogy run scratch4.preql --param zips=24128,76232,65084`

  ```text
  Nothing was executed: parsed 3 definition statement(s) (1 concept, 1 import, 1 rowset) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```
- `trilogy run probe_counts.preql`

  ```text
  Nothing was executed: parsed 4 definition statement(s) (3 rowsets, 1 import) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```
