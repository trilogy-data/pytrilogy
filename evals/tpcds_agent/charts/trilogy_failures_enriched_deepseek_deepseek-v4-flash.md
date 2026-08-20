# Trilogy failure analysis — 20260820-031801

- Run `20260820-031800_enriched_deepseek_deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 1184 | failed: 66 (6%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 22 | 33% |
| `disabled-tool` | 20 | 30% |
| `other` | 10 | 15% |
| `undefined-concept` | 9 | 14% |
| `join-resolution` | 4 | 6% |
| `cli-misuse` | 1 | 2% |

## Detail

### `syntax-parse`

- `trilogy file write probe6.preql --run-and-delete`

  ```text
  refused to write 'probe6.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:35
    |
  6 |   sum(1 ? (s.channel_dim_text_id <> s.return_channel_dim_text_id)) as diff_entity,
    |                                   ^---
    |
    = expected sum_operator
  Location:
  ...m(1 ? (s.channel_dim_text_id < ??? > s.return_channel_dim_text_id...
  ```
- `trilogy file write probeX.preql --run-and-delete`

  ```text
  refused to write 'probeX.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:129
    |
  7 |   count(s.order_id ? (s.channel_dim_text_id is not null and s.return_channel_dim_text_id is not null and s.channel_dim_text_id <> s.return_channel_dim_text_id)) as diff_entity_rows,
    |                                                                                                                                 ^---
    |
    = expected sum_operator
  Location:
  ...ll and s.channel_dim_text_id < ??? > s.return_channel_dim_text_id...
  ```
- `trilogy file write probe_intersection.preql --run-and-delete`

  ```text
  refused to write 'probe_intersection.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ....sk = cs.billing_customer.sk
   ??? union join ss.item.sk = cs.ite...
  ```
- `trilogy file write probe_distinct.preql --run-and-delete`

  ```text
  refused to write 'probe_distinct.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...n (pairs.p_csk, pairs.p_isk)
   ???
  ```
- `trilogy file write probe_intersection.preql --run-and-delete`

  ```text
  refused to write 'probe_intersection.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ....sk = cs.billing_customer.sk
   ??? union join ss.item.sk = cs.ite...
  ```
- `trilogy file write probe_distinct.preql --run-and-delete`

  ```text
  refused to write 'probe_distinct.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...ss.customer.sk = pairs.p_csk
   ??? subset join ss.item.sk = pairs...
  ```
- `trilogy file write probe_min.preql --run-and-delete`

  ```text
  refused to write 'probe_min.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...ount(cs.order_number) as n2,
   ??? union join ss.customer.sk = cs...
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  refused to write 'probe_a.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ....sk = cs.billing_customer.sk
   ??? union join ss.item.sk = cs.ite...
  ```
- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  refused to write 'probe_b.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...ount(cs.order_number) as n2,
   ??? union join ss.customer.sk = cs...
  ```
- `trilogy file write probe_c.preql --run-and-delete`

  ```text
  refused to write 'probe_c.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
     count(ss.quantity) as n1,
   ??? union join ss.customer.sk = cs...
  ```
- `trilogy file write probe_d.preql --run-and-delete`

  ```text
  refused to write 'probe_d.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
     count(cs.quantity) as n2,
   ??? union join ss.customer.sk = cs...
  ```
- `trilogy file write probe_e.preql --run-and-delete`

  ```text
  refused to write 'probe_e.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ....sk = cs.billing_customer.sk
   ??? union join ss.item.sk = cs.ite...
  ```
- `trilogy file write probe_f.preql --run`

  ```text
  refused to write 'probe_f.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ....sk = cs.billing_customer.sk
   ??? union join ss.item.sk = cs.ite...
  ```
- `trilogy file write probe_g.preql --run-and-delete`

  ```text
  refused to write 'probe_g.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ....sk = cs.billing_customer.sk
   ??? union join ss.item.sk = cs.ite...
  ```
- `trilogy file write probe_h.preql --run-and-delete`

  ```text
  refused to write 'probe_h.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
     count(ss.quantity) as n1,
   ??? union join ss.customer.sk = cs...
  ```
- `trilogy file write probe_j.preql --run-and-delete`

  ```text
  refused to write 'probe_j.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
     count(ss.quantity) as n1,
   ??? subset join ss.customer.sk = c...
  ```
- `trilogy file write probe_l.preql --run-and-delete`

  ```text
  refused to write 'probe_l.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
     count(ss.quantity) as n1,
   ??? union join ss.customer.sk = cs...
  ```
- `trilogy file write probe_n.preql --run-and-delete`

  ```text
  refused to write 'probe_n.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ....sk = cs.billing_customer.sk
   ??? union join ss.item.sk = cs.ite...
  ```
- `trilogy file write probe_p.preql --run-and-delete`

  ```text
  refused to write 'probe_p.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ....sk = cs.billing_customer.sk
   ??? union join ss.item.sk = cs.ite...
  ```
- `trilogy file write answer_3544057080.preql --run`

  ```text
  refused to write 'answer_3544057080.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:53
     |
  20 |     and ss.pos_customer_demographic.marital_status <> ss.customer.current_demographics.marital_status
     |                                                     ^---
     |
     = expected sum_operator
  Location:
  ...r_demographic.marital_status < ??? > ss.customer.current_demograp...
  ```
- `trilogy file write probe_d.preql --run-and-delete`

  ```text
  refused to write 'probe_d.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:1
     |
  15 | union join ws.item.sk = item_avg.item_sk
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...e.date <= '2000-04-26'::date
   ??? union join ws.item.sk = item_a...
  ```
- `trilogy file write probe_check2.preql --run-and-delete`

  ```text
  refused to write 'probe_check2.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:35
    |
  5 |     sum(ss.is_returned = true ? 1 : 0) as returned_rows,
    |                                   ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...sum(ss.is_returned = true ? 1 ??? : 0) as returned_rows,
       su...
  ```

### `disabled-tool`

- `trilogy file read raw/all_sales.preql`

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
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_765177085.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/inventory.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/_q04_agent_rowset_union_join.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

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
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `other`

- `trilogy file write probe2.preql --run-and-delete --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Unexpected error in probe2.preql: Could not render the query: Missing source reference to local._virt_func_split_4785012549328100. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  wakeful as (
  SELECT
      "c_current_address_customer_address"."CA_ADDRESS_SK" as "c_current_address_sk",
      "c_current_address_customer_address"."CA_ZIP" as "c_current_address_zip",
      "c_customers"."C_CUSTOMER_SK" as "c_sk",
      "c_customers"."C_PREFERRED_CUST_FLAG" as "c_preferred_cust_flag"
  FROM
      "customer" as "c_customers"
      INNER JOIN "customer_address" as "c_current_address_customer_address" on "c_customers"."C_CURRENT_ADDR_SK" = "c_current_address_customer_address"."CA_ADDRESS_SK"),
  thoughtful as (
  SELECT
      "wakeful"."c_current_address_zip" as "c_current_address_zip",
      count(CASE WHEN "wakeful"."c_preferred_cust_flag" = 'Y' THEN "wakeful"."c_sk" ELSE NULL END) as "_virt_agg_count_7105403771812092"
  FROM
      "wakeful"
  GROUP BY
      1),
  cheerful as (
  SELECT
      "wakeful"."c_current_address_zip" as "c_current_address_zip"
  FROM
      "wakeful"
  GROUP BY
      1,
      "wakeful"."c_current_address_sk"),
  cooperative as (
  SELECT
      SUBSTRING(CASE WHEN ( "thoughtful"."_virt_agg_count_7105403771812092" ) > 10 and exists (select 1 from (select unnest(INVALID_REFERENCE_BUG<Missing source reference to local._virt_func_split_4785012549328100>."_virt_func_split_4785012549328100") as unnest_member from INVALID_REFERENCE_BUG<Missing source reference to local._virt_func_split_4785012549328100>) as unnest_members where unnest_member is not distinct from "cheerful"."c_current_address_zip") THEN "cheerful"."c_current_address_zip" ELSE NULL END,1,2) as "_virt_func_substring_1010830310934997"
  FROM
      "thoughtful"
      INNER JOIN "cheerful" on "thoughtful"."c_current_address_zip" is not distinct from "cheerful"."c_current_address_zip"),
  yummy as (
  SELECT
      "s_store"."S_STORE_NAME" as "s_name",
      CASE WHEN exists (select 1 from cooperative where cooperative."_virt_func_substring_1010830310934997" is not distinct from SUBSTRING("s_store"."S_ZIP",1,2)) THEN "s_store"."S_ZIP" ELSE NULL END as "matching_store_zip"
  FROM
      "store" as "s_store"),
  juicy as (
  SELECT
      "yummy"."matching_store_zip" as "store_zip",
      "yummy"."s_name" as "store_name"
  FROM
      "yummy")
  SELECT
      "juicy"."store_zip" as "store_zip",
      "juicy"."store_name" as "store_name"
  FROM
      "juicy"
  GROUP BY
      1,
      2
  ORDER BY
      "juicy"."store_name" asc
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Unexpected error in probe2.preql: (_duckdb.BinderException) Binder Error: Values list "cheerful" does not have a column named "_virt_filter_sk_1987727915913391"

  LINE 34:     "cheerful"."_virt_filter_sk_1987727915913391" as "_virt_fil...
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "s_channel",
      "s_catalog_sales_unified"."CS_ITEM_SK" as "s_item_sk",
      "s_catalog_sales_unified"."CS_ORDER_NUMBER" as "s_order_id"
  FROM
      "catalog_sales" as "s_catalog_sales_unified"
  UNION ALL
  SELECT
       'STORE'  as "s_channel",
      "s_store_sales_unified"."SS_ITEM_SK" as "s_item_sk",
      "s_store_sales_unified"."SS_TICKET_NUMBER" as "s_order_id"
  FROM
      "store_sales" as "s_store_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "s_channel",
      "s_web_sales_unified"."WS_ITEM_SK" as "s_item_sk",
      "s_web_sales_unified"."WS_ORDER_NUMBER" as "s_order_id"
  FROM
      "web_sales" as "s_web_sales_unified"),
  questionable as (
  SELECT
      "cheerful"."s_channel" as "s_channel",
      count(md5(CONCAT_WS('', coalesce(cast("cheerful"."s_channel" as string),'
  '), coalesce(cast("cheerful"."s_order_id" as string),'
  '), coalesce(cast("cheerful"."s_item_sk" as string),'
  ')))) as "total_rows"
  FROM
      "cheerful"
  GROUP BY
      1),
  thoughtful as (
  SELECT
      "cheerful"."_virt_filter_sk_1987727915913391" as "_virt_filter_sk_1987727915913391",
      "cheerful"."_virt_filter_sk_2972560933666249" as "_virt_filter_sk_2972560933666249",
      "cheerful"."_virt_filter_sk_9572960914066364" as "_virt_filter_sk_9572960914066364",
      "cheerful"."s_channel" as "s_channel"
  FROM
      "cheerful"
  GROUP BY
      1,
      2,
      3,
      4,
      "cheerful"."s_item_sk"),
  cooperative as (
  SELECT
      "thoughtful"."s_channel" as "s_channel",
      count("thoughtful"."_virt_filter_sk_1987727915913391") as "nonnull_lp",
      count("thoughtful"."_virt_filter_sk_2972560933666249") as "nonnull_qty",
      count("thoughtful"."_virt_filter_sk_9572960914066364") as "both_nonnull"
  FROM
      "thoughtful"
  GROUP BY
      1)
  SELECT
      "cooperative"."s_channel" as "s_channel",
      "questionable"."total_rows" as "total_rows",
      "cooperative"."nonnull_qty" as "nonnull_qty",
      "cooperative"."nonnull_lp" as "nonnull_lp",
      "cooperative"."both_nonnull" as "both_nonnull"
  FROM
      "questionable"
      INNER JOIN "cooperative" on "questionable"."s_channel" = "cooperative"."s_channel"]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting value: line 1 column 75 (char 74). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file write probe_lit.preql --run-and-delete`

  ```text
  Unexpected error in probe_lit.preql: (_duckdb.ConversionException) Conversion Error: Could not convert string 'esecallypri' to INT64 when casting from source column i_manufact

  LINE 16: ...alutin where highfalutin."profile_sk" is not distinct from "i_items"."I_MANUFACT")
                                                                         ^
  [SQL:
  WITH
  highfalutin as (
  SELECT
      "i_items"."I_ITEM_SK" as "profile_sk"
  FROM
      "item" as "i_items"
  WHERE
      ( ( "i_items"."I_CATEGORY" = 'Books' and "i_items"."I_COLOR" = 'tan' and "i_items"."I_UNITS" = 'Oz' and "i_items"."I_SIZE" = 'N/A' ) or ( "i_items"."I_CATEGORY" = 'Electronics' and "i_items"."I_COLOR" = 'purple' and "i_items"."I_UNITS" = 'Ton' and "i_items"."I_SIZE" = 'N/A' ) or ( "i_items"."I_CATEGORY" = 'Men' and "i_items"."I_COLOR" = 'misty' and "i_items"."I_UNITS" = 'Box' and "i_items"."I_SIZE" = 'medium' ) or ( "i_items"."I_CATEGORY" = 'Books' and "i_items"."I_COLOR" = 'medium' and "i_items"."I_UNITS" = 'Tsp' and "i_items"."I_SIZE" = 'N/A' ) or ( "i_items"."I_CATEGORY" = 'Books' and "i_items"."I_COLOR" = 'midnight' and "i_items"."I_UNITS" = 'Gram' and "i_items"."I_SIZE" = 'N/A' ) or ( "i_items"."I_CATEGORY" = 'Books' and "i_items"."I_COLOR" = 'pale' and "i_items"."I_UNITS" = 'Pound' and "i_items"."I_SIZE" = 'N/A' ) or ( "i_items"."I_CATEGORY" = 'Electronics' and "i_items"."I_COLOR" = 'khaki' and "i_items"."I_UNITS" = 'Pallet' and "i_items"."I_SIZE" = 'N/A' ) or ( "i_items"."I_CATEGORY" = 'Electronics' and "i_items"."I_COLOR" = 'mint' and "i_items"."I_UNITS" = 'Gross' and "i_items"."I_SIZE" = 'N/A' ) )
  )
  SELECT
      "i_items"."I_PRODUCT_NAME" as "i_product_name"
  FROM
      "item" as "i_items"
  WHERE
      "i_items"."I_MANUFACT_ID" >= 1 and "i_items"."I_MANUFACT_ID" <= 500 and exists (select 1 from highfalutin where highfalutin."profile_sk" is not distinct from "i_items"."I_MANUFACT")

  GROUP BY
      1
  ORDER BY
      "i_items"."I_PRODUCT_NAME" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/9h9h)
  ```
- `trilogy file write probe5_374591292.preql --run-and-delete`

  ```text
  Resolution error in probe5_374591292.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.store_sales_at_ss_item_sk_ss_ticket_number onto ss.item.items_at_ss_item_sk_at_ss_item_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_filtered_by_1108901128953984_grouped_by__at_abstract_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_filtered_by_1108901128953984_grouped_by_ss.item.sk_at_ss_item_sk_at_ss_item_sk_at_v1_item_sk_at_v1_item_sk_filtered_by_5862030560182170_at_v1_item_sk_at_qualifying_item_sk_join_ss.item.items_at_ss_item_sk_at_ss_item_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_filtered_by_1108901128953984_grouped_by__at_abstract_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_filtered_by_1108901128953984_grouped_by_ss.item.sk_at_ss_item_sk_at_ss_item_sk_at_v1_item_sk_at_v1_item_sk_filtered_by_5862030560182170_at_v1_item_sk_at_qualifying_item_sk_at_qualifying_item_sk_at_qualifying_item_sk_at_qualifying_item_sk_at_qualifying_item_sk_filtered_by_4971171135237854_at_best_item_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write answer_3553309440.preql --run`

  ```text
  Resolution error in answer_3553309440.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.customer.current_address.customer_address_at_ss_customer_current_address_sk_grouped_by_ss.customer.current_address.county_ss.customer.current_address.state_at_ss_customer_current_address_county_ss_customer_current_address_state onto cs.catalog_sales_at_cs_item_sk_cs_order_number_grouped_by_cs.billing_customer.sk_cs.item.sk_cs.sale_date.sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_join_cs.item.items_at_cs_item_sk_join_cs.sale_date.date_at_cs_sale_date_sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_filtered_by_2979763004092112_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_grouped_by_local.___tvf_arm_0_cid_at_local____tvf_arm_0_cid_at_local____tvf_arm_0_cid_union_ws.item.items_at_ws_item_sk_join_ws.sale_date.date_at_ws_sale_date_sk_join_ws.web_sales_at_ws_item_sk_ws_order_number_grouped_by_ws.billing_customer.sk_ws.item.sk_ws.sale_date.sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_filtered_by_7618068722719838_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_grouped_by_local.___tvf_arm_1_cid_at_local____tvf_arm_1_cid_at_local____tvf_arm_1_cid_unioned_at_qualifying_cid_join_ss.sale_date.date_at_ss_sale_date_sk_join_ss.store.store_at_ss_store_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_22867802696037, cs.catalog_sales_at_cs_item_sk_cs_order_number_grouped_by_cs.billing_customer.sk_cs.item.sk_cs.sale_date.sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_join_cs.item.items_at_cs_item_sk_join_cs.sale_date.date_at_cs_sale_date_sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_filtered_by_2979763004092112_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_grouped_by_local.___tvf_arm_0_cid_at_local____tvf_arm_0_cid_at_local____tvf_arm_0_cid_union_ws.item.items_at_ws_item_sk_join_ws.sale_date.date_at_ws_sale_date_sk_join_ws.web_sales_at_ws_item_sk_ws_order_number_grouped_by_ws.billing_customer.sk_ws.item.sk_ws.sale_date.sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_filtered_by_7618068722719838_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_grouped_by_local.___tvf_arm_1_cid_at_local____tvf_arm_1_cid_at_local____tvf_arm_1_cid_unioned_at_qualifying_cid_join_ss.sale_date.date_at_ss_sale_date_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_22867802696037_join_d.date_at_d_sk_grouped_by__at_abstract_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_2846670917549047_grouped_by_ss.customer.sk_at_ss_customer_sk_at_ss_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write answer_3553309440.preql --run`

  ```text
  Resolution error in answer_3553309440.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.customer.current_address.customer_address_at_ss_customer_current_address_sk_grouped_by_ss.customer.current_address.county_ss.customer.current_address.state_at_ss_customer_current_address_county_ss_customer_current_address_state onto _at_ss_sale_date_sk_at_abstract_join_cs.catalog_sales_at_cs_item_sk_cs_order_number_grouped_by_cs.billing_customer.sk_cs.item.sk_cs.sale_date.sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_join_cs.item.items_at_cs_item_sk_join_cs.sale_date.date_at_cs_sale_date_sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_filtered_by_7728443233863146_grouped_by_local.cs_buyers_at_local_cs_buyers_join_ss.sale_date.date_at_ss_sale_date_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_join_ws.item.items_at_ws_item_sk_join_ws.sale_date.date_at_ws_sale_date_sk_join_ws.web_sales_at_ws_item_sk_ws_order_number_grouped_by_ws.billing_customer.sk_ws.item.sk_ws.sale_date.sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_filtered_by_1756199520886489_grouped_by_local.ws_buyers_at_local_ws_buyers_at_ss_item_sk_ss_ticket_number_filtered_by_2364954959500146_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_2846670917549047_grouped_by_ss.customer.sk_at_ss_customer_sk_at_ss_customer_sk, cs.catalog_sales_at_cs_item_sk_cs_order_number_grouped_by_cs.billing_customer.sk_cs.item.sk_cs.sale_date.sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_join_cs.item.items_at_cs_item_sk_join_cs.sale_date.date_at_cs_sale_date_sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_filtered_by_7728443233863146_grouped_by_local.cs_buyers_at_local_cs_buyers_join_ss.sale_date.date_at_ss_sale_date_sk_join_ss.store.store_at_ss_store_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_join_ws.item.items_at_ws_item_sk_join_ws.sale_date.date_at_ws_sale_date_sk_join_ws.web_sales_at_ws_item_sk_ws_order_number_grouped_by_ws.billing_customer.sk_ws.item.sk_ws.sale_date.sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_filtered_by_1756199520886489_grouped_by_local.ws_buyers_at_local_ws_buyers_at_ss_item_sk_ss_ticket_number_filtered_by_2364954959500146. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_d.preql --run-and-delete`

  ```text
  Resolution error in probe_d.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.customer.current_address.customer_address_at_ss_customer_current_address_sk_grouped_by_ss.customer.current_address.county_ss.customer.current_address.state_at_ss_customer_current_address_county_ss_customer_current_address_state onto cs.catalog_sales_at_cs_item_sk_cs_order_number_grouped_by_cs.billing_customer.sk_cs.item.sk_cs.sale_date.sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_join_cs.item.items_at_cs_item_sk_join_cs.sale_date.date_at_cs_sale_date_sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_filtered_by_2979763004092112_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_grouped_by_local.___tvf_arm_0_cid_at_local____tvf_arm_0_cid_at_local____tvf_arm_0_cid_union_ws.item.items_at_ws_item_sk_join_ws.sale_date.date_at_ws_sale_date_sk_join_ws.web_sales_at_ws_item_sk_ws_order_number_grouped_by_ws.billing_customer.sk_ws.item.sk_ws.sale_date.sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_filtered_by_7618068722719838_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_grouped_by_local.___tvf_arm_1_cid_at_local____tvf_arm_1_cid_at_local____tvf_arm_1_cid_unioned_at_qualifying_cid_join_ss.sale_date.date_at_ss_sale_date_sk_join_ss.store.store_at_ss_store_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_grouped_by_ss.customer.sk_ss.sale_date.sk_ss.store.sk_ss.ticket_number_at_ss_customer_sk_ss_sale_date_sk_ss_store_sk_ss_ticket_number_at_ss_customer_sk_ss_sale_date_sk_ss_store_sk_ss_ticket_number_at_ss_customer_sk_ss_sale_date_sk_ss_store_sk_ss_ticket_number_filtered_by_22867802696037, cs.catalog_sales_at_cs_item_sk_cs_order_number_grouped_by_cs.billing_customer.sk_cs.item.sk_cs.sale_date.sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_join_cs.item.items_at_cs_item_sk_join_cs.sale_date.date_at_cs_sale_date_sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_filtered_by_2979763004092112_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_grouped_by_local.___tvf_arm_0_cid_at_local____tvf_arm_0_cid_at_local____tvf_arm_0_cid_union_ws.item.items_at_ws_item_sk_join_ws.sale_date.date_at_ws_sale_date_sk_join_ws.web_sales_at_ws_item_sk_ws_order_number_grouped_by_ws.billing_customer.sk_ws.item.sk_ws.sale_date.sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_filtered_by_7618068722719838_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_grouped_by_local.___tvf_arm_1_cid_at_local____tvf_arm_1_cid_at_local____tvf_arm_1_cid_unioned_at_qualifying_cid_join_ss.sale_date.date_at_ss_sale_date_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_grouped_by_ss.customer.sk_ss.sale_date.sk_ss.ticket_number_at_ss_customer_sk_ss_sale_date_sk_ss_ticket_number_at_ss_customer_sk_ss_sale_date_sk_ss_ticket_number_at_ss_customer_sk_ss_sale_date_sk_ss_ticket_number_filtered_by_22867802696037_join_ss.sale_date.date_at_ss_sale_date_sk_grouped_by__at_abstract_at_ss_customer_sk_ss_sale_date_sk_ss_ticket_number_at_ss_customer_sk_ss_sale_date_sk_ss_ticket_number_filtered_by_2846670917549047_grouped_by_ss.customer.sk_ss.ticket_number_at_ss_customer_sk_ss_ticket_number_grouped_by_ss.customer.sk_at_ss_customer_sk_at_ss_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_nocoalesce.preql --run-and-delete`

  ```text
  Syntax error in probe_nocoalesce.preql: ORDER BY references 'agg_1999.ws_cost', which is not in the SELECT projection (line 45). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --agg_1999.ws_cost order by agg_1999.ws_cost asc`.
  ```
- `trilogy file write probe_x.preql --run-and-delete`

  ```text
  Syntax error in probe_x.preql: Cannot join `all_chan.ch` to itself (`all_chan.ch` resolves to the same key `all_chan.ch`), which degenerates to `1=1`. Join distinct keys (e.g. separate rowset outputs or distinct expressions). (line 10, column 1)
  ```

### `undefined-concept`

- `trilogy file write answer_3863442186.preql --run`

  ```text
  Syntax error in answer_3863442186.preql: Undefined concept: s.line_value. Suggestions: ['line_value']
  ```
- `trilogy file write answer_2844519538.preql --run`

  ```text
  Syntax error in answer_2844519538.preql: Undefined concept: _virt_agg_sum_6819123508778543_wscope.
  ```
- `trilogy file write answer_1798498862.preql --run`

  ```text
  Syntax error in answer_1798498862.preql: Undefined concept: qs.cust_sk. Suggestions: ['qs.item_sk', 'qc.cust_sk', 'qual_store.cust_sk', 'qual_catalog.cust_sk']
  ```
- `trilogy file write answer_1798498862.preql --run`

  ```text
  Syntax error in answer_1798498862.preql: 4 undefined concept references; fix all before re-running:
    - item_code (line 38, col 10, in ORDER BY); did you mean: qual_store.item_code?
    - item_desc (line 38, col 25, in ORDER BY); did you mean: qual_store.item_desc, ss.item.desc, cs.item.desc?
    - store_code (line 38, col 40, in ORDER BY); did you mean: qual_store.store_code, qual_store.item_code, ss.store_credit?
    - store_name (line 38, col 56, in ORDER BY); did you mean: qual_store.store_name, ss.store.name, store_sale_qty, ss.store.street_name?
  ```
- `trilogy file write answer_2802535988.preql --run`

  ```text
  Syntax error in answer_2802535988.preql: 11 undefined concept references; fix all before re-running:
    - salutation (line 27, col 5, in ORDER BY); did you mean: ws.ship_customer.salutation, ws.return_customer.salutation, ws.billing_customer.salutation, ws.return_refund_customer.salutation?
    - first_name (line 28, col 5, in ORDER BY); did you mean: ws.ship_customer.first_name, ws.return_customer.first_name, ws.billing_customer.first_name, ws.return_refund_customer.first_name?
    - last_name (line 29, col 5, in ORDER BY); did you mean: ws.ship_customer.last_name, ws.return_customer.last_name, ws.billing_customer.last_name, ws.return_refund_customer.last_name?
    - preferred_cust_flag (line 30, col 5, in ORDER BY); did you mean: ws.ship_customer.preferred_cust_flag, ws.return_customer.preferred_cust_flag, ws.billing_customer.preferred_cust_flag, ws.return_refund_customer.preferred_cust_flag?
    - birth_day (line 31, col 5, in ORDER BY); did you mean: ws.ship_customer.birth_day, ws.return_customer.birth_day, ws.billing_customer.birth_day, ws.return_refund_customer.birth_day?
    - birth_month (line 32, col 5, in ORDER BY); did you mean: ws.ship_customer.birth_month, ws.return_customer.birth_month, ws.billing_customer.birth_month, ws.return_refund_customer.birth_month?
    - birth_year (line 33, col 5, in ORDER BY); did you mean: ws.ship_customer.birth_year, ws.return_customer.birth_year, ws.billing_customer.birth_year, ws.return_refund_customer.birth_year?
    - birth_country (line 34, col 5, in ORDER BY); did you mean: ws.ship_customer.birth_country, ws.return_customer.birth_country, ws.billing_customer.birth_country, ws.return_refund_customer.birth_country?
    - login (line 35, col 5, in ORDER BY); did you mean: ws.ship_customer.login, ws.return_customer.login, ws.billing_customer.login, ws.return_refund_customer.login?
    - email_address (line 36, col 5, in ORDER BY); did you mean: ws.ship_customer.email_address, ws.return_customer.email_address, ws.billing_customer.email_address, ws.return_refund_customer.email_address?
    - last_review_date (line 37, col 5, in ORDER BY); did you mean: ws.ship_customer.last_review_date, ws.return_customer.last_review_date, ws.billing_customer.last_review_date, ws.return_refund_customer.last_review_date?
  ```
- `trilogy file write verify_374591292.preql --run-and-delete`

  ```text
  Syntax error in verify_374591292.preql: Undefined concept: worst.item_avg_profit. Suggestions: ['worst.item_sk', 'item_avg_profit', 'v1.item_avg_profit', 'qualifying.item_avg_profit']
  ```
- `trilogy file write probe7_968172492.preql --run-and-delete`

  ```text
  Syntax error in probe7_968172492.preql: Undefined concept: ws.billing_customer.current_address. Suggestions: ['ws.billing_customer.current_address.sk', 'ws.billing_customer.current_address.id', 'ws.billing_customer.current_address.street_number', 'ws.billing_customer.current_address.street_name', 'ws.billing_customer.current_address.street_type', 'ws.billing_customer.current_address.suite_number']
  ```
- `trilogy file write probe_cte.preql --run-and-delete`

  ```text
  Syntax error in probe_cte.preql: 2 undefined concept references; fix all before re-running:
    - monthly_totals.store_name (line 17, col 5, in SELECT); did you mean: monthly_totals.ss.store.name, monthly_totals.ss.store.company_name, monthly_totals.ss.item.brand_name?
    - monthly_totals.store_name (line 32, col 5, in ORDER BY); did you mean: monthly_totals.ss.store.name, monthly_totals.ss.store.company_name, monthly_totals.ss.item.brand_name?
  ```
- `trilogy file write answer_3281742578.preql --run`

  ```text
  Syntax error in answer_3281742578.preql: 2 undefined concept references; fix all before re-running:
    - ws.wh_count (line 6, col 7, in WHERE); did you mean: ws.row_counter, ws.web_page.char_count, ws.warehouse.county, wh_count?
    - ws.returned_lines (line 7, col 7, in WHERE); did you mean: ws.return_time.sk, ws.return_net_loss, ws.return_fee, returned_lines?
  ```

### `join-resolution`

- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Resolution error in probe3.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 21). The requested concepts split into 2 disconnected subgraphs: {cs.billing_customer.sk, cs.item.sk, cat_lines, cat_qty, cat_years}; {qual_store.item_code, qual_store.item_desc, qual_store.sr_qty, qual_store.ss_qty, qual_store.store_code, qual_store.store_name}. Are you missing a join or merge statement to relate them?
  Note: the membership predicate(s) `(cs.billing_customer.sk, cs.item.sk) in (_subquery_22_5.qual_store.cust_sk, _subquery_22_5.qual_store.item_sk)` span these subgraphs, but membership only filters rows on its left side — it does not join the two sides, so it cannot relate them for outputs or grouping. To combine values from both sides, author a query-scoped join or a merge on shared keys.
  ```
- `trilogy file write probe_items.preql --run-and-delete`

  ```text
  Resolution error in probe_items.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 3 disconnected subgraphs: {i.current_price, i.manufacturer_id, item_code, item_sk, manuf, price}; {n_cs_lines}; {n_qual_inv}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_items.preql --run-and-delete`

  ```text
  Resolution error in probe_items.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {inv.item.current_price, inv.item.manufacturer_id, item_code, item_sk, manuf, n_qual_inv, price}; {n_cs_lines}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_setops.preql --run-and-delete`

  ```text
  Resolution error in probe_setops.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {_catalog_only_cust_sk}; {_catalog_only_item_sk}. Are you missing a join or merge statement to relate them?
  ```

### `cli-misuse`

- `trilogy explore raw/store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_returns.preql' does not exist.
  ```
