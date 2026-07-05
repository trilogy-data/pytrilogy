# Trilogy failure analysis — 20260705-200535

- Run `20260705-200535` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1318 | failed: 123 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 70 | 57% |
| `syntax-parse` | 41 | 33% |
| `syntax-missing-alias` | 4 | 3% |
| `cli-misuse` | 4 | 3% |
| `planner-recursion` | 2 | 2% |
| `type-error` | 1 | 1% |
| `undefined-concept` | 1 | 1% |

## Detail

### `other`

- `trilogy run query04.preql duckdb`

  ```text
  Syntax error in query04.preql: Nothing was executed: parsed 7 definition statement(s) (4 rowsets, 3 imports) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run query04.preql duckdb`

  ```text
  Syntax error in query04.preql: Nothing was executed: parsed 10 definition statement(s) (7 rowsets, 3 imports) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run query04.preql duckdb`

  ```text
  Unexpected error in query04.preql: Conflicting join types (full, left outer) on keys joined into one group: a FULL/UNION join cannot be mixed with another type on the same key (it is ambiguous whether the key is required or one-sided). Make the whole group one type (e.g. `UNION JOIN a = b = c`), or use a distinct key.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 73 column 12 (char 2913). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(combined.channel)` (line 55), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel) as g order by g desc`.
  ```
- `trilogy file read query05.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Syntax error in query08.preql: Undefined concept: customer.preferred_cust_flag. Suggestions: ['ss.customer.preferred_cust_flag', 'ss.return_customer.preferred_cust_flag', 'ss.customer.first_name']
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Syntax error in query08.preql: Comparison `ss.date.quarter = 'Q2'` can never match enum field 'ss.date.quarter', which contains only these values: 1, 2, 3, 4. It is always false and should be removed.
  ```
- `trilogy file read query08.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write query10.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Customers who had at …aphics.employed_dependent_count as employed_dependent_count,
    ss.customer.demographics.college_dependent_count as college_dependent_count
;
 stdin`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run query10.preql`

  ```text
  Syntax error in query10.preql: Comparison `ss.date.month_of_year >= 1` matches every value of enum field 'ss.date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy run query10.preql`

  ```text
  Syntax error in query10.preql: Undefined concept: local.store_custs. Suggestions: ['store_custs.cid', '_store_custs_cid', 'store_custs.gender']
  ```
- `trilogy file read query10.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query13.preql`

  ```text
  Syntax error in query13.preql: 6 undefined concept references; fix all before re-running:
    - store_sales.customer_demographics.marital_status (line 7, col 8, in WHERE); did you mean: store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status, store_sales.customer_demographic.marital_status?
    - store_sales.customer_demographics.education_status (line 8, col 12, in WHERE); did you mean: store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status, store_sales.customer_demographic.education_status?
    - store_sales.customer_demographics.marital_status (line 12, col 8, in WHERE); did you mean: store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status, store_sales.customer_demographic.marital_status?
    - store_sales.customer_demographics.education_status (line 13, col 12, in WHERE); did you mean: store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status, store_sales.customer_demographic.education_status?
    - store_sales.customer_demographics.marital_status (line 17, col 8, in WHERE); did you mean: store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status, store_sales.customer_demographic.marital_status?
    - store_sales.customer_demographics.education_status (line 18, col 12, in WHERE); did you mean: store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status, store_sales.customer_demographic.education_status?
  ```
- `trilogy run query14.preql duckdb`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(local.channel)` (line 26), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.channel) as g order by g desc`.
  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query16.preql`

  ```text
  Syntax error in query16.preql: Undefined concept: raw.catalog_returns.order_number (line 16, col 30, in WHERE). Suggestions: ['cs.order_number', 'multi_warehouse.cs.order_number']
  ```
- `trilogy run query18.preql`

  ```text
  Syntax error in query18.preql: ORDER BY contains aggregate `grouping(cs.bill_customer.address.country)` (line 3), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(cs.bill_customer.address.country) as g order by g desc`.
  ```
- `trilogy run query20.preql`

  ```text
  Syntax error in query20.preql: 4 undefined concept references; fix all before re-running:
    - item_category (line 28, col 5, in ORDER BY); did you mean: item_totals.item_category, item.category, item.category_id, cs.item.category?
    - item_class (line 29, col 5, in ORDER BY); did you mean: item_totals.item_class, item.class, item.class_id, cs.item.class?
    - item_code (line 30, col 5, in ORDER BY); did you mean: item_totals.item_code, item.desc, item.color, item.id?
    - item_desc (line 31, col 5, in ORDER BY); did you mean: item_totals.item_desc, item.desc, cs.item.desc, item.id?
  ```
- `trilogy run query23.preql`

  ```text
  Unexpected error in query23.preql: Concept max_total purpose Purpose.METRIC does not match declared purpose Purpose.PROPERTY. Suggest defaulting to "auto"
  ```
- `trilogy file read query23.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query25.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query26.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query27.preql`

  ```text
  Syntax error in query27.preql: Comparison `ss.customer_demographic.marital_status = 'Single'` can never match enum field 'ss.customer_demographic.marital_status', which contains only these values: 'M', 'S', 'D', 'W', 'U'. It is always false and should be removed.
  ```
- `trilogy run query29.preql`

  ```text
  Syntax error in query29.preql: Comparison `sr.return_date.month_of_year <= 12` matches every value of enum field 'sr.return_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy run query29.preql`

  ```text
  Syntax error in query29.preql: 4 undefined concept references; fix all before re-running:
    - store_sales_filtered.store_return_qty (line 49, col 5, in SELECT); did you mean: store_returns_filtered.store_return_qty, store_sales_filtered.store_sale_qty, _store_returns_filtered_store_return_qty?
    - store_sales_filtered.catalog_sale_qty (line 50, col 5, in SELECT); did you mean: catalog_sales_filtered.catalog_sale_qty, store_sales_filtered.store_sale_qty, _catalog_sales_filtered_catalog_sale_qty?
    - store_sales_filtered.store_return_qty (line 41, col 7, in WHERE); did you mean: store_returns_filtered.store_return_qty, store_sales_filtered.store_sale_qty, _store_returns_filtered_store_return_qty?
    - store_sales_filtered.catalog_sale_qty (line 42, col 7, in WHERE); did you mean: catalog_sales_filtered.catalog_sale_qty, store_sales_filtered.store_sale_qty, _catalog_sales_filtered_catalog_sale_qty?
  ```
- `trilogy file read query29.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query31.preql`

  ```text
  Resolution error in query31.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 23). The requested concepts split into 2 disconnected subgraphs: {store_by_county_q.county, store_by_county_q.quarter, store_by_county_q.store_ext_total}; {web_by_county_q.county}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query31.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query32.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query37.preql`

  ```text
  Syntax error in query37.preql: 5 undefined concept references; fix all before re-running:
    - item.current_price (line 14, col 3, in SELECT); did you mean: cs.item.current_price, inv.item.current_price, item.text_id?
    - item.current_price (line 6, col 3, in WHERE); did you mean: cs.item.current_price, inv.item.current_price, item.text_id?
    - item.manufacturer_id (line 7, col 7, in WHERE); did you mean: cs.item.manufacturer_id, inv.item.manufacturer_id, cs.item.manager_id?
    - item.id (line 10, col 7, in WHERE); did you mean: cs.item.id, inv.item.id, cs.date.id, cs.ship_date.id, cs.sold_date.id, cs.time.id?
    - item.text_id (line 16, col 3, in ORDER BY); did you mean: cs.item.text_id, inv.item.text_id, cs.date.text_id, cs.ship_date.text_id, cs.sold_date.text_id, cs.time.text_id?
  ```
- `trilogy file read query37.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs select count(ss.customer.last_name) as cnt where ss.date.year = 2000 and cs.sold_date.y…stomer.last_name = cs.bill_customer.last_name and ss.customer.first_name = cs.bill_customer.first_name and ss.date.date = cs.sold_date.date limit 10;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {cs.bill_customer.first_name, cs.bill_customer.id, cs.bill_customer.last_name, cs.sold_date.date, cs.sold_date.year}; {ss.customer.first_name, ss.customer.id, ss.customer.last_name, ss.date.date, ss.date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: 2 undefined concept references; fix all before re-running:
    - best_name (line 54, col 5, in ORDER BY); did you mean: best_performers.best_name?
    - worst_name (line 55, col 5, in ORDER BY); did you mean: worst_performers.worst_name, ss.store.street_name?
  ```
- `trilogy run query44.preql`

  ```text
  Unexpected error in query44.preql: (_duckdb.BinderException) Binder Error: column "ss_item_id" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(ss_item_id)" if the exact value of "ss_item_id" is not important.

  LINE 39: ..."."threshold" and "questionable"."ss_store_id" = 1 THEN "questionable"."ss_item_id" ELSE NULL END as "_virt_filter_...
                                                                      ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
      "ss_store_sales"."SS_STORE_SK" as "ss_store_id",
      CASE WHEN "ss_store_sales"."SS_STORE_SK" = 1 THEN "ss_store_sales"."SS_NET_PROFIT" ELSE NULL END as "_virt_filter_net_profit_5663507918641416",
      CASE WHEN "ss_store_sales"."SS_STORE_SK" = 1 and "ss_store_sales"."SS_ADDR_SK" is null THEN "ss_store_sales"."SS_NET_PROFIT" ELSE NULL END as "_virt_filter_net_profit_6875718047322987"
  FROM
      "store_sales" as "ss_store_sales"),
  cheerful as (
  SELECT
      "wakeful"."ss_item_id" as "ss_item_id",
      avg("wakeful"."_virt_filter_net_profit_5663507918641416") as "_virt_agg_avg_7099385623960808",
      avg("wakeful"."_virt_filter_net_profit_6875718047322987") as "_virt_agg_avg_4335960860042687"
  FROM
      "wakeful"
  GROUP BY
      1),
  questionable as (
  SELECT
      "cheerful"."_virt_agg_avg_4335960860042687" * 0.9 as "threshold",
      "wakeful"."_virt_filter_net_profit_5663507918641416" as "_virt_filter_net_profit_5663507918641416",
      "wakeful"."ss_item_id" as "ss_item_id",
      "wakeful"."ss_store_id" as "ss_store_id"
  FROM
      "wakeful"
      INNER JOIN "cheerful" on "wakeful"."ss_item_id" = "cheerful"."ss_item_id"),
  thoughtful as (
  SELECT
      "cheerful"."ss_item_id" as "ss_item_id",
      rank() over (order by "cheerful"."_virt_agg_avg_7099385623960808" asc ) as "_item_ranks_rnk_asc",
      rank() over (order by "cheerful"."_virt_agg_avg_7099385623960808" desc ) as "_item_ranks_rnk_desc"
  FROM
      "cheerful"),
  abundant as (
  SELECT
      "questionable"."ss_store_id" as "ss_store_id",
      CASE WHEN avg("questionable"."_virt_filter_net_profit_5663507918641416") > "questionable"."threshold" and "questionable"."ss_store_id" = 1 THEN "questionable"."ss_item_id" ELSE NULL END as "_virt_filter_id_9066172145544209"
  FROM
      "questionable"
  GROUP BY
      1,
      "questionable"."threshold"
  HAVING
      avg("questionable"."_virt_filter_net_profit_5663507918641416") > "questionable"."threshold"
  ),
  cooperative as (
  SELECT
      "ss_item_items"."I_ITEM_SK" as "ss_item_id",
      "ss_item_items"."I_PRODUCT_NAME" as "ss_item_product_name",
      "thoughtful"."_item_ranks_rnk_asc" as "_item_ranks_rnk_asc",
      "thoughtful"."_item_ranks_rnk_desc" as "_item_ranks_rnk_desc"
  FROM
      "thoughtful"
      INNER JOIN "item" as "ss_item_items" on "thoughtful"."ss_item_id" = "ss_item_items"."I_ITEM_SK"),
  uneven as (
  SELECT
      "abundant"."_virt_filter_id_9066172145544209" as "_virt_filter_id_9066172145544209"
  FROM
      "abundant"
  WHERE
      "abundant"."ss_store_id" = 1
  ),
  yummy as (
  SELECT
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id"
  FROM
      "store_sales" as "ss_store_sales"
  WHERE
      "ss_store_sales"."SS_STORE_SK" = 1 and "ss_store_sales"."SS_ITEM_SK" in (select uneven."_virt_filter_id_9066172145544209" from uneven where uneven."_virt_filter_id_9066172145544209" is not null)

  GROUP BY
      1),
  vacuous as (
  SELECT
      "cooperative"."_item_ranks_rnk_asc" as "_item_ranks_rnk_asc",
      "cooperative"."_item_ranks_rnk_desc" as "_item_ranks_rnk_desc",
      "cooperative"."ss_item_product_name" as "_item_ranks_product_name"
  FROM
      "yummy"
      INNER JOIN "cooperative" on "yummy"."ss_item_id" = "cooperative"."ss_item_id"
  WHERE
      "cooperative"."ss_item_id" in (select uneven."_virt_filter_id_9066172145544209" from uneven where uneven."_virt_filter_id_9066172145544209" is not null)
  ),
  concerned as (
  SELECT
      "vacuous"."_item_ranks_product_name" as "item_ranks_product_name",
      "vacuous"."_item_ranks_rnk_asc" as "item_ranks_rnk_asc",
      "vacuous"."_item_ranks_rnk_desc" as "item_ranks_rnk_desc"
  FROM
      "vacuous"),
  sweltering as (
  SELECT
      "concerned"."item_ranks_product_name" as "_worst_worst_name",
      "concerned"."item_ranks_rnk_desc" as "_worst_pair_rank"
  FROM
      "concerned"
  WHERE
      "concerned"."item_ranks_rnk_desc" <= 10

  GROUP BY
      1,
      2
  ORDER BY
      "_worst_pair_rank" asc
  LIMIT (10)),
  young as (
  SELECT
      "concerned"."item_ranks_product_name" as "_best_best_name",
      "concerned"."item_ranks_rnk_asc" as "_best_pair_rank"
  FROM
      "concerned"
  WHERE
      "concerned"."item_ranks_rnk_asc" <= 10

  GROUP BY
      1,
      2
  ORDER BY
      "_best_pair_rank" asc
  LIMIT (10)),
  macho as (
  SELECT
      "sweltering"."_worst_pair_rank" as "worst_pair_rank",
      "sweltering"."_worst_worst_name" as "worst_worst_name"
  FROM
      "sweltering"),
  abhorrent as (
  SELECT
      "young"."_best_best_name" as "best_best_name",
      "young"."_best_pair_rank" as "best_pair_rank"
  FROM
      "young")
  SELECT
      coalesce("abhorrent"."best_pair_rank","macho"."worst_pair_rank") as "rank",
      "abhorrent"."best_best_name" as "best_best_name",
      "macho"."worst_worst_name" as "worst_worst_name"
  FROM
      "macho"
      FULL JOIN "abhorrent" on "macho"."worst_pair_rank" = "abhorrent"."best_pair_rank"
  ORDER BY
      "rank" asc,
      "abhorrent"."best_best_name" desc,
      "macho"."worst_worst_name" desc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 16). The requested concepts split into 2 disconnected subgraphs: {item_avgs.item_id}; {ss.item.id, ss.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: Undefined concept: ss_item.item_id. Suggestions: ['item_ranks.item_id', 'best.item_ranks.item_id', 'worst.item_ranks.item_id', 'ss.item.id', '_item_ranks_item_id']
  ```
- `trilogy run query44.preql`

  ```text
  Unexpected error in query44.preql: (_duckdb.BinderException) Binder Error: column "ss_item_id" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(ss_item_id)" if the exact value of "ss_item_id" is not important.

  LINE 38: ...rofit") > "yummy"."thr" and "yummy"."ss_store_id" = 1 THEN "yummy"."ss_item_id" ELSE NULL END as "_virt_filter_id_3312...
                                                                         ^
  [SQL:
  WITH
  abundant as (
  SELECT
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
      avg(CASE WHEN "ss_store_sales"."SS_STORE_SK" = 1 and "ss_store_sales"."SS_ADDR_SK" is null THEN "ss_store_sales"."SS_NET_PROFIT" ELSE NULL END) as "_virt_agg_avg_4335960860042687"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1),
  wakeful as (
  SELECT
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
      avg("ss_store_sales"."SS_NET_PROFIT") as "_virt_agg_avg_7907292611450278"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1),
  yummy as (
  SELECT
      "abundant"."_virt_agg_avg_4335960860042687" * 0.9 as "thr",
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
      "ss_store_sales"."SS_NET_PROFIT" as "ss_net_profit",
      "ss_store_sales"."SS_STORE_SK" as "ss_store_id"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "abundant" on "ss_store_sales"."SS_ITEM_SK" = "abundant"."ss_item_id"),
  cheerful as (
  SELECT
      "wakeful"."ss_item_id" as "ss_item_id",
      rank() over (order by "wakeful"."_virt_agg_avg_7907292611450278" asc ) as "_best_items_rnk",
      rank() over (order by "wakeful"."_virt_agg_avg_7907292611450278" desc ) as "_worst_items_rnk"
  FROM
      "wakeful"),
  juicy as (
  SELECT
      "yummy"."ss_store_id" as "ss_store_id",
      CASE WHEN avg("yummy"."ss_net_profit") > "yummy"."thr" and "yummy"."ss_store_id" = 1 THEN "yummy"."ss_item_id" ELSE NULL END as "_virt_filter_id_3312939458270936"
  FROM
      "yummy"
  GROUP BY
      1,
      "yummy"."thr"
  HAVING
      avg("yummy"."ss_net_profit") > "yummy"."thr"
  ),
  thoughtful as (
  SELECT
      "cheerful"."_best_items_rnk" as "_best_items_rnk",
      "cheerful"."_worst_items_rnk" as "_worst_items_rnk",
      "ss_item_items"."I_ITEM_SK" as "ss_item_id",
      "ss_item_items"."I_PRODUCT_NAME" as "ss_item_product_name"
  FROM
      "cheerful"
      INNER JOIN "item" as "ss_item_items" on "cheerful"."ss_item_id" = "ss_item_items"."I_ITEM_SK"),
  vacuous as (
  SELECT
      "juicy"."_virt_filter_id_3312939458270936" as "_virt_filter_id_3312939458270936"
  FROM
      "juicy"
  WHERE
      "juicy"."ss_store_id" = 1
  ),
  cooperative as (
  SELECT
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id"
  FROM
      "store_sales" as "ss_store_sales"
  WHERE
      "ss_store_sales"."SS_STORE_SK" = 1 and "ss_store_sales"."SS_ITEM_SK" in (select vacuous."_virt_filter_id_3312939458270936" from vacuous where vacuous."_virt_filter_id_3312939458270936" is not null)

  GROUP BY
      1),
  concerned as (
  SELECT
      "thoughtful"."_best_items_rnk" as "_best_items_rnk",
      "thoughtful"."_worst_items_rnk" as "_worst_items_rnk",
      "thoughtful"."ss_item_product_name" as "_best_items_product_name",
      "thoughtful"."ss_item_product_name" as "_worst_items_product_name"
  FROM
      "cooperative"
      INNER JOIN "thoughtful" on "cooperative"."ss_item_id" = "thoughtful"."ss_item_id"
  WHERE
      "thoughtful"."ss_item_id" in (select vacuous."_virt_filter_id_3312939458270936" from vacuous where vacuous."_virt_filter_id_3312939458270936" is not null)
  ),
  young as (
  SELECT
      "concerned"."_best_items_product_name" as "_best_items_product_name",
      "concerned"."_best_items_rnk" as "_best_items_rnk",
      "concerned"."_worst_items_product_name" as "_worst_items_product_name",
      "concerned"."_worst_items_rnk" as "_worst_items_rnk"
  FROM
      "concerned"
  ORDER BY
      "concerned"."_worst_items_rnk" asc
  LIMIT (10)),
  abhorrent as (
  SELECT
      "young"."_worst_items_product_name" as "worst_items_product_name",
      "young"."_worst_items_rnk" as "worst_items_rnk"
  FROM
      "young"),
  sparkling as (
  SELECT
      "young"."_best_items_product_name" as "best_items_product_name",
      "young"."_best_items_rnk" as "best_items_rnk"
  FROM
      "young")
  SELECT
      coalesce("abhorrent"."worst_items_rnk","sparkling"."best_items_rnk") as "rank",
      "sparkling"."best_items_product_name" as "best_name",
      "abhorrent"."worst_items_product_name" as "worst_name"
  FROM
      "abhorrent"
      FULL JOIN "sparkling" on "abhorrent"."worst_items_rnk" = "sparkling"."best_items_rnk"
  GROUP BY
      1,
      2,
      3
  ORDER BY
      "rank" asc,
      "best_name" desc,
      "worst_name" desc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read raw/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_sales:store_sales select date.day_of_week limit 100;`

  ```text
  Syntax error in stdin: Undefined concept: date.day_of_week (line 2, col 8, in SELECT). Suggestions: ['store_sales.date.day_of_week', 'store_sales.store.date.day_of_week', 'store_sales.return_store.date.day_of_week', 'store_sales.return_date.day_of_week', 'store_sales.customer.first_sales_date.day_of_week', 'store_sales.customer.first_shipto_date.day_of_week']
  ```
- `trilogy file read query49.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query50.preql`

  ```text
  Syntax error in query50.preql: Undefined concept: ss.return_date. Suggestions: ['ss.return_date.id', 'ss.return_date.text_id', 'ss.return_date._date_string', 'ss.return_date.date', 'ss.return_date.year', 'ss.return_date.day_of_week']
  ```
- `trilogy file read query52.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query53.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query54.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query57.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query62.preql`

  ```text
  Syntax error in query62.preql: 5 undefined concept references; fix all before re-running:
    - warehouse.name (line 4, in SELECT); did you mean: web.warehouse.name, web.web_site.name, web_site.name, warehouse.id, warehouse_name_prefix?
    - ship_date.year (line 5, col 3, in WHERE); did you mean: web.ship_date.year, web.date.year, web.return_date.year, web.billing_customer.first_sales_date.year, web.billing_customer.first_shipto_date.year, web.ship_customer.first_sales_date.year?
    - warehouse.id (line 6, col 7, in WHERE); did you mean: web.warehouse.id, web.item.id, web.date.id, web.ship_date.id, web.return_date.id, web.time.id?
    - ship_mode.id (line 7, col 7, in WHERE); did you mean: web.ship_mode.id, web.item.id, web.date.id, web.ship_date.id, web.return_date.id, web.time.id?
    - web_site.id (line 8, col 7, in WHERE); did you mean: web.web_site.id, web.item.id, web.date.id, web.ship_date.id, web.return_date.id, web.time.id?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: agg_1999.item_id. Suggestions: ['agg_sales.item_id', 'agg_2000.item_id', 'agg_1999.store_zip']
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: ORDER BY references 'ss.store.id', which is not in the SELECT projection (line 6). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.store.id order by ss.store.id asc`.
  ```
- `trilogy file read query69.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query72.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query74.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query77.preql duckdb`

  ```text
  Syntax error in query77.preql: Undefined concept: s.channel_label (line 12, in SELECT). Suggestions: ['channel_label', 's.channel', 's.channel_dim_id']
  ```
- `trilogy file read query78.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query80.preql duckdb`

  ```text
  Syntax error in query80.preql: ORDER BY contains aggregate `grouping(combined.channel)` (line 44), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel) as g order by g desc`.
  ```
- `trilogy run query82.preql`

  ```text
  Resolution error in query82.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {ss.item.current_price, ss.item.desc, ss.item.manufacturer_id, ss.item.text_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query82.preql`

  ```text
  Resolution error in query82.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {item.current_price, item.manufacturer_id, item.text_id, description, item_code, price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query82.preql`

  ```text
  Resolution error in query82.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {item.current_price, item.manufacturer_id, item.text_id, description, item_code, price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query82.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query84.preql`

  ```text
  Unexpected error in query84.preql: Cannot join `ret.ticket_number` to itself (`ret.ticket_number` resolves to the same key `ret.ticket_number`), which degenerates to `1=1`. Join distinct keys (e.g. separate rowset outputs or distinct expressions).
  ```
- `trilogy run query84.preql`

  ```text
  Resolution error in query84.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {cust.address.city, cust.demographics.id, cust.household_demographic.income_band.lower_bound, cust.household_demographic.income_band.upper_bound, customer_code, full_name}; {ret.customer_demographic.id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query85.preql duckdb`

  ```text
  Syntax error in query85.preql: Comparison `wr.returning_demographic.marital_status = 'Married'` can never match enum field 'wr.returning_demographic.marital_status', which contains only these values: 'M', 'S', 'D', 'W', 'U'. It is always false and should be removed.
  ```
- `trilogy file read query87.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query93.preql`

  ```text
  Syntax error in query93.preql: ORDER BY references 'ss.customer.id', which is not in the SELECT projection (line 14). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.customer.id order by ss.customer.id asc`.
  ```
- `trilogy run query93.preql`

  ```text
  Syntax error in query93.preql: ORDER BY references 'local.customer_sort', which is not in the SELECT projection (line 17). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.customer_sort order by local.customer_sort asc`.
  ```
- `trilogy run query95.preql`

  ```text
  Syntax error in query95.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 9). Move the filter to WHERE to filter before aggregation.
  ```
- `trilogy run query97.preql`

  ```text
  Syntax error in query97.preql: Undefined concept: in_store.
  ```
- `trilogy file read query97.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query01.preql --content import raw.store_returns as sr;

auto cust_store_returns <- sum(sr.return_amount) by sr.billing_customer.id, sr.st…d
    --cust_store_returns,
    --store_avg_return
having
    cust_store_returns > 1.2 * store_avg_return
order by
    billing_customer_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:5
     |
  11 |     --cust_store_returns,
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...id as billing_customer_id     ??? --cust_store_returns,     --st...

  Write stats: received 464 chars / 464 bytes; tail: …'store_avg_return\\norder by\\n    billing_customer_id\\nlimit 100;'.
  ```
- `trilogy run --import raw.all_sales:s with cur as where s.channel in ('WEB', 'CATALOG') select s.date.week_seq, s.date.day_of_week, sum(s.ext_sales_price) as …d(cur.sales_amt, 53) over (partition by cur.day_of_week order by cur.week_seq) as fut_sales from cur order by cur.week_seq, cur.day_of_week limit 20;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...by cur.week_seq) as fut_sales ??? from cur order by cur.week_seq...
  ```
- `trilogy file write query04.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Store: annual value p…f_flag,
union join store_annual.cust_id = catalog_annual.cust_id = web_annual.cust_id
union join store_annual.yr = catalog_annual.yr = web_annual.yr
`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...log_annual.yr = web_annual.yr ???

  Write stats: received 1360 chars / 1360 bytes; tail: …'on join store_annual.yr = catalog_annual.yr = web_annual.yr\\n'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as s;

# Channel label and entity prefix mapping
auto channel_label <- case
    when s.channe…d.entity_id)
order by
    _g_channel asc,
    combined.channel asc nulls first,
    _g_entity asc,
    combined.entity_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 55:4
     |
  55 | -- grouping(combined.channel) as _g_channel,
     |    ^---
     |
     = expected PURPOSE, PROPERTY, UNIQUE, or AUTO
  Location:
   # Final query with rollup -- ??? grouping(combined.channel) as

  Write stats: received 2934 chars / 2934 bytes; tail: …'tity asc,\\n    combined.entity_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.all_sales:s select distinct s.channel, s.channel_dim_text_id as sale_entity, s.return_channel_dim_text_id as return_entity where s.channel = 'CATALOG' and s.return_channel_dim_id is not null and s.channel_dim_text_id != s.return_channel_dim_text_id limit 10;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...rt raw.all_sales as s; select ??? distinct s.channel, s.channel_...
  ```
- `trilogy file write query08.preql --content import raw.store_sales as ss;

parameter zips string;

# Param ZIP list as array
auto param_zip_set <- split(zips,… 1, 2) in qualifying_zip_prefix
select
    ss.store.name as store_name,
    sum(ss.net_profit) as total_net_profit
order by
    store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:38
     |
  14 | auto qualifying_zip <- pref_cust_zip where pref_cust_zip in param_zip_set;
     |                                      ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...alifying_zip <- pref_cust_zip ??? where pref_cust_zip in param_z...

  Write stats: received 950 chars / 950 bytes; tail: …'ofit) as total_net_profit\\norder by\\n    store_name\\nlimit 100;'.
  ```
- `trilogy file write query08.preql --content import raw.store_sales as ss;

parameter zips string;

# Param ZIP list as array
auto param_zip_set <- split(zips,… 1, 2) in qualifying_zip_prefix
select
    ss.store.name as store_name,
    sum(ss.net_profit) as total_net_profit
order by
    store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:36
     |
  16 | auto eligible_zip <- pref_cust_zip where pref_cust_zip in param_zips;
     |                                    ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...eligible_zip <- pref_cust_zip ??? where pref_cust_zip in param_z...

  Write stats: received 1044 chars / 1044 bytes; tail: …'ofit) as total_net_profit\\norder by\\n    store_name\\nlimit 100;'.
  ```
- `trilogy file write query10.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Store customers in Ja…, marital_status, education_status, purchase_estimate, credit_rating, dependent_count, employed_dependent_count, college_dependent_count
limit 100
;
`

  ```text
  refused to write 'query10.preql': not syntactically valid Trilogy.

  Parse error:
    --> 39:1
     |
  39 | where web_custs.cid is not null
     | ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
   join store_custs = web_custs ??? where web_custs.cid is not nul...

  Write stats: received 4139 chars / 4139 bytes; tail: …'ployed_dependent_count, college_dependent_count\\nlimit 100\\n;\\n'.
  ```
- `trilogy file write query13.preql --content import raw.store_sales as store_sales;

# Year 2001
where store_sales.date.year = 2001

# Demographic-and-price cl…ext_sales_price,
  avg(store_sales.ext_wholesale_cost) as avg_ext_wholesale_cost,
  sum(store_sales.ext_wholesale_cost) as total_ext_wholesale_cost
;`

  ```text
  refused to write 'query13.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:1
    |
  8 | where (
    | ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...+ state/net_profit conditions ??? where (   # Combination 1: mar...

  Write stats: received 1945 chars / 1945 bytes; tail: …'tore_sales.ext_wholesale_cost) as total_ext_wholesale_cost\\n;'.
  ```
- `trilogy file write query14.preql -e -c import raw.all_sales as sales;

# Stage 1: Compute overall average sale value (qty * list_price) for 1999-2001 across …t
    sales.item.brand_id as brand_id,
    sales.item.class_id as class_id,
    sales.item.category_id as category_id,
    sales.channel as channel
;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:1
    |
  9 | rowset qualifying_combos as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...l three channels in 1999-2001 ??? rowset qualifying_combos as wh...

  Write stats: received 707 chars / 707 bytes; tail: …'m.category_id as category_id,\\n    sales.channel as channel\\n;'.
  ```
- `trilogy file write query14.preql -e -c import raw.all_sales as sales;

# Overall average sale value (qty * list_price) across all 3 channels in 1999-2001
aut…       channel asc nulls first,
         brand_id asc nulls first,
         class_id asc nulls first,
         category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 31:5
     |
  31 |     --overall_avg_sale as _avg
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
   count(1) as total_number     ??? --overall_avg_sale as _avg hav...

  Write stats: received 1628 chars / 1628 bytes; tail: …'nulls first,\\n         category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql -e -c import raw.all_sales as sales;

# Overall average sale value (qty * list_price) across all 3 channels in 1999-2001
aut…       channel asc nulls first,
         brand_id asc nulls first,
         class_id asc nulls first,
         category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 38:1
     |
  38 | having total_sales > overall_avg_sale
     | ^---
     |
     = expected limit or order_by
  Location:
  ...nd_id, class_id, category_id) ??? having total_sales > overall_a...

  Hint: HAVING must come *before* the `by rollup/cube/grouping sets` clause in Trilogy (the reverse of SQL's `GROUP BY ... HAVING`). Reorder to:
      select <cols> having <cond> by rollup (<keys>) order by <cols> limit <n>;

  Write stats: received 1712 chars / 1712 bytes; tail: …'nulls first,\\n         category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query16.preql --content import raw.catalog_sales as cs;

# Orders that ship from more than one warehouse
auto multi_warehouse_orders <- cs…orders,
    sum(cs.ext_ship_cost) as total_extended_ship_cost,
    sum(cs.net_profit) as total_net_profit
order by
    unique_orders desc
limit 100;
`

  ```text
  refused to write 'query16.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:41
    |
  7 | auto returned_orders <- cs.order_number where cs.order_number in raw.catalog_returns.order_number;
    |                                         ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ned_orders <- cs.order_number ??? where cs.order_number in raw.c...

  Write stats: received 785 chars / 785 bytes; tail: …'total_net_profit\\norder by\\n    unique_orders desc\\nlimit 100;\\n'.
  ```
- `trilogy file write query21.preql --content import raw/inventory as inv;

# Before window: 2000-02-10 <= date < 2000-03-11
auto inv_before <- sum(inv.quantity…fore >= 2.0/3.0
  and inv_after / inv_before <= 3.0/2.0
order by
  inv.warehouse.name asc nulls first,
  inv.item.text_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/inventory as inv;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/inventory as inv;  # Befor...

  Write stats: received 864 chars / 864 bytes; tail: …' nulls first,\\n  inv.item.text_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.item as i;
im…um(combined.line_total) as total_sales,
order by combined.last_name nulls first, combined.first_name nulls first, total_sales nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...irst 30 chars of description. ??? rowset frequent_items as where...

  Write stats: received 2593 chars / 2593 bytes; tail: …'.first_name nulls first, total_sales nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.item as i;
im…um(combined.line_total) as total_sales,
order by combined.last_name nulls first, combined.first_name nulls first, total_sales nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...irst 30 chars of description. ??? rowset frequent_items as where...

  Write stats: received 2164 chars / 2164 bytes; tail: …'.first_name nulls first, total_sales nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.item as i;
im…um(combined.line_total) as total_sales,
order by combined.last_name nulls first, combined.first_name nulls first, total_sales nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 28:95
     |
  28 | where store_customer_totals.total_spent > 0.5 * (select max(store_customer_totals.total_spent))
     |                                                                                               ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, aggregate_over, or window_sql_over
  Location:
  ...e_customer_totals.total_spent) ??? ) select     store_customer_to...

  Write stats: received 2093 chars / 2093 bytes; tail: …'.first_name nulls first, total_sales nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query26.preql --content import raw/catalog_sales as cs;

where cs.sold_date.year = 2000
  and cs.bill_customer_demographic.gender = 'M'
  …rice) as avg_list_price,
    avg(cs.coupon_amt) as avg_coupon_amt,
    avg(cs.sales_price) as avg_sales_price
order by
    cs.item.text_id
limit 100;`

  ```text
  refused to write 'query26.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs;  wher...

  Write stats: received 549 chars / 549 bytes; tail: …') as avg_sales_price\\norder by\\n    cs.item.text_id\\nlimit 100;'.
  ```
- `trilogy file write query28.preql --content import raw.store_sales as store_sales;

# Bucket 1: ticket quantity 0 to 5, and (list price 8-18 OR coupon 459-145…list_price_5,
    bucket5.unique_list_price_5,
    bucket6.avg_list_price_6,
    bucket6.count_list_price_6,
    bucket6.unique_list_price_6
limit 1;`

  ```text
  refused to write 'query28.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:1
     |
  13 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...price) as unique_list_price_1 ??? by *;  # Bucket 2: ticket quan...

  Write stats: received 3726 chars / 3726 bytes; tail: …'count_list_price_6,\\n    bucket6.unique_list_price_6\\nlimit 1;'.
  ```
- `trilogy run query29.preql`

  ```text
  Syntax error in query29.preql: Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities - to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...return_date.year = 1999   and ??? sr.return_date.month_of_year >...
  ```
- `trilogy file write query29.preql -e -c import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

# Rowset: store sales i…re_sales_filtered.item_code,
    store_sales_filtered.item_desc,
    store_sales_filtered.store_code,
    store_sales_filtered.store_name
limit 100;
`

  ```text
  refused to write 'query29.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:1
    |
  6 | rowset store_sales_filtered as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...wset: store sales in Sep 1999 ??? rowset store_sales_filtered as...

  Write stats: received 1976 chars / 1976 bytes; tail: …'.store_code,\\n    store_sales_filtered.store_name\\nlimit 100;\\n'.
  ```
- `trilogy file write query38.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Store sales tuples: c…ame
union join ss_tuples.sale_date = ws_tuples.sale_date
where
    cs_tuples.last_name is not null
    and ws_tuples.last_name is not null
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...aring in all three select     ??? count(*) as combo_count union

  Write stats: received 1545 chars / 1545 bytes; tail: …' not null\\n    and ws_tuples.last_name is not null\\nlimit 100;'.
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities - to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
   top 10 from each subset join ??? best_items.rnk <= 10 subset jo...
  ```
- `trilogy run --import raw.web_sales:ws select distinct substring(ws.billing_customer.address.zip, 1, 5) as zip_prefix where substring(ws.billing_customer.address.zip, 1, 5) in ('85669','86197','88274','83405','86475','85392','85460','80348','81792') limit 10;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...t raw.web_sales as ws; select ??? distinct substring(ws.billing_...
  ```
- `trilogy run --import raw/store_sales:store_sales select distinct date.day_of_week order by 1 limit 100;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ..._sales as store_sales; select ??? distinct date.day_of_week orde...
  ```
- `trilogy file write query47.preql --content import raw.store_sales as ss;

# Monthly total of unit price per (category, brand, store, company, year, month)
au…c,
    ss.date.year asc,
    ss.date.month_of_year asc,
    avg_monthly asc,
    monthly_total asc,
    prev_total asc,
    next_total asc
limit 100;`

  ```text
  refused to write 'query47.preql': not syntactically valid Trilogy.

  Parse error:
    --> 36:1
     |
  36 | select
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
     and next_total is not null ??? select     ss.item.category,

  Write stats: received 2334 chars / 2334 bytes; tail: …'total asc,\\n    prev_total asc,\\n    next_total asc\\nlimit 100;'.
  ```
- `trilogy file write query51.preql --content import raw.web_sales as ws;
import raw.store_sales as ss;

# Per-item, per-day: sum of sales_price (unit price) fo…rows between unbounded preceding and current row) as store_running_max
order by combined.item asc nulls first, combined.dt asc nulls first
limit 100;`

  ```text
  refused to write 'query51.preql': not syntactically valid Trilogy.

  Parse error:
    --> 38:80
     |
  38 |     sum(coalesce(combined.web_unit_price, 0)) over (partition by combined.item order by combined.dt asc rows between unbounded preceding and current row) as web_running_total,
     |                                                                                ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...r (partition by combined.item ??? order by combined.dt asc rows

  Write stats: received 1984 chars / 1984 bytes; tail: …'item asc nulls first, combined.dt asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss where ss.customer.id = (select ss2.customer.id from raw.store_sales as ss2 where ss2.date.month_seq between 1188 and … and ss2.item.category = 'Women' limit 1) and ss.date.month_seq between 1188 and 1190 select ss.customer.id, sum(ss.ext_sales_price) as tot limit 10;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ....id = (select ss2.customer.id ??? from raw.store_sales as ss2 wh...
  ```
- `trilogy file write query59.preql --content import raw.store_sales as ss;

# Define a macro to sum sales_price for a given day-of-week (0=Sunday .. 6=Saturday…not null
order by this_yr.store_name asc nulls first,
         this_yr.store_code asc nulls first,
         this_yr.wk_seq asc nulls first
limit 100;`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | rowset this_yr as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
   data, pivoted by day-of-week ??? rowset this_yr as where ss.dat...

  Write stats: received 2079 chars / 2079 bytes; tail: …'ls first,\\n         this_yr.wk_seq asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_r…icket_number) as cnt,
    sum(ss.ext_wholesale_cost) as wholesale_sum,
    sum(ss.ext_list_price) as list_sum,
    sum(ss.coupon_amt) as coupon_sum
;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 26:1
     |
  26 | select
     | ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...r.demographics.marital_status ??? select     ss.item.product_nam...

  Write stats: received 2020 chars / 2020 bytes; tail: …'t_price) as list_sum,\\n    sum(ss.coupon_amt) as coupon_sum\\n;'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_r…rder_number = cr.order_number
select
    cs.item.text_id,
    sum(cr.refunded_cash + cr.reversed_charge + cr.store_credit) as cum_refund
;

select 1;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:9
     |
  21 | select 1;
     |         ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...dit) as cum_refund ;  select 1 ??? ;

  Write stats: received 525 chars / 525 bytes; tail: …'eversed_charge + cr.store_credit) as cum_refund\\n;\\n\\nselect 1;'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_r…d_charge, 0) + coalesce(cr.store_credit, 0)) as cum_refund
    subset join cs.item.id = cr.item.id and cs.order_number = cr.order_number
;

select 1;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:9
     |
  22 | select 1;
     |         ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   = cr.order_number ;  select 1 ??? ;

  Write stats: received 790 chars / 790 bytes; tail: …'r.item.id and cs.order_number = cr.order_number\\n;\\n\\nselect 1;'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_r…st_price
rowset cat_sales_by_item <-
    select
        cs.item.text_id as text_id,
        sum(cs.ext_list_price) as cum_ext_list_price
;

select 1;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:9
     |
  13 | select 1;
     |         ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...cum_ext_list_price ;  select 1 ??? ;

  Write stats: received 328 chars / 328 bytes; tail: …'   sum(cs.ext_list_price) as cum_ext_list_price\\n;\\n\\nselect 1;'.
  ```
- `trilogy run --import raw.item:i select distinct lower(i.color) as col where lower(i.color) in ('purple','burlywood','indian','spring','floral','medium') order by col;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  import raw.item as i; select ??? distinct lower(i.color) as col...
  ```
- `trilogy file write query65.preql --content import raw.store_sales as ss;

auto revenue_per_store_item <- sum(ss.sales_price) by ss.store.id, ss.item.id;
auto…aving revenue_per_store_item <= 0.1 * store_avg_revenue
order by ss.store.name, ss.item.desc, ss.store.id, ss.item.id
    with nulls first
limit 100;`

  ```text
  refused to write 'query65.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:5
     |
  17 |     with nulls first
     |     ^---
     |
     = expected limit, ORDERING_DIRECTION, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ..., ss.store.id, ss.item.id     ??? with nulls first limit 100;

  Write stats: received 534 chars / 534 bytes; tail: …'esc, ss.store.id, ss.item.id\\n    with nulls first\\nlimit 100;'.
  ```
- `trilogy file write query76.preql --content import raw.store_sales as store;
import raw.web_sales as web;
import raw.catalog_sales as catalog;

with combined …tal_ext_sales_price
order by
  combined.channel,
  combined.missing_ref_label,
  combined.yr,
  combined.qtr,
  combined.cat
  nulls first
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ..._count, total_ext_sales_price) ???  select   combined.channel,

  Write stats: received 1494 chars / 1494 bytes; tail: …'.yr,\\n  combined.qtr,\\n  combined.cat\\n  nulls first\\nlimit 100;'.
  ```
- `trilogy file write query78_debug.preql -c import raw.store_sales as store;
import raw.web_sales as web;
import raw.catalog_sales as catalog;

with store_nr a…store.item.id as item_id,
    store.customer.id as cust_id,
    sum(store.quantity) as store_qty
;
select store_nr.yr, count(*) as cnt from store_nr;`

  ```text
  refused to write 'query78_debug.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...ore_qty ; select store_nr.yr, ??? count(*) as cnt from store_nr;...

  Write stats: received 401 chars / 401 bytes; tail: …'ore_qty\\n;\\nselect store_nr.yr, count(*) as cnt from store_nr;'.
  ```
- `trilogy file write query78_debug.preql -c import raw.store_sales as store;
import raw.web_sales as web;
import raw.catalog_sales as catalog;

with store_nr a…as cust_id,
    sum(store.quantity) as store_qty
;

select store_nr.yr, store_nr.item_id, store_nr.cust_id, store_nr.store_qty from store_nr limit 5;`

  ```text
  refused to write 'query78_debug.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...r.cust_id, store_nr.store_qty ??? from store_nr limit 5;

  Write stats: received 449 chars / 449 bytes; tail: …' store_nr.cust_id, store_nr.store_qty from store_nr limit 5;'.
  ```
- `trilogy file write query82.preql --content import raw.store_sales as ss;
import raw.inventory as inv;
import raw.item as item;

subset join item.text_id = ss…07-24'::date
select
    item.text_id as item_code,
    item.desc as description,
    item.current_price as price
order by
    item.text_id
limit 100;`

  ```text
  refused to write 'query82.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:1
    |
  8 | where
    | ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...m.text_id = inv.item.text_id  ??? where     item.current_price b...

  Write stats: received 530 chars / 530 bytes; tail: …'.current_price as price\\norder by\\n    item.text_id\\nlimit 100;'.
  ```
- `trilogy file write query93.preql --content import raw.store_sales as ss;

# The adjusted amount per sale line that has return reason 28
auto adjusted_amount …ed_amount) as total_adjusted_amount,
    --ss.customer.id
order by
    total_adjusted_amount nulls first,
    --ss.customer.id nulls first
limit 100;`

  ```text
  refused to write 'query93.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:6
     |
  21 |     --ss.customer.id nulls first
     |      ^---
     |
     = expected access_chain
  Location:
  ...sted_amount nulls first,     - ??? -ss.customer.id nulls first li...

  Write stats: received 721 chars / 721 bytes; tail: …'unt nulls first,\\n    --ss.customer.id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query93.preql --content import raw.store_sales as ss;

# The adjusted amount per sale line that has return reason 28
auto adjusted_amount …as total_adjusted_amount,
    --customer_sort
order by
    total_adjusted_amount nulls first,
    --customer_sort asc,
    customer_id asc
limit 100;`

  ```text
  refused to write 'query93.preql': not syntactically valid Trilogy.

  Parse error:
    --> 24:6
     |
  24 |     --customer_sort asc,
     |      ^---
     |
     = expected access_chain
  Location:
  ...sted_amount nulls first,     - ??? -customer_sort asc,     custom...

  Write stats: received 870 chars / 870 bytes; tail: …'rst,\\n    --customer_sort asc,\\n    customer_id asc\\nlimit 100;'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.catalog_returns:cr select count(cr.order_number), count(cr.order_number ? cr.order_number is not null) as non_null, count(cr.order_number ? cr.order_number is null) as null_count;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(cr.order_number) as order_number_count`
  Location:
   select count(cr.order_number) ??? , count(cr.order_number ? cr.o...
  ```
- `trilogy run --import raw.all_sales:sales select sales.date.year, sales.date.month_of_year, count(sales.order_id), sum(sales.return_amount) where sales.return_amount > 10000 group by sales.date.year, sales.date.month_of_year order by sales.date.year, sales.date.month_of_year;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(sales.order_id) as order_id_count`
  Location:
  ...of_year, count(sales.order_id) ??? , sum(sales.return_amount) whe...
  ```
- `trilogy run --import raw.store_sales:store select store.date.year, sum(store.quantity) where store.date.year = 2000 and store.customer.id is not null and store.is_returned = false limit 5;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `sum(store.quantity) as quantity_sum`
  Location:
  ...ate.year, sum(store.quantity) ??? where store.date.year = 2000 a...
  ```
- `trilogy run --import raw.store_sales:ss select coalesce(ss.customer.id::string,'unidentified') as cid, sum(ss.quantity) where ss.return_reason.desc = 'reason 28' and ss.customer.id is null;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `sum(ss.quantity) as quantity_sum`
  Location:
  ...ed') as cid, sum(ss.quantity) ??? where ss.return_reason.desc =
  ```

### `cli-misuse`

- `trilogy explore raw/date_dim.preql`

  ```text
  Invalid value for 'PATH': File 'raw/date_dim.preql' does not exist.
  ```
- `trilogy explore ./raw/sales/sales.preql`

  ```text
  Invalid value for 'PATH': File './raw/sales/sales.preql' does not exist.
  ```
- `trilogy explore raw/customer_demographics.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_demographics.preql' does not exist.
  ```
- `trilogy explore raw/household_demographics.preql`

  ```text
  Invalid value for 'PATH': File 'raw/household_demographics.preql' does not exist.
  ```

### `planner-recursion`

- `trilogy run query02.preql`

  ```text
  Resolution error in query02.preql: query could not be planned; this is a bug.
  ```
- `trilogy run query02.preql`

  ```text
  Resolution error in query02.preql: query could not be planned; this is a bug.
  ```

### `type-error`

- `trilogy run query38.preql`

  ```text
  Type error in query38.preql: Invalid argument type 'DATE' passed into CONCAT function in position 2 from concept: ss_tuples.ss.date.date. Valid: 'STRING'.
  ```

### `undefined-concept`

- `trilogy explore query64.preql`

  ```text
  Failed to parse query64.preql: (UndefinedConceptException(...), "Undefined concept: agg_1999.item_id. Suggestions: ['agg_sales.item_id', 'agg_2000.item_id', 'agg_1999.store_zip']")
  ```
