# Trilogy failure analysis — 20260628-194910

- Run `20260628-194910` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 238 | failed: 34 (14%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 19 | 56% |
| `syntax-parse` | 12 | 35% |
| `cli-misuse` | 2 | 6% |
| `syntax-missing-alias` | 1 | 3% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Undefined concept: local.dow.
  ```
- `trilogy run query02.preql`

  ```text
  Resolution error in query02.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 32). The requested concepts split into 7 disconnected subgraphs: {cur_dow.wk, nxt_dow.amt}; {local._virt_filter_amt_3813119485260619}; {local._virt_filter_amt_4138258214457585}; {local._virt_filter_amt_5395061096804011}; {local._virt_filter_amt_624131215018987}; {local._virt_filter_amt_9023431424270700}; {local._virt_filter_amt_9676972943017305}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query02.preql`

  ```text
  Resolution error in query02.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 23). The requested concepts split into 7 disconnected subgraphs: {cur.wk, nxt.amt}; {local._virt_filter_amt_1742324099235801}; {local._virt_filter_amt_2363809946469648}; {local._virt_filter_amt_4041040528354212}; {local._virt_filter_amt_4967873934528573}; {local._virt_filter_amt_6846657472860907}; {local._virt_filter_amt_7868248051144643}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Ambiguous reference 'joined_data.wk': matches ['joined_data.cur_dow.wk', 'joined_data.nxt_dow.wk']. Qualify the full path to disambiguate.
  ```
- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query05.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(s.channel)` (line 19), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(s.channel) as g order by g desc`.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 39 column 13 (char 1290). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 16). The requested concepts split into 2 disconnected subgraphs: {local.coupon_amt_sum, local.list_price_sum, local.sale_lines, local.wholesale_cost_sum, ss.customer.address.city, ss.customer.address.street_name, ss.customer.address.street_number, ss.customer.address.zip, ss.customer.demographics.marital_status, ss.customer.first_sales_date.year, ss.customer.first_shipto_date.year, ss.customer_demographic.marital_status, ss.date.year, ss.item.color, ss.item.current_price, ss.item.id, ss.item.product_name, ss.sale_address.city, ss.sale_address.street_name, ss.sale_address.street_number, ss.sale_address.zip, ss.store.name, ss.store.zip, ss.ticket_number}; {sr.item.id, sr.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {cs.item.id, local.cat_list_by_item, local.cat_refund_by_item}
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: agg_rows_1999.item_id. Suggestions: ['qualifying_items.item_id', 'agg_rows.item_id', 'agg_rows_2000.item_id', '_agg_rows_item_id', 'agg_rows_1999.store_zip']
  ```
- `trilogy file read query64.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query70.preql`

  ```text
  Unexpected error in query70.preql: (_duckdb.BinderException) Binder Error: GROUPING child "sales_store_county" must be a grouping column

  LINE 118:     grouping("concerned"."sales_store_county") as "_virt_agg_gr...
                ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "sales_store_sales"."SS_SOLD_DATE_SK" as "sales_date_id",
      "sales_store_sales"."SS_STORE_SK" as "sales_store_id"
  FROM
      "store_sales" as "sales_store_sales"
  GROUP BY
      1,
      2),
  cheerful as (
  SELECT
      "sales_store_sales"."SS_ITEM_SK" as "sales_item_id",
      "sales_store_sales"."SS_NET_PROFIT" as "sales_net_profit",
      "sales_store_sales"."SS_TICKET_NUMBER" as "sales_ticket_number",
      "sales_store_store"."S_COUNTY" as "sales_store_county",
      "sales_store_store"."S_STATE" as "sales_store_state"
  FROM
      "store_sales" as "sales_store_sales"
      INNER JOIN "date_dim" as "sales_date_date" on "sales_store_sales"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "sales_store_store" on "sales_store_sales"."SS_STORE_SK" = "sales_store_store"."S_STORE_SK"
  WHERE
      "sales_date_date"."D_YEAR" = 2000
  ),
  cooperative as (
  SELECT
      "sales_store_store"."S_COUNTY" as "sales_store_county",
      "sales_store_store"."S_STATE" as "sales_store_state"
  FROM
      "thoughtful"
      INNER JOIN "date_dim" as "sales_date_date" on "thoughtful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "sales_store_store" on "thoughtful"."sales_store_id" = "sales_store_store"."S_STORE_SK"
  WHERE
      "sales_date_date"."D_YEAR" = 2000

  GROUP BY
      1,
      2,
      "thoughtful"."sales_store_id"),
  questionable as (
  SELECT
      "cooperative"."sales_store_county" as "sales_store_county",
      "cooperative"."sales_store_state" as "sales_store_state",
      grouping("cooperative"."sales_store_county") as "_virt_agg_grouping_4700760153379391",
      grouping("cooperative"."sales_store_state") as "_virt_agg_grouping_4252344123812359"
  FROM
      "cooperative"
  GROUP BY
      ROLLUP (2, 1)),
  yummy as (
  SELECT
      "cooperative"."sales_store_county" as "sales_store_county",
      "cooperative"."sales_store_state" as "sales_store_state",
      CASE
  	WHEN grouping("cooperative"."sales_store_county") = 1 THEN 'All Counties'
  	ELSE "cooperative"."sales_store_county"
  	END as "county",
      CASE
  	WHEN grouping("cooperative"."sales_store_state") = 1 and grouping("cooperative"."sales_store_county") = 1 THEN 'All States'
  	WHEN grouping("cooperative"."sales_store_state") = 1 THEN "cooperative"."sales_store_state"
  	ELSE "cooperative"."sales_store_state"
  	END as "state",
      grouping("cooperative"."sales_store_state") + grouping("cooperative"."sales_store_county") as "hierarchy_level"
  FROM
      "cooperative"
  GROUP BY
      ROLLUP (2, 1)),
  concerned as (
  SELECT
      "questionable"."_virt_agg_grouping_4252344123812359" + "questionable"."_virt_agg_grouping_4700760153379391" as "hierarchy_level",
      CASE
  	WHEN "questionable"."_virt_agg_grouping_4252344123812359" = 1 and "questionable"."_virt_agg_grouping_4700760153379391" = 1 THEN 'All States'
  	WHEN "questionable"."_virt_agg_grouping_4252344123812359" = 1 THEN coalesce("cooperative"."sales_store_state","questionable"."sales_store_state")
  	ELSE coalesce("cooperative"."sales_store_state","questionable"."sales_store_state")
  	END as "state",
      CASE
  	WHEN "questionable"."_virt_agg_grouping_4700760153379391" = 1 THEN 'All Counties'
  	ELSE coalesce("cooperative"."sales_store_county","questionable"."sales_store_county")
  	END as "county",
      coalesce("cooperative"."sales_store_county","questionable"."sales_store_county") as "sales_store_county",
      coalesce("cooperative"."sales_store_state","questionable"."sales_store_state") as "sales_store_state"
  FROM
      "questionable"
      FULL JOIN "cooperative" on "questionable"."sales_store_county" is not distinct from "cooperative"."sales_store_county" AND "questionable"."sales_store_state" is not distinct from "cooperative"."sales_store_state"),
  abundant as (
  SELECT
      "cheerful"."sales_net_profit" as "sales_net_profit",
      "questionable"."_virt_agg_grouping_4252344123812359" + "questionable"."_virt_agg_grouping_4700760153379391" as "hierarchy_level",
      CASE
  	WHEN "questionable"."_virt_agg_grouping_4252344123812359" = 1 and "questionable"."_virt_agg_grouping_4700760153379391" = 1 THEN 'All States'
  	WHEN "questionable"."_virt_agg_grouping_4252344123812359" = 1 THEN coalesce("cheerful"."sales_store_state","questionable"."sales_store_state")
  	ELSE coalesce("cheerful"."sales_store_state","questionable"."sales_store_state")
  	END as "state",
      CASE
  	WHEN "questionable"."_virt_agg_grouping_4700760153379391" = 1 THEN 'All Counties'
  	ELSE coalesce("cheerful"."sales_store_county","questionable"."sales_store_county")
  	END as "county",
      coalesce("cheerful"."sales_store_county","questionable"."sales_store_county") as "sales_store_county",
      coalesce("cheerful"."sales_store_state","questionable"."sales_store_state") as "sales_store_state"
  FROM
      "questionable"
      FULL JOIN "cheerful" on "questionable"."sales_store_county" is not distinct from "cheerful"."sales_store_county" AND "questionable"."sales_store_state" is not distinct from "cheerful"."sales_store_state"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      "cheerful"."sales_item_id",
      "cheerful"."sales_ticket_number"),
  young as (
  SELECT
      "concerned"."county" as "county",
      "concerned"."hierarchy_level" as "hierarchy_level",
      "concerned"."state" as "state",
      grouping("concerned"."sales_store_county") as "_virt_agg_grouping_3163114698427542",
      grouping("concerned"."sales_store_state") as "_virt_agg_grouping_9000468855706710"
  FROM
      "concerned"
  GROUP BY
      1,
      2,
      3),
  uneven as (
  SELECT
      "abundant"."county" as "county",
      "abundant"."hierarchy_level" as "hierarchy_level",
      "abundant"."state" as "state",
      sum("abundant"."sales_net_profit") as "total_np"
  FROM
      "abundant"
  GROUP BY
      ROLLUP ("abundant"."sales_store_state", "abundant"."sales_store_county")),
  juicy as (
  SELECT
      "uneven"."total_np" as "total_np",
      "yummy"."sales_store_county" as "sales_store_county",
      "yummy"."sales_store_state" as "sales_store_state"
  FROM
      "yummy"
      FULL JOIN "uneven" on "yummy"."county" is not distinct from "uneven"."county" AND "yummy"."hierarchy_level" is not distinct from "uneven"."hierarchy_level" AND "yummy"."state" is not distinct from "uneven"."state"),
  vacuous as (
  SELECT
      "juicy"."total_np" as "total_np",
      "questionable"."_virt_agg_grouping_4252344123812359" + "questionable"."_virt_agg_grouping_4700760153379391" as "hierarchy_level",
      CASE
  	WHEN "questionable"."_virt_agg_grouping_4252344123812359" = 1 and "questionable"."_virt_agg_grouping_4700760153379391" = 1 THEN 'All States'
  	WHEN "questionable"."_virt_agg_grouping_4252344123812359" = 1 THEN coalesce("juicy"."sales_store_state","questionable"."sales_store_state")
  	ELSE coalesce("juicy"."sales_store_state","questionable"."sales_store_state")
  	END as "state",
      CASE
  	WHEN "questionable"."_virt_agg_grouping_4700760153379391" = 1 THEN 'All Counties'
  	ELSE coalesce("juicy"."sales_store_county","questionable"."sales_store_county")
  	END as "county"
  FROM
      "questionable"
      INNER JOIN "juicy" on "questionable"."sales_store_county" is not distinct from "juicy"."sales_store_county" AND "questionable"."sales_store_state" is not distinct from "juicy"."sales_store_state")
  SELECT
      coalesce("vacuous"."state","young"."state") as "state",
      coalesce("vacuous"."county","young"."county") as "county",
      "vacuous"."total_np" as "total_np",
      coalesce("vacuous"."hierarchy_level","young"."hierarchy_level") as "hierarchy_level",
      row_number() over (partition by "young"."_virt_agg_grouping_9000468855706710" + "young"."_virt_agg_grouping_3163114698427542" order by "vacuous"."total_np" desc ) as "rnk"
  FROM
      "young"
      INNER JOIN "vacuous" on "young"."county" is not distinct from "vacuous"."county" AND "young"."hierarchy_level" is not distinct from "vacuous"."hierarchy_level" AND "young"."state" is not distinct from "vacuous"."state"
  ORDER BY
      coalesce("vacuous"."hierarchy_level","young"."hierarchy_level") desc,
      coalesce("vacuous"."state","young"."state") asc,
      "rnk" asc]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query70.preql`

  ```text
  Unexpected error in query70.preql: maximum recursion depth exceeded
  ```
- `trilogy run query70.preql`

  ```text
  Unexpected error in query70.preql: Recursion error building concept local.within_parent_rank with grain Grain<sales.item.id,sales.ticket_number> and lineage case(WHEN add(grouping(ref:sales.store.state)<['ref:sales.store.state', 'ref:sales.store.county']>,grouping(ref:sales.store.county)<['ref:sales.store.state', 'ref:sales.store.county']>) >= 1 THEN 1,ELSE rank([ref:sales.store.state, ref:sales.store.county]) over [add(grouping(ref:sales.store.state)<abstract>,grouping(ref:sales.store.county)<abstract>)] order [OrderItem(expr=AggregateWrapper(function=sum(ref:sales.net_profit), by=[ref:sales.store.state], grouping=<AggregateGroupingMode.STANDARD: 'standard'>, grouping_sets=[]), order=<Ordering.DESCENDING: 'desc'>)]). This is likely due to a circular reference.
  ```
- `trilogy run query70.preql`

  ```text
  Unexpected error in query70.preql: Recursion error building concept local.within_parent_rank with grain Grain<sales.item.id,sales.ticket_number,top_states.state> and lineage case(WHEN grouping(ref:sales.store.state)<['ref:sales.store.state', 'ref:sales.store.county']> = 1 and grouping(ref:sales.store.county)<['ref:sales.store.state', 'ref:sales.store.county']> = 1 THEN 1,WHEN grouping(ref:sales.store.county)<['ref:sales.store.state', 'ref:sales.store.county']> = 1 THEN ref:top_states.rnk,ELSE row_number([]) over [ref:sales.store.state] order [OrderItem(expr=AggregateWrapper(function=sum(ref:sales.net_profit), by=[ref:local.state], grouping=<AggregateGroupingMode.STANDARD: 'standard'>, grouping_sets=[]), order=<Ordering.DESCENDING: 'desc'>)]). This is likely due to a circular reference.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query97.preql`

  ```text
  Resolution error in query97.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 30). The requested concepts split into 2 disconnected subgraphs: {catalog_pairs.item_id}; {store_pairs.cust_id, store_pairs.item_id}. Are you missing a join or merge statement to relate them?
  ```

### `syntax-parse`

- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.all_sales as all_sales;

# Frequent items: count of (item, sold_…l_sales.list_price) as total_sales
order by
    last_name asc nulls first,
    first_name asc nulls first,
    total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens — write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ...ore_sales.date.date::string)) ??? by substring(store_sales.item....

  Write stats: received 1455 chars / 1455 bytes; tail: …' asc nulls first,\\n    total_sales asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query38.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Rowsets for each chan…name = web_combos.first_name
inner join store_combos.sale_date = web_combos.sale_date;

# Count distinct combinations
select count(*) as combo_count;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
   distinct combinations select ??? count(*) as combo_count;

  Write stats: received 1553 chars / 1553 bytes; tail: …' Count distinct combinations\\nselect count(*) as combo_count;'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_r… as coupon_amt_sum
    inner join ss.item.id = sr.item.id
    inner join ss.ticket_number = sr.ticket_number
order by ss.item.product_name
limit 100;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...2 * cumulative catalog refund ??? rowset qualifying_items as sel...

  Write stats: received 1697 chars / 1697 bytes; tail: …' = sr.ticket_number\\norder by ss.item.product_name\\nlimit 100;'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_r… as coupon_amt_sum
    inner join ss.item.id = sr.item.id
    inner join ss.ticket_number = sr.ticket_number
order by ss.item.product_name
limit 100;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...2 * cumulative catalog refund ??? rowset qualifying_items as sel...

  Write stats: received 1697 chars / 1697 bytes; tail: …' = sr.ticket_number\\norder by ss.item.product_name\\nlimit 100;'.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as sales;

# Step 1: Define net profit sum by state for year 2000,
# to identify top 5 stat…ate, sales.store.county)
having
    state in ('Top5StatePlaceholder')
order by
    hierarchy_level desc,
    state,
    within_parent_rank
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:41
     |
  12 |         grouping(sales.store.state) = 1 -> 'All States'
     |                                         ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or fcase_simple_when
  Location:
  ...ouping(sales.store.state) = 1 ??? -> 'All States'         else s...

  Write stats: received 1412 chars / 1412 bytes; tail: …'chy_level desc,\\n    state,\\n    within_parent_rank\\nlimit 100;'.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as sales;

# Define concepts that are used multiple times
auto total_np <- sum(sales.net_pr…nk(state_np.state) over (order by state_np.state_np_total desc) <= 5)
order by
    hierarchy_level desc,
    state,
    within_parent_rank
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...es.store.state in             ??? (select state_np.state

  Write stats: received 1451 chars / 1451 bytes; tail: …'chy_level desc,\\n    state,\\n    within_parent_rank\\nlimit 100;'.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as sales;

# Rowset: state-level total net profit for year 2000, ranked
# We want top 5 sta… within_parent_rank
by rollup (sales.store.state, sales.store.county)
order by
    hierarchy_level desc,
    state,
    within_parent_rank
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens — write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ...ranked # We want top 5 states ??? by net profit rowset top_state...

  Write stats: received 1276 chars / 1276 bytes; tail: …'chy_level desc,\\n    state,\\n    within_parent_rank\\nlimit 100;'.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as sales;

rowset top_states as
where sales.date.year = 2000
select
    sales.store.state a… within_parent_rank
by rollup (sales.store.state, sales.store.county)
order by
    hierarchy_level desc,
    state,
    within_parent_rank
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
   --> 3:1
    |
  3 | rowset top_states as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...rt raw.store_sales as sales;  ??? rowset top_states as where sal...

  Write stats: received 1115 chars / 1115 bytes; tail: …'chy_level desc,\\n    state,\\n    within_parent_rank\\nlimit 100;'.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as sales;

# Top 5 states by net profit for year 2000
rowset top_states <-
where sales.date…state)
    or (g_state = 0 and sales.store.state in top_states.state)
order by
    hierarchy_level desc,
    state,
    within_parent_rank
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
    --> 34:1
     |
  34 | having
     | ^---
     |
     = expected limit or order_by
  Location:
  ...re.state, sales.store.county) ??? having     (g_state = 1 and g_...

  Write stats: received 1313 chars / 1313 bytes; tail: …'chy_level desc,\\n    state,\\n    within_parent_rank\\nlimit 100;'.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as sales;

# Top 5 states by net profit for year 2000
rowset top_states <-
where sales.date…state)
    or (g_state = 0 and sales.store.state in top_states.state)
order by
    hierarchy_level desc,
    state,
    within_parent_rank
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
    --> 34:1
     |
  34 | having
     | ^---
     |
     = expected limit or order_by
  Location:
  ...re.state, sales.store.county) ??? having     (g_state = 1 and g_...

  Write stats: received 1313 chars / 1313 bytes; tail: …'chy_level desc,\\n    state,\\n    within_parent_rank\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:sales select sales.store.state, sales.store.county, sum(sales.net_profit) as total_np, grouping(sales.store.state) + gro…(sales.store.county) as level where sales.date.year = 2000 and sales.store.state is not null group by rollup (sales.store.state, sales.store.county);`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...sales.store.state is not null ??? group by rollup (sales.store.s...
  ```
- `trilogy file write query75.preql --content import raw.all_sales as sales;

# Items in 'Books' category
where sales.item.category = 'Books'

# Per-line net me…    curr_amt - prev_amt as amt_diff
having
    prev_qty > 0
    and curr_qty / prev_qty < 0.9
order by
    qty_diff asc,
    amt_diff asc
limit 100;
`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...reat missing returns as zero) ??? auto net_qty <- sales.quantity...

  Write stats: received 1667 chars / 1667 bytes; tail: …' 0.9\\norder by\\n    qty_diff asc,\\n    amt_diff asc\\nlimit 100;\\n'.
  ```

### `cli-misuse`

- `trilogy run --import raw/all_sales:all_sales --format json select all_sales.channel, all_sales.channel_dim_id, all_sales.channel_dim_text_id, all_sales.date.…s.return_amount, all_sales.return_net_loss, all_sales.is_returned, all_sales.sale_line_item_counter, all_sales.return_date.date as return_dt limit 5;`

  ```text
  'json' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run query97.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```

### `syntax-missing-alias`

- `trilogy file write query02.preql --content import raw.all_sales as sales;

# Filter to only web and catalog sales
auto web_catalog_sales <- sales.ext_sales_p…ow = current_year_sales.dow)
    , 2) as ratio
where current_year_sales.ws in weeks_in_2001
order by current_year_sales.ws asc nulls first
limit 100;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `Need to think differently... use self-join approach as need_to_think_differently_use_self_join_`
  Location:
  ...ales.daily_total,     -- Need ??? to think differently... use se...

  Write stats: received 2656 chars / 2656 bytes; tail: …'01\\norder by current_year_sales.ws asc nulls first\\nlimit 100;'.
  ```
