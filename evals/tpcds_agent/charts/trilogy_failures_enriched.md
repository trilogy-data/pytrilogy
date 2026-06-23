# Trilogy failure analysis — 20260623-145720

- Run `20260623-145720` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1672 | failed: 218 (13%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 128 | 59% |
| `syntax-parse` | 53 | 24% |
| `syntax-missing-alias` | 9 | 4% |
| `cli-misuse` | 7 | 3% |
| `undefined-concept` | 7 | 3% |
| `file-not-found` | 7 | 3% |
| `join-resolution` | 6 | 3% |
| `type-error` | 1 | 0% |

## Detail

### `other`

- `trilogy run query02.preql duckdb`

  ```text
  Syntax error in query02.preql: 5 undefined concept references; fix all before re-running:
    - weekly_base.week_seq (line 13, col 5, in SELECT); did you mean: weekly_base.sl.date.week_seq, sl.date.week_seq, sl.return_date.week_seq, sl.billing_customer.first_sales_date.week_seq, sl.billing_customer.first_shipto_date.week_seq, sl.ship_customer.first_sales_date.week_seq?
    - weekly_base.day_of_week (line 14, col 5, in SELECT); did you mean: weekly_base.sl.date.day_of_week, sl.date.day_of_week, sl.return_date.day_of_week, sl.billing_customer.first_sales_date.day_of_week, sl.billing_customer.first_shipto_date.day_of_week, sl.ship_customer.first_sales_date.day_of_week?
    - weekly_base.week_seq (line 16, col 59, in SELECT); did you mean: weekly_base.sl.date.week_seq, sl.date.week_seq, sl.return_date.week_seq, sl.billing_customer.first_sales_date.week_seq, sl.billing_customer.first_shipto_date.week_seq, sl.ship_customer.first_sales_date.week_seq?
    - weekly_base.week_seq (line 17, col 10, in ORDER BY); did you mean: weekly_base.sl.date.week_seq, sl.date.week_seq, sl.return_date.week_seq, sl.billing_customer.first_sales_date.week_seq, sl.billing_customer.first_shipto_date.week_seq, sl.ship_customer.first_sales_date.week_seq?
    - weekly_base.day_of_week (line 17, col 36, in ORDER BY); did you mean: weekly_base.sl.date.day_of_week, sl.date.day_of_week, sl.return_date.day_of_week, sl.billing_customer.first_sales_date.day_of_week, sl.billing_customer.first_shipto_date.day_of_week, sl.ship_customer.first_sales_date.day_of_week?
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Syntax error in query02.preql: 5 undefined concept references; fix all before re-running:
    - with_next.sl.date.week_seq (line 26, col 5, in SELECT); did you mean: with_next.weekly_base.sl.date.week_seq, sl.date.week_seq, sl.return_date.week_seq, sl.billing_customer.first_sales_date.week_seq, sl.billing_customer.first_shipto_date.week_seq, sl.ship_customer.first_sales_date.week_seq?
    - with_next.sl.date.day_of_week (line 27, col 5, in SELECT); did you mean: with_next.weekly_base.sl.date.day_of_week, sl.date.day_of_week, sl.return_date.day_of_week, sl.billing_customer.first_sales_date.day_of_week, sl.billing_customer.first_shipto_date.day_of_week, sl.ship_customer.first_sales_date.day_of_week?
    - with_next.sl.date.week_seq (line 24, col 7, in WHERE); did you mean: with_next.weekly_base.sl.date.week_seq, sl.date.week_seq, sl.return_date.week_seq, sl.billing_customer.first_sales_date.week_seq, sl.billing_customer.first_shipto_date.week_seq, sl.ship_customer.first_sales_date.week_seq?
    - with_next.sl.date.week_seq (line 30, col 10, in ORDER BY); did you mean: with_next.weekly_base.sl.date.week_seq, sl.date.week_seq, sl.return_date.week_seq, sl.billing_customer.first_sales_date.week_seq, sl.billing_customer.first_shipto_date.week_seq, sl.ship_customer.first_sales_date.week_seq?
    - with_next.sl.date.day_of_week (line 30, col 42, in ORDER BY); did you mean: with_next.weekly_base.sl.date.day_of_week, sl.date.day_of_week, sl.return_date.day_of_week, sl.billing_customer.first_sales_date.day_of_week, sl.billing_customer.first_shipto_date.day_of_week, sl.ship_customer.first_sales_date.day_of_week?
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Unexpected error in query02.preql: list index out of range
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query04.preql`

  ```text
  Syntax error in query04.preql: Undefined concept: annual_values.channel (line 20, in SELECT). Suggestions: ['annual_values.all_sales.channel', 'all_sales.channel', 'annual_values.annual_value']
  ```
- `trilogy run query04.preql`

  ```text
  Resolution error in query04.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {catalog_01.catalog_01, catalog_02.catalog_02, store_01.fname, store_01.lname, store_01.pref_flag, store_01.store_01, store_02.store_02, web_01.web_01, web_02.cust_id, web_02.web_02}
  ```
- `trilogy run query04.preql`

  ```text
  Syntax error in query04.preql: Undefined concept: ann_val.channel. Suggestions: ['ann_val.s.channel', 's.channel', 'ann_val.val', 'ann_val.fname']
  ```
- `trilogy run query04.preql`

  ```text
  Syntax error in query04.preql: 2 undefined concept references; fix all before re-running:
    - store01.cust_id (line 25, col 8, in SELECT); did you mean: store01.ann_val.cust_id, ann_val.cust_id, store01.s01, s.item.class_id?
    - store01.fname (line 25, col 25, in SELECT); did you mean: store01.ann_val.fname, ann_val.fname, store01.ann_val.lname, store01.s01?
  ```
- `trilogy run query04.preql`

  ```text
  Resolution error in query04.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {catalog01.c01, catalog02.c02, store01.ann_val.fname, store01.ann_val.lname, store01.ann_val.pref_flag, store01.s01, store02.s02, web01.w01, web02.ann_val.cust_id, web02.w02}
  ```
- `trilogy run query04.preql`

  ```text
  Syntax error in query04.preql: HAVING references 'local.store_01_val', 'local.catalog_01_val', 'local.web_01_val', 'local.catalog_02_val', 'local.store_02_val', 'local.web_02_val', which are not in the SELECT projection (line 23). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.store_01_val, --local.catalog_01_val, --local.web_01_val, --local.catalog_02_val, --local.store_02_val, --local.web_02_val
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY references 'local.g_channel', which is not in the SELECT projection (line 56). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.g_channel order by local.g_channel asc`.
  ```
- `trilogy run --import raw.all_sales:all_sales select sales.channel, count(sales.row_one) as cnt;`

  ```text
  Syntax error in stdin: 2 undefined concept references; fix all before re-running:
    - sales.channel (line 2, col 8, in SELECT); did you mean: all_sales.channel, all_sales.channel_dim_id, all_sales.channel_dim_text_id?
    - sales.row_one (line 2, in SELECT); did you mean: all_sales.row_one, sales.channel?
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, grouping(all_sales.channel) as gc, grouping(all_sales.channel_dim_text_id) as ge limit 20;`

  ```text
  Unexpected error in stdin: (_duckdb.BinderException) Binder Error: GROUPING statement cannot be used without groups

  LINE 35:     grouping("thoughtful"."all_sales_channel") as "gc"
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as "all_sales_channel_dim_text_id"
  FROM
      "catalog_page" as "all_sales_catalog_dim_unified"
  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_dim_unified"."S_STORE_ID" as "all_sales_channel_dim_text_id"
  FROM
      "store" as "all_sales_store_dim_unified"
  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_dim_unified"."web_site_id" as "all_sales_channel_dim_text_id"
  FROM
      "web_site" as "all_sales_web_dim_unified"),
  thoughtful as (
  SELECT
      "cheerful"."all_sales_channel" as "all_sales_channel",
      "cheerful"."all_sales_channel_dim_text_id" as "all_sales_channel_dim_text_id",
      grouping("cheerful"."all_sales_channel_dim_text_id") as "ge"
  FROM
      "cheerful"
  GROUP BY
      1,
      2),
  cooperative as (
  SELECT
      "thoughtful"."all_sales_channel" as "all_sales_channel",
      "thoughtful"."all_sales_channel_dim_text_id" as "all_sales_channel_dim_text_id",
      grouping("thoughtful"."all_sales_channel") as "gc"
  FROM
      "thoughtful")
  SELECT
      "thoughtful"."all_sales_channel" as "all_sales_channel",
      "thoughtful"."all_sales_channel_dim_text_id" as "all_sales_channel_dim_text_id",
      "cooperative"."gc" as "gc",
      "thoughtful"."ge" as "ge"
  FROM
      "cooperative"
      INNER JOIN "thoughtful" on "cooperative"."all_sales_channel" = "thoughtful"."all_sales_channel" AND "cooperative"."all_sales_channel_dim_text_id" = "thoughtful"."all_sales_channel_dim_text_id"
  LIMIT (20)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query05.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query06.preql`

  ```text
  Resolution error in query06.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {local.cat_avg_price}; {local.qualifying_line_items, local.state, ss.customer.address.id, ss.date.month_of_year, ss.date.year, ss.item.category, ss.item.current_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run --import raw/store_sales:ss select ss.item.category, max(item_cat_price) as price, max(cat_avg_price) as cat_avg where ss.date.year=2001 and ss.date.month_of_year=1 and ss.item.category is not null LIMIT 10;`

  ```text
  Syntax error in stdin: 2 undefined concept references; fix all before re-running:
    - local.item_cat_price (line 2, in SELECT); did you mean: ss.item.current_price, cat_avg_price?
    - local.cat_avg_price (line 2, in SELECT); did you mean: cat_avg, item_cat_price?
  ```
- `trilogy run --import raw/store_sales:ss WITH distinct_items AS SELECT ss.item.category as cat, ss.item.id as item_id, ss.item.current_price as price;
WITH cat_avg AS SELECT distinct_items.cat, avg(distinct_items.price) as avg_price;
select cat_avg.cat, cat_avg.avg_price limit 10;`

  ```text
  Syntax error in stdin: Undefined concept: cat_avg.cat (line 4, col 8, in SELECT). Suggestions: ['cat_avg.distinct_items.cat', 'distinct_items.cat', 'cat_avg.avg_price']
  ```
- `trilogy run --import raw/store_sales:ss WITH distinct_items AS SELECT ss.item.category as cat, ss.item.id as item_id, ss.item.current_price as price;
WITH ca…ddress.state,
    ss.item.category,
    ss.item.current_price,
    cat_avg.avg_price
WHERE ss.date.year = 2001 AND ss.date.month_of_year = 1
LIMIT 5;`

  ```text
  Unexpected error in stdin: (_duckdb.BinderException) Binder Error: Values list "questionable" does not have a column named "ss_item_category"

  LINE 64:     "questionable"."ss_item_category" as "ss_item_category",
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "ss_item_items"."I_CATEGORY" as "_distinct_items_cat",
      "ss_item_items"."I_CATEGORY" as "cat_avg_distinct_items_cat",
      "ss_item_items"."I_CURRENT_PRICE" as "_distinct_items_price",
      "ss_item_items"."I_CURRENT_PRICE" as "ss_item_current_price",
      "ss_item_items"."I_ITEM_SK" as "_distinct_items_item_id",
      "ss_item_items"."I_ITEM_SK" as "ss_item_id"
  FROM
      "item" as "ss_item_items"),
  thoughtful as (
  SELECT
      "ss_store_sales"."SS_CUSTOMER_SK" as "ss_customer_id",
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
      "ss_store_sales"."SS_SOLD_DATE_SK" as "ss_date_id"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1,
      2,
      3),
  abundant as (
  SELECT
      "cheerful"."_distinct_items_cat" as "distinct_items_cat",
      "cheerful"."_distinct_items_price" as "distinct_items_price"
  FROM
      "cheerful"
  GROUP BY
      1,
      2,
      "cheerful"."_distinct_items_item_id"),
  questionable as (
  SELECT
      "cheerful"."cat_avg_distinct_items_cat" as "cat_avg_distinct_items_cat",
      "cheerful"."ss_item_current_price" as "ss_item_current_price",
      "ss_customer_address_customer_address"."CA_STATE" as "ss_customer_address_state"
  FROM
      "thoughtful"
      INNER JOIN "date_dim" as "ss_date_date" on "thoughtful"."ss_date_id" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "cheerful" on "thoughtful"."ss_item_id" = "cheerful"."ss_item_id"
      LEFT OUTER JOIN "customer" as "ss_customer_customers" on "thoughtful"."ss_customer_id" = "ss_customer_customers"."C_CUSTOMER_SK"
      LEFT OUTER JOIN "customer_address" as "ss_customer_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_address_customer_address"."CA_ADDRESS_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2001 and "ss_date_date"."D_MOY" = 1
  ),
  yummy as (
  SELECT
      "abundant"."distinct_items_cat" as "distinct_items_cat",
      avg("abundant"."distinct_items_price") as "_cat_avg_avg_price"
  FROM
      "abundant"
  GROUP BY
      1),
  juicy as (
  SELECT
      "yummy"."_cat_avg_avg_price" as "cat_avg_avg_price",
      "yummy"."distinct_items_cat" as "cat_avg_distinct_items_cat"
  FROM
      "yummy")
  SELECT
      "questionable"."ss_customer_address_state" as "ss_customer_address_state",
      "questionable"."ss_item_category" as "ss_item_category",
      "questionable"."ss_item_current_price" as "ss_item_current_price",
      "juicy"."cat_avg_avg_price" as "cat_avg_avg_price"
  FROM
      "questionable"
      LEFT OUTER JOIN "juicy" on "questionable"."cat_avg_distinct_items_cat" = "juicy"."cat_avg_distinct_items_cat"
  LIMIT (5)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run --import raw/store_sales:ss WITH distinct_items AS SELECT ss.item.category as cat, ss.item.id as item_id, ss.item.current_price as price WHERE ss…HERE ss.date.year=2001 and ss.date.month_of_year=1 and ss.customer.address.state is null and ss.item.current_price > 1.2 * cat_avg.avg_price limit 5;`

  ```text
  Resolution error in stdin: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {cat_avg.avg_price, ss.customer.address.state, ss.date.month_of_year, ss.date.year, ss.item.current_price, ss.line_item}
  ```
- `trilogy file read query06.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query07.preql`

  ```text
  Resolution error in query07.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {cd.education_status, cd.gender, cd.marital_status}; {local.avg_coupon_amt, local.avg_list_price, local.avg_quantity, local.avg_sales_price, local.item_code, ss.date.year}; {prom.channel_email, prom.channel_event}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query08.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query12.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: Undefined concept: sales.line_item (line 20, in SELECT). Suggestions: ['sales.item.id', 'sales.net_profit', 'sales.sales_price']
  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query20.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query21.preql`

  ```text
  Resolution error in query21.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {item.current_price, local.item_code}; {local.after_total, local.before_qoh, local.before_total}; {local.warehouse_name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query21.preql`

  ```text
  Resolution error in query21.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {item.current_price, local.item_code}; {local.after_total, local.before_total}; {local.warehouse_name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query22.preql duckdb`

  ```text
  Resolution error in query22.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {d.year}; {item.brand_name, item.category, item.class, item.product_name}; {local.avg_qty_on_hand}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Undefined concept: best_cust_set.cust_id (line 42, col 42, in WHERE). Suggestions: ['best_cust_set.customer_store_totals.cust_id', 'customer_store_totals.cust_id']
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.customer.last_name as ln, store_sales.customer.first_name as fn, sum(store_sales.quantity…store_sales.customer.id is not null having total > 0.5 * max(sum(store_sales.quantity * store_sales.sales_price)) by * order by total desc limit 100;`

  ```text
  Syntax error in stdin: HAVING clause aggregate `max(sum(multiply(ref:store_sales.quantity,ref:store_sales.sales_price))) by __preql_internal.all_rows` is not in the SELECT projection (line 2). HAVING can only filter on off-grain or nested aggregates that are also computed in the SELECT. Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --max(sum(multiply(ref:store_sales.quantity,ref:store_sales.sales_price))) by __preql_internal.all_rows`; (b) move the filter to WHERE — for an aggregate condition on a non-output grain, write the aggregate inline as `agg(x) by grain` directly in WHERE.
  ```
- `trilogy run query24.preql`

  ```text
  Syntax error in query24.preql: HAVING references 'local.avg_fine', which is not in the SELECT projection (line 13). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.avg_fine
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query26.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query29.preql`

  ```text
  Syntax error in query29.preql: Undefined concept: ss_filtered.ticket_number. Suggestions: ['ss_filtered.ss.ticket_number', 'ss.ticket_number', 'pr.ticket_number', 'pr_filtered.pr.ticket_number']
  ```
- `trilogy file read raw/physical_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query30.preql`

  ```text
  Syntax error in query30.preql: HAVING references 'local.state_avg', which is not in the SELECT projection (line 9). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.state_avg
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query30.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query31.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query33.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query34.preql`

  ```text
  Unexpected error in query34.preql: Unknown cast target 'decimal': expected a data type or a registered trait.
  ```
- `trilogy file read query35.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 26 column 14 (char 1213). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query37.preql`

  ```text
  Resolution error in query37.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {cs.item.current_price, cs.item.id, cs.item.manufacturer_id, local.item_code, local.item_description}; {inv.date.date, inv.quantity_on_hand}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query39.preql`

  ```text
  Syntax error in query39.preql: Undefined concept: cv_filtered.m. Suggestions: ['cv_filtered.monthly_stats.m', 'monthly_stats.m', 'cv_filtered.cv', '_cv_filtered_cv']
  ```
- `trilogy run query39.preql`

  ```text
  Syntax error in query39.preql: Undefined concept: monthly_stats.jan. Suggestions: ['monthly_stats.mn', 'monthly_stats.sd', 'monthly_stats.mo']
  ```
- `trilogy file read query39.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: Undefined concept: sale_address.id. Suggestions: ['ss.sale_address.id', 'ss.item.id', 'ss.date.id', 'ss.return_date.id', 'ss.time.id', 'ss.return_time.id']
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: Undefined concept: eligible_items.item.id. Suggestions: ['eligible_items.item_profits.item.id', 'ss.item.id', 'ss.date.id', 'ss.return_date.id', 'ss.time.id', 'ss.return_time.id']
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: Undefined concept: item_profits.id. Suggestions: ['item_profits.item.id', 'ss.item.id', 'ss.date.id', 'ss.return_date.id', 'ss.time.id', 'ss.return_time.id']
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: Undefined concept: eligible_items.item.id. Suggestions: ['eligible_items.item_profits.item.id', 'ss.item.id', 'ss.date.id', 'ss.return_date.id', 'ss.time.id', 'ss.return_time.id']
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: Undefined concept: best_ranked.product_name. Suggestions: ['best_ranked.item.product_name', 'ss.item.product_name', 'item.product_name', 'worst_ranked.item.product_name']
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {item.id, item.product_name, local._best_ranked_rnk, local.item_avg_profit, local.profit_threshold}; {store.id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {item.id, item.product_name}; {local.avg_profit}; {store.id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {item.id, item.product_name}; {local.avg_profit, ss.store.id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query44.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query45.preql`

  ```text
  Syntax error in query45.preql: Undefined concept: item.id. Suggestions: ['ws.item.id', 'ws.date.id', 'ws.ship_date.id', 'ws.return_date.id', 'ws.time.id', 'ws.return_time.id']
  ```
- `trilogy run query47.preql`

  ```text
  Syntax error in query47.preql: 16 undefined concept references; fix all before re-running:
    - monthly_data.category (line 25, col 5, in SELECT); did you mean: monthly_data.item.category, ss.item.category, item.category, monthly_data.date.year, monthly_data.name?
    - monthly_data.brand_name (line 26, col 5, in SELECT); did you mean: monthly_data.item.brand_name, ss.item.brand_name, item.brand_name, monthly_data.name, monthly_data.company_name?
    - monthly_data.name (line 27, col 5, in SELECT); did you mean: monthly_data.store.name, ss.store.name, ss.return_store.name, store.name, monthly_data.brand_name, monthly_data.year?
    - monthly_data.company_name (line 28, col 5, in SELECT); did you mean: monthly_data.store.company_name, ss.store.company_name, ss.return_store.company_name, store.company_name, monthly_data.brand_name, monthly_data.name?
    - monthly_data.year (line 29, col 5, in SELECT); did you mean: monthly_data.date.year, ss.date.year, ss.return_date.year, ss.customer.first_sales_date.year, ss.customer.first_shipto_date.year, ss.return_customer.first_sales_date.year?
    - monthly_data.month_of_year (line 30, col 5, in SELECT); did you mean: monthly_data.date.month_of_year, ss.date.month_of_year, ss.return_date.month_of_year, ss.customer.first_sales_date.month_of_year, ss.customer.first_shipto_date.month_of_year, ss.return_customer.first_sales_date.month_of_year?
    - monthly_data.year (line 33, col 159, in SELECT); did you mean: monthly_data.date.year, ss.date.year, ss.return_date.year, ss.customer.first_sales_date.year, ss.customer.first_shipto_date.year, ss.return_customer.first_sales_date.year?
    - monthly_data.month_of_year (line 33, col 182, in SELECT); did you mean: monthly_data.date.month_of_year, ss.date.month_of_year, ss.return_date.month_of_year, ss.customer.first_sales_date.month_of_year, ss.customer.first_shipto_date.month_of_year, ss.return_customer.first_sales_date.month_of_year?
    - monthly_data.year (line 34, col 160, in SELECT); did you mean: monthly_data.date.year, ss.date.year, ss.return_date.year, ss.customer.first_sales_date.year, ss.customer.first_shipto_date.year, ss.return_customer.first_sales_date.year?
    - monthly_data.month_of_year (line 34, col 183, in SELECT); did you mean: monthly_data.date.month_of_year, ss.date.month_of_year, ss.return_date.month_of_year, ss.customer.first_sales_date.month_of_year, ss.customer.first_shipto_date.month_of_year, ss.return_customer.first_sales_date.month_of_year?
    - monthly_data.category (line 43, col 5, in ORDER BY); did you mean: monthly_data.item.category, ss.item.category, item.category, monthly_data.date.year, monthly_data.name?
    - monthly_data.brand_name (line 44, col 5, in ORDER BY); did you mean: monthly_data.item.brand_name, ss.item.brand_name, item.brand_name, monthly_data.name, monthly_data.company_name?
    - monthly_data.name (line 45, col 5, in ORDER BY); did you mean: monthly_data.store.name, ss.store.name, ss.return_store.name, store.name, monthly_data.brand_name, monthly_data.year?
    - monthly_data.company_name (line 46, col 5, in ORDER BY); did you mean: monthly_data.store.company_name, ss.store.company_name, ss.return_store.company_name, store.company_name, monthly_data.brand_name, monthly_data.name?
    - monthly_data.year (line 47, col 5, in ORDER BY); did you mean: monthly_data.date.year, ss.date.year, ss.return_date.year, ss.customer.first_sales_date.year, ss.customer.first_shipto_date.year, ss.return_customer.first_sales_date.year?
    - monthly_data.month_of_year (line 48, col 5, in ORDER BY); did you mean: monthly_data.date.month_of_year, ss.date.month_of_year, ss.return_date.month_of_year, ss.customer.first_sales_date.month_of_year, ss.customer.first_shipto_date.month_of_year, ss.return_customer.first_sales_date.month_of_year?
  ```
- `trilogy run query51.preql`

  ```text
  Syntax error in query51.preql: 2 undefined concept references; fix all before re-running:
    - combined.web_daily_amt (line 34, in SELECT); did you mean: combined.web_daily.web_daily_amt, web_daily.web_daily_amt, combined.store_daily_amt?
    - combined.store_daily_amt (line 34, in SELECT); did you mean: combined.store_daily.store_daily_amt, store_daily.store_daily_amt, combined.web_daily_amt?
  ```
- `trilogy run query53.preql`

  ```text
  Syntax error in query53.preql: HAVING references 'local.rel_dev', which is not in the SELECT projection (line 24). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.rel_dev
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query53.preql`

  ```text
  Syntax error in query53.preql: 2 undefined concept references; fix all before re-running:
    - q_totals.manufacturer_id (line 29, col 5, in SELECT); did you mean: q_totals.ss.item.manufacturer_id, ss.item.manufacturer_id, q_totals.ss.date.quarter?
    - q_totals.manufacturer_id (line 38, col 5, in ORDER BY); did you mean: q_totals.ss.item.manufacturer_id, ss.item.manufacturer_id, q_totals.ss.date.quarter?
  ```
- `trilogy run query53.preql`

  ```text
  Syntax error in query53.preql: HAVING references 'local.rel_dev', which is not in the SELECT projection (line 29). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.rel_dev
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query53.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query57.preql`

  ```text
  Syntax error in query57.preql: 10 undefined concept references; fix all before re-running:
    - monthly_totals.category (line 19, col 5, in SELECT); did you mean: monthly_totals.cs.item.category, cs.item.category, monthly_totals.name, monthly_totals.year?
    - monthly_totals.brand_name (line 20, col 5, in SELECT); did you mean: monthly_totals.cs.item.brand_name, cs.item.brand_name, monthly_totals.name, monthly_totals.year?
    - monthly_totals.name (line 21, col 5, in SELECT); did you mean: monthly_totals.cs.call_center.name, cs.call_center.name, cs.warehouse.name, monthly_totals.brand_name, monthly_totals.year, monthly_totals.category?
    - monthly_totals.year (line 22, col 5, in SELECT); did you mean: monthly_totals.cs.sold_date.year, cs.date.year, cs.ship_date.year, cs.sold_date.year, cs.ship_customer.first_sales_date.year, cs.ship_customer.first_shipto_date.year?
    - monthly_totals.month_of_year (line 23, col 5, in SELECT); did you mean: monthly_totals.cs.sold_date.month_of_year, cs.date.month_of_year, cs.ship_date.month_of_year, cs.sold_date.month_of_year, cs.ship_customer.first_sales_date.month_of_year, cs.ship_customer.first_shipto_date.month_of_year?
    - monthly_totals.category (line 34, col 5, in ORDER BY); did you mean: monthly_totals.cs.item.category, cs.item.category, monthly_totals.name, monthly_totals.year?
    - monthly_totals.brand_name (line 35, col 5, in ORDER BY); did you mean: monthly_totals.cs.item.brand_name, cs.item.brand_name, monthly_totals.name, monthly_totals.year?
    - monthly_totals.name (line 36, col 5, in ORDER BY); did you mean: monthly_totals.cs.call_center.name, cs.call_center.name, cs.warehouse.name, monthly_totals.brand_name, monthly_totals.year, monthly_totals.category?
    - monthly_totals.year (line 37, col 5, in ORDER BY); did you mean: monthly_totals.cs.sold_date.year, cs.date.year, cs.ship_date.year, cs.sold_date.year, cs.ship_customer.first_sales_date.year, cs.ship_customer.first_shipto_date.year?
    - monthly_totals.month_of_year (line 38, col 5, in ORDER BY); did you mean: monthly_totals.cs.sold_date.month_of_year, cs.date.month_of_year, cs.ship_date.month_of_year, cs.sold_date.month_of_year, cs.ship_customer.first_sales_date.month_of_year, cs.ship_customer.first_shipto_date.month_of_year?
  ```
- `trilogy run query59.preql`

  ```text
  Syntax error in query59.preql: Undefined concept: local.store. Suggestions: ['ss.store.id']
  ```
- `trilogy run --import raw/store_sales:ss select ss.date.day_name, sum(ss.sales_price) as total where ss.date.year = 2001 order by ss.date.day_of_week limit 10;`

  ```text
  Syntax error in stdin: ORDER BY references 'ss.date.day_of_week', which is not in the SELECT projection (line 2). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.date.day_of_week order by ss.date.day_of_week asc`.
  ```
- `trilogy file read query59.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query60.preql`

  ```text
  Syntax error in query60.preql: Undefined concept: item.category. Suggestions: ['all_sales.item.category', 'all_sales.item.category_id']
  ```
- `trilogy run query63.preql`

  ```text
  Syntax error in query63.preql: Undefined concept: date.year. Suggestions: ['sales.date.year', 'sales.store.date.year', 'sales.return_store.date.year', 'sales.return_date.year', 'sales.customer.first_sales_date.year', 'sales.customer.first_shipto_date.year']
  ```
- `trilogy run query63.preql`

  ```text
  Syntax error in query63.preql: 2 undefined concept references; fix all before re-running:
    - mgr_month.manager_id (line 25, col 5, in SELECT); did you mean: mgr_month.sales.item.manager_id, sales.item.manager_id, mgr_month.monthly_total?
    - mgr_month.manager_id (line 30, col 10, in ORDER BY); did you mean: mgr_month.sales.item.manager_id, sales.item.manager_id, mgr_month.monthly_total?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: HAVING references 'cs.ext_list_price', 'cr.refunded_cash', 'cr.reversed_charge', 'cr.store_credit', 'cr.item.text_id', which are not in the SELECT projection (line 43). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --cs.ext_list_price, --cr.refunded_cash, --cr.reversed_charge, --cr.store_credit, --cr.item.text_id
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: y1999.sale_year. Suggestions: ['agg_store_sales.sale_year', 'y1999.sale_city', 'y1999.sale_lines', 'y1999.sale_zip']
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {cs.item.id, cs.item.text_id, local.cr_refund_by_item, local.cs_ext_list_by_item}
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {y1999.cust_city, y1999.cust_street_name, y1999.cust_street_number, y1999.cust_zip, y1999.item_text_id, y1999.product_name, y1999.sale_city, y1999.sale_lines, y1999.sale_street_name, y1999.sale_street_number, y1999.sale_year, y1999.sale_zip, y1999.store_name, y1999.store_zip, y1999.total_coupon_amt, y1999.total_list_price, y1999.total_wholesale_cost, y2000.sale_lines, y2000.sale_year, y2000.store_name, y2000.store_zip, y2000.total_coupon_amt, y2000.total_list_price, y2000.total_wholesale_cost}
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: HAVING references 'local.store_avg_rev', which is not in the SELECT projection (line 9). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.store_avg_rev
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.all_sales:s select s.warehouse.id as wh_id, s.warehouse.name as w_name where s.channel in ('WEB', 'CATALOG') and s.date.year = 2001 …s.time.time between 30838 and 59638 and s.ship_mode.carrier in ('DHL', 'BARIAN') and s.warehouse.id is not null and s.warehouse.name is null limit 5;`

  ```text
  Unexpected error in stdin: (_duckdb.BinderException) Binder Error: Referenced table "s_warehouse_warehouse" not found!
  Candidate tables: "cheerful"

  LINE 28:     "s_warehouse_warehouse"."w_warehouse_sk" as "wh_id",
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "s_warehouse_warehouse"."w_warehouse_name" as "s_warehouse_name"
  FROM
      "catalog_sales" as "s_catalog_sales_unified"
      INNER JOIN "date_dim" as "s_date_date" on "s_catalog_sales_unified"."CS_SOLD_DATE_SK" = "s_date_date"."D_DATE_SK"
      INNER JOIN "ship_mode" as "s_ship_mode_ship_mode" on "s_catalog_sales_unified"."CS_SHIP_MODE_SK" = "s_ship_mode_ship_mode"."SM_SHIP_MODE_SK"
      INNER JOIN "time_dim" as "s_time_time" on "s_catalog_sales_unified"."CS_SOLD_TIME_SK" = "s_time_time"."T_TIME_SK"
      INNER JOIN "warehouse" as "s_warehouse_warehouse" on "s_catalog_sales_unified"."CS_WAREHOUSE_SK" = "s_warehouse_warehouse"."w_warehouse_sk"
  WHERE
      "s_catalog_sales_unified"."CS_WAREHOUSE_SK" is not null and "s_date_date"."D_YEAR" = 2001 and "s_ship_mode_ship_mode"."SM_CARRIER" in ('DHL','BARIAN') and "s_time_time"."T_TIME" BETWEEN 30838 AND 59638 and "s_warehouse_warehouse"."w_warehouse_name" is null

  UNION ALL
  SELECT
      "s_warehouse_warehouse"."w_warehouse_name" as "s_warehouse_name"
  FROM
      "web_sales" as "s_web_sales_unified"
      INNER JOIN "date_dim" as "s_date_date" on "s_web_sales_unified"."WS_SOLD_DATE_SK" = "s_date_date"."D_DATE_SK"
      INNER JOIN "ship_mode" as "s_ship_mode_ship_mode" on "s_web_sales_unified"."WS_SHIP_MODE_SK" = "s_ship_mode_ship_mode"."SM_SHIP_MODE_SK"
      INNER JOIN "time_dim" as "s_time_time" on "s_web_sales_unified"."WS_SOLD_TIME_SK" = "s_time_time"."T_TIME_SK"
      INNER JOIN "warehouse" as "s_warehouse_warehouse" on "s_web_sales_unified"."WS_WAREHOUSE_SK" = "s_warehouse_warehouse"."w_warehouse_sk"
  WHERE
      "s_web_sales_unified"."WS_WAREHOUSE_SK" is not null and "s_date_date"."D_YEAR" = 2001 and "s_ship_mode_ship_mode"."SM_CARRIER" in ('DHL','BARIAN') and "s_time_time"."T_TIME" BETWEEN 30838 AND 59638 and "s_warehouse_warehouse"."w_warehouse_name" is null
  )
  SELECT
      "s_warehouse_warehouse"."w_warehouse_sk" as "wh_id",
      "cheerful"."s_warehouse_name" as "w_name"
  FROM
      "cheerful"
  GROUP BY
      1,
      2
  LIMIT (5)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query66.preql`

  ```text
  Resolution error in query66.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {all_months.month, local.monthly_net, local.monthly_sales, local.sales_per_sqft, local.ship_carriers, local.year, wh_groups.w_city, wh_groups.w_country, wh_groups.w_county, wh_groups.w_name, wh_groups.w_sqft, wh_groups.w_state}
  ```
- `trilogy run query70.preql`

  ```text
  Syntax error in query70.preql: Undefined concept: top_states.state (line 27, col 34, in WHERE). Suggestions: ['top_states.store_sales.store.state', 'store_sales.customer.address.state', 'store_sales.return_customer.address.state', 'store_sales.store.state', 'store_sales.return_store.state', 'store_sales.sale_address.state']
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query72.preql duckdb`

  ```text
  Syntax error in query72.preql: HAVING references 'inv.quantity_on_hand', 'cs.quantity', which are not in the SELECT projection (line 4). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --inv.quantity_on_hand, --cs.quantity
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query72.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query74.preql`

  ```text
  Syntax error in query74.preql: Have {'RowsetNode<store_cust.fname,store_cust.lname,store_cust.store_2001...2 more>': None} and need store_cust.store_2001 > 0 and store_cust.store_2002 > 0 and web_cust.web_2001 > 0 and web_cust.web_2002 > 0 and (divide(web_cust.web_2002@Grain<web_cust.cust_code,web_cust.fname,web_cust.lname>,web_cust.web_2001@Grain<web_cust.cust_code,web_cust.fname,web_cust.lname>)) > (divide(store_cust.store_2002@Grain<store_cust.cust_code,store_cust.fname,store_cust.lname>,store_cust.store_2001@Grain<store_cust.cust_code,store_cust.fname,store_cust.lname>))
  ```
- `trilogy run query74.preql`

  ```text
  Syntax error in query74.preql: HAVING references 'store_cust.store_2001', 'store_cust.store_2002', 'web_cust.web_2001', 'web_cust.web_2002', which are not in the SELECT projection (line 24). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --store_cust.store_2001, --store_cust.store_2002, --web_cust.web_2001, --web_cust.web_2002
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query75.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Output column 'prev_qty' aliases 'local.prev_qty', which is itself the 'prev_qty' output of a union(...)/rowset, so the rename refers back to itself. Use a distinct output name (e.g. 'prev_qty_out').
  ```
- `trilogy run query75.preql`

  ```text
  Unexpected error in query75.preql: Invalid reference string found in query:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
      "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
      "sales_catalog_returns_unified"."CR_RETURN_AMOUNT" as "sales_return_amount",
      "sales_catalog_returns_unified"."CR_RETURN_QUANTITY" as "sales_return_quantity"
  FROM
      "catalog_returns" as "sales_catalog_returns_unified"
  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
      "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
      "sales_store_returns_unified"."SR_RETURN_AMT" as "sales_return_amount",
      "sales_store_returns_unified"."SR_RETURN_QUANTITY" as "sales_return_quantity"
  FROM
      "store_returns" as "sales_store_returns_unified"
  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
      "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
      "sales_web_returns_unified"."WR_RETURN_AMT" as "sales_return_amount",
      "sales_web_returns_unified"."WR_RETURN_QUANTITY" as "sales_return_quantity"
  FROM
      "web_returns" as "sales_web_returns_unified"),
  abundant as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
      "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
      "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
      "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
      "sales_date_date"."D_YEAR" as "sales_date_year"
  FROM
      "catalog_sales" as "sales_catalog_sales_unified"
      INNER JOIN "date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
      INNER JOIN "item" as "sales_item_items" on "sales_catalog_sales_unified"."CS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
  WHERE
      "sales_date_date"."D_YEAR" in (2001,2002) and "sales_item_items"."I_CATEGORY" = 'Books'

  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
      "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
      "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
      "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
      "sales_date_date"."D_YEAR" as "sales_date_year"
  FROM
      "store_sales" as "sales_store_sales_unified"
      INNER JOIN "date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
      INNER JOIN "item" as "sales_item_items" on "sales_store_sales_unified"."SS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
  WHERE
      "sales_date_date"."D_YEAR" in (2001,2002) and "sales_item_items"."I_CATEGORY" = 'Books'

  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
      "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
      "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
      "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
      "sales_date_date"."D_YEAR" as "sales_date_year"
  FROM
      "web_sales" as "sales_web_sales_unified"
      INNER JOIN "date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
      INNER JOIN "item" as "sales_item_items" on "sales_web_sales_unified"."WS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
  WHERE
      "sales_date_date"."D_YEAR" in (2001,2002) and "sales_item_items"."I_CATEGORY" = 'Books'
  ),
  juicy as (
  SELECT
      "abundant"."sales_date_year" as "_deduped_yr",
      "sales_item_items"."I_BRAND_ID" as "_deduped_brand_id",
      "sales_item_items"."I_CATEGORY_ID" as "_deduped_cat_id",
      "sales_item_items"."I_CLASS_ID" as "_deduped_class_id",
      "sales_item_items"."I_MANUFACT_ID" as "_deduped_mfr_id",
      coalesce("abundant"."sales_ext_sales_price",0) - coalesce("cheerful"."sales_return_amount",0) as "_deduped_net_amt",
      coalesce("abundant"."sales_quantity",0) - coalesce("cheerful"."sales_return_quantity",0) as "_deduped_net_qty"
  FROM
      "abundant"
      LEFT OUTER JOIN "cheerful" on "abundant"."sales_channel" = "cheerful"."sales_channel" AND "abundant"."sales_item_id" = "cheerful"."sales_item_id" AND "abundant"."sales_order_id" = "cheerful"."sales_order_id"
      INNER JOIN "item" as "sales_item_items" on "abundant"."sales_item_id" = "sales_item_items"."I_ITEM_SK"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7),
  vacuous as (
  SELECT
      "juicy"."_deduped_brand_id" as "_aggregated_brand_id",
      "juicy"."_deduped_cat_id" as "_aggregated_cat_id",
      "juicy"."_deduped_class_id" as "_aggregated_class_id",
      "juicy"."_deduped_mfr_id" as "_aggregated_mfr_id",
      "juicy"."_deduped_yr" as "_aggregated_yr",
      sum("juicy"."_deduped_net_amt") as "_aggregated_total_amt",
      sum("juicy"."_deduped_net_qty") as "_aggregated_total_qty"
  FROM
      "juicy"
  GROUP BY
      1,
      2,
      3,
      4,
      5),
  young as (
  SELECT
      "vacuous"."_aggregated_brand_id" as "aggregated_brand_id",
      "vacuous"."_aggregated_cat_id" as "aggregated_cat_id",
      "vacuous"."_aggregated_class_id" as "aggregated_class_id",
      "vacuous"."_aggregated_mfr_id" as "aggregated_mfr_id",
      "vacuous"."_aggregated_total_amt" as "aggregated_total_amt",
      "vacuous"."_aggregated_total_qty" as "aggregated_total_qty",
      "vacuous"."_aggregated_yr" as "aggregated_yr"
  FROM
      "vacuous"),
  sparkling as (
  SELECT
      "young"."aggregated_brand_id" as "aggregated_brand_id",
      "young"."aggregated_cat_id" as "aggregated_cat_id",
      "young"."aggregated_class_id" as "aggregated_class_id",
      "young"."aggregated_mfr_id" as "aggregated_mfr_id",
      "young"."aggregated_total_amt" as "aggregated_total_amt",
      "young"."aggregated_total_qty" as "aggregated_total_qty",
      "young"."aggregated_yr" as "aggregated_yr"
  FROM
      "young"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7),
  abhorrent as (
  SELECT
      "sparkling"."aggregated_brand_id" as "aggregated_brand_id",
      "sparkling"."aggregated_cat_id" as "aggregated_cat_id",
      "sparkling"."aggregated_class_id" as "aggregated_class_id",
      "sparkling"."aggregated_mfr_id" as "aggregated_mfr_id",
      "sparkling"."aggregated_total_amt" as "aggregated_total_amt",
      "sparkling"."aggregated_total_qty" as "aggregated_total_qty",
      lag("sparkling"."aggregated_total_amt", 1) over (partition by "sparkling"."aggregated_brand_id","sparkling"."aggregated_class_id","sparkling"."aggregated_cat_id","sparkling"."aggregated_mfr_id" order by "sparkling"."aggregated_yr" asc ) as "_virt_window_lag_2164656292228880",
      lag("sparkling"."aggregated_total_qty", 1) over (partition by "sparkling"."aggregated_brand_id","sparkling"."aggregated_class_id","sparkling"."aggregated_cat_id","sparkling"."aggregated_mfr_id" order by "sparkling"."aggregated_yr" asc ) as "_virt_window_lag_7627298183929594",
      lag("sparkling"."aggregated_total_qty", 1) over (partition by "sparkling"."aggregated_brand_id","sparkling"."aggregated_class_id","sparkling"."aggregated_cat_id","sparkling"."aggregated_mfr_id" order by "sparkling"."aggregated_yr" asc ) as "prev_qty"
  FROM
      "sparkling"),
  sweltering as (
  SELECT
      "abhorrent"."aggregated_brand_id" as "brand_id",
      "abhorrent"."aggregated_cat_id" as "cat_id",
      "abhorrent"."aggregated_class_id" as "class_id",
      "abhorrent"."aggregated_mfr_id" as "mfr_id",
      "abhorrent"."aggregated_total_amt" - "abhorrent"."_virt_window_lag_2164656292228880" as "amt_diff",
      "abhorrent"."aggregated_total_qty" - "abhorrent"."_virt_window_lag_7627298183929594" as "qty_diff",
      "abhorrent"."aggregated_total_qty" as "current_qty",
      "abhorrent"."prev_qty" as "prev_qty",
      2001 as "previous_year",
      2002 as "current_year"
  FROM
      "abhorrent"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      "abhorrent"."_virt_window_lag_2164656292228880",
      "abhorrent"."_virt_window_lag_7627298183929594",
      "abhorrent"."aggregated_total_amt")
  SELECT
      "sweltering"."current_year" as "current_year",
      "sweltering"."previous_year" as "previous_year",
      "sweltering"."brand_id" as "brand_id",
      "sweltering"."class_id" as "class_id",
      "sweltering"."cat_id" as "cat_id",
      "sweltering"."mfr_id" as "mfr_id",
      "sweltering"."prev_qty" as "prev_qty",
      "sweltering"."current_qty" as "current_qty",
      "sweltering"."qty_diff" as "qty_diff",
      "sweltering"."amt_diff" as "amt_diff"
  FROM
      "sweltering"
  WHERE
      "sweltering"."prev_qty" is not null and INVALID_REFERENCE_BUG = 2002 and "sweltering"."current_qty" / ("sweltering"."prev_qty" * 1.0) < 0.9

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10
  ORDER BY
      "sweltering"."qty_diff" asc,
      "sweltering"."amt_diff" asc
  LIMIT (100), this should never occur. Please create an issue to report this.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query77.preql`

  ```text
  Syntax error in query77.preql: ORDER BY contains aggregate `sum(<Filter: coalesce(ref:s.return_amount,0) where ref:s.return_date.date between constant(2000-08-23) and constant(2000-09-22)>)` (line 5), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --sum(<Filter: coalesce(ref:s.return_amount,0) where ref:s.return_date.date between constant(2000-08-23) and constant(2000-09-22)>) as g order by g desc`.
  ```
- `trilogy run query77.preql`

  ```text
  Unexpected error in query77.preql: Invalid reference string found in query:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "s_channel",
      "s_catalog_returns_unified"."CR_ITEM_SK" as "s_item_id",
      "s_catalog_returns_unified"."CR_ORDER_NUMBER" as "s_order_id",
      "s_catalog_returns_unified"."CR_RETURN_AMOUNT" as "s_return_amount",
      "s_catalog_returns_unified"."CR_RETURNED_DATE_SK" as "s_return_date_id",
      "s_catalog_returns_unified"."CR_NET_LOSS" as "s_return_net_loss"
  FROM
      "catalog_returns" as "s_catalog_returns_unified"
  UNION ALL
  SELECT
       'STORE'  as "s_channel",
      "s_store_returns_unified"."SR_ITEM_SK" as "s_item_id",
      "s_store_returns_unified"."SR_TICKET_NUMBER" as "s_order_id",
      "s_store_returns_unified"."SR_RETURN_AMT" as "s_return_amount",
      "s_store_returns_unified"."SR_RETURNED_DATE_SK" as "s_return_date_id",
      "s_store_returns_unified"."SR_NET_LOSS" as "s_return_net_loss"
  FROM
      "store_returns" as "s_store_returns_unified"
  UNION ALL
  SELECT
       'WEB'  as "s_channel",
      "s_web_returns_unified"."WR_ITEM_SK" as "s_item_id",
      "s_web_returns_unified"."WR_ORDER_NUMBER" as "s_order_id",
      "s_web_returns_unified"."WR_RETURN_AMT" as "s_return_amount",
      "s_web_returns_unified"."WR_RETURNED_DATE_SK" as "s_return_date_id",
      "s_web_returns_unified"."WR_NET_LOSS" as "s_return_net_loss"
  FROM
      "web_returns" as "s_web_returns_unified"),
  abundant as (
  SELECT
       'CATALOG'  as "s_channel",
      "s_catalog_sales_unified"."CS_SOLD_DATE_SK" as "s_date_id",
      "s_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "s_ext_sales_price",
      "s_catalog_sales_unified"."CS_ITEM_SK" as "s_item_id",
      "s_catalog_sales_unified"."CS_NET_PROFIT" as "s_net_profit",
      "s_catalog_sales_unified"."CS_ORDER_NUMBER" as "s_order_id",
      "s_catalog_sales_unified"."CS_CALL_CENTER_SK" as "s_outlet_id"
  FROM
      "catalog_sales" as "s_catalog_sales_unified"
  WHERE
      "s_catalog_sales_unified"."CS_CALL_CENTER_SK" is not null

  UNION ALL
  SELECT
       'STORE'  as "s_channel",
      "s_store_sales_unified"."SS_SOLD_DATE_SK" as "s_date_id",
      "s_store_sales_unified"."SS_EXT_SALES_PRICE" as "s_ext_sales_price",
      "s_store_sales_unified"."SS_ITEM_SK" as "s_item_id",
      "s_store_sales_unified"."SS_NET_PROFIT" as "s_net_profit",
      "s_store_sales_unified"."SS_TICKET_NUMBER" as "s_order_id",
      "s_store_sales_unified"."SS_STORE_SK" as "s_outlet_id"
  FROM
      "store_sales" as "s_store_sales_unified"
  WHERE
      "s_store_sales_unified"."SS_STORE_SK" is not null

  UNION ALL
  SELECT
       'WEB'  as "s_channel",
      "s_web_sales_unified"."WS_SOLD_DATE_SK" as "s_date_id",
      "s_web_sales_unified"."WS_EXT_SALES_PRICE" as "s_ext_sales_price",
      "s_web_sales_unified"."WS_ITEM_SK" as "s_item_id",
      "s_web_sales_unified"."WS_NET_PROFIT" as "s_net_profit",
      "s_web_sales_unified"."WS_ORDER_NUMBER" as "s_order_id",
      "s_web_sales_unified"."WS_WEB_PAGE_SK" as "s_outlet_id"
  FROM
      "web_sales" as "s_web_sales_unified"
  WHERE
      "s_web_sales_unified"."WS_WEB_PAGE_SK" is not null
  ),
  juicy as (
  SELECT
      "abundant"."s_channel" as "s_channel",
      "abundant"."s_outlet_id" as "s_outlet_id",
      CASE WHEN cast("s_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-22' THEN "abundant"."s_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_56319405702519",
      CASE WHEN cast("s_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-22' THEN "abundant"."s_net_profit" ELSE NULL END as "_virt_filter_net_profit_231099731031653",
      CASE WHEN cast("s_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-22' THEN coalesce("cheerful"."s_return_amount",0) ELSE NULL END as "_virt_filter_2769281535303448",
      CASE WHEN cast("s_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-22' THEN coalesce("cheerful"."s_return_net_loss",0) ELSE NULL END as "_virt_filter_1494855988141452"
  FROM
      "abundant"
      LEFT OUTER JOIN "cheerful" on "abundant"."s_channel" = "cheerful"."s_channel" AND "abundant"."s_item_id" = "cheerful"."s_item_id" AND "abundant"."s_order_id" = "cheerful"."s_order_id"
      LEFT OUTER JOIN "date_dim" as "s_date_date" on "abundant"."s_date_id" = "s_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "date_dim" as "s_return_date_date" on "cheerful"."s_return_date_id" = "s_return_date_date"."D_DATE_SK"
  WHERE
      "abundant"."s_outlet_id" is not null

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      "abundant"."s_item_id",
      "abundant"."s_order_id"),
  sparkling as (
  SELECT
      "juicy"."s_outlet_id" as "outlet_id",
      CASE
  	WHEN "juicy"."s_channel" = 'STORE' THEN 'store channel'
  	WHEN "juicy"."s_channel" = 'CATALOG' THEN 'catalog channel'
  	WHEN "juicy"."s_channel" = 'WEB' THEN 'web channel'
  	END as "channel",
      sum("juicy"."_virt_filter_2769281535303448") as "returns",
      sum("juicy"."_virt_filter_ext_sales_price_56319405702519") as "sales",
      sum("juicy"."_virt_filter_net_profit_231099731031653") - sum("juicy"."_virt_filter_1494855988141452") as "profit"
  FROM
      "juicy"
  GROUP BY
      ROLLUP ("juicy"."s_channel", 1))
  SELECT
      "sparkling"."channel" as "channel",
      "sparkling"."outlet_id" as "outlet_id",
      "sparkling"."sales" as "sales",
      "sparkling"."returns" as "returns",
      "sparkling"."profit" as "profit"
  FROM
      "sparkling"
  WHERE
      "sparkling"."channel" is not null or "sparkling"."outlet_id" is not null

  ORDER BY
      INVALID_REFERENCE_BUG asc nulls first,
      "sparkling"."outlet_id" asc nulls first,
      "sparkling"."returns" desc
  LIMIT (100), this should never occur. Please create an issue to report this.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs select ss.customer.id as sid, cs.bill_customer.id as cid, count(ss.quantity) as cnt whe…turned=false and cs.date.year=2000 and cs.bill_customer.id is not null and cs.return_amount is null and ss.customer.id = cs.bill_customer.id limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {cs.bill_customer.id, cs.date.year, cs.return_amount, local.cid}; {local.cnt, local.sid, ss.customer.id, ss.date.year, ss.is_returned}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query78.preql`

  ```text
  Resolution error in query78.preql: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['cs.ext_sales_price', 'cs.item.wholesale_cost', 'cs.order_number', 'cs.quantity', 'cs.return_amount', 'ss.customer.id', 'ss.date.year', 'ss.is_returned', 'ss.item.id'], have {'cs.date.id'} or {'ss.date.id', 'ss.ticket_number'} or {'ws.date.id'}. Different paths are is: [{'cs.date.id'}, {'ss.date.id', 'ss.ticket_number'}, set()]")
  ```
- `trilogy run test_join.preql`

  ```text
  Unexpected error in test_join.preql: Cannot join `store_agg.item_id` to itself (`store_agg.item_id` resolves to the same concept `store_agg.item_id`), which degenerates to `1=1`. Join distinct concepts (e.g. separate rowset outputs).
  ```
- `trilogy run --import raw.all_sales:all select all.date.year as yr, all.item.id as iid, all.billing_customer.id as cid, sum(all.ext_wholesale_cost ? all.chann…channel = 'STORE' and all.is_returned is null) > 0 and sum(all.quantity ? all.channel in ('CATALOG', 'WEB') and all.is_returned is null) > 0 limit 3;`

  ```text
  Syntax error in stdin: HAVING references 'all.quantity', which is not in the SELECT projection (line 2). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --all.quantity
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query78.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query82.preql`

  ```text
  Resolution error in query82.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.item.id, inv.quantity_on_hand}; {item.current_price, item.id, item.manufacturer_id, local.current_price, local.description, local.item_code}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run --import raw.store_sales:store --import raw.catalog_returns:catalog --import raw.web_returns:web select store.item.text_id, count(store.return_qu…n (5244,5257,5264) or catalog.date.week_seq in (5244,5257,5264) or web.return_date.week_seq in (5244,5257,5264) order by store.item.text_id limit 10;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {catalog.date.week_seq, local.cat_count, local.cat_qty}; {local.store_count, local.store_qty, store.item.text_id, store.return_date.week_seq}; {local.web_count, local.web_qty, web.return_date.week_seq}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query83.preql`

  ```text
  Syntax error in query83.preql: Undefined concept: web.item.text_id. Suggestions: ['web.web_sales.item.text_id', 'store.item.text_id', 'store.date.text_id', 'store.return_date.text_id', 'store.time.text_id', 'store.return_time.text_id']
  ```
- `trilogy file read query84.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query86.preql`

  ```text
  Syntax error in query86.preql: Undefined concept: web_sales. Suggestions: ['web_sales.time.id', 'web_sales.item.id', 'web_sales.date.id']
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query86.preql`

  ```text
  Syntax error in query86.preql: Undefined concept: web_sales. Suggestions: ['web_sales.time.id', 'web_sales.item.id', 'web_sales.date.id']
  ```
- `trilogy run --import raw.web_sales select web_sales.item.category limit 3;`

  ```text
  Syntax error in stdin: Undefined concept: web_sales.item.category (line 2, col 8, in SELECT). Suggestions: ['item.category', 'item.category_id', 'web_site.name']
  ```
- `trilogy run query86.preql`

  ```text
  Syntax error in query86.preql: Undefined concept: web_sales. Suggestions: ['web_site.id', 'web_page.id']
  ```
- `trilogy run query87.preql`

  ```text
  Resolution error in query87.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {catalog_tuples.first_name, catalog_tuples.last_name, catalog_tuples.sale_date, store_tuples.first_name, store_tuples.last_name, store_tuples.sale_date}
  ```
- `trilogy file read query89.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:ss where ss.return_reason.desc = 'reason 28'
select
    count(ss.customer.id) as cnt
having ss.customer.id is not null
 …sum(case when ss.return_quantity is not null then (ss.quantity - ss.return_quantity) * ss.sales_price else ss.quantity * ss.sales_price end) is null;`

  ```text
  Syntax error in stdin: HAVING references 'ss.return_quantity', 'ss.quantity', 'ss.sales_price', which are not in the SELECT projection (line 2). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --ss.return_quantity, --ss.quantity, --ss.sales_price
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query93.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query94.preql`

  ```text
  Syntax error in query94.preql: Undefined concept: qualified_orders.order_number (line 25, col 35, in WHERE). Suggestions: ['qualified_orders.web_sales.order_number', 'web_sales.order_number', 'qualified_orders.has_return']
  ```
- `trilogy run query94.preql`

  ```text
  Syntax error in query94.preql: Undefined concept: orders_with_qualifying_line.order_number. Suggestions: ['orders_with_qualifying_line.web_sales.order_number', 'web_sales.order_number']
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query99.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query02.preql -e -c import raw.all_sales as sl;

# Rowset: per (week_seq, day_of_week), total ext_sales_price for WEB and CATALOG
with wee…week,
    with_next.total_sales,
    with_next.next_year_sales
order by with_next.sl.date.week_seq asc, with_next.sl.date.day_of_week asc
limit 200;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...with_next.sl.date.week_seq in ??? (     select sl.date.week_seq

  Write stats: received 1038 chars / 1038 bytes; tail: …'.week_seq asc, with_next.sl.date.day_of_week asc\\nlimit 200;\\n'.
  ```
- `trilogy file write query04.preql --content import raw.all_sales as s;

# Annual value per billing customer-channel-year
with ann_val as
where s.date.year in …t 5
select
    store01.cust_id,
    store01.fname,
    store01.lname,
    store01.s01,
    store02.s02
inner join store01.cust_id = store02.cust_id
;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
    --> 35:1
     |
  35 | limit 5
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...id,     ann_val.val as s02 ;  ??? limit 5 select     store01.cus...

  Write stats: received 1027 chars / 1027 bytes; tail: …'  store02.s02\\ninner join store01.cust_id = store02.cust_id\\n;'.
  ```
- `trilogy run --import raw.all_sales:all_sales where all_sales.is_returned is not null and all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date select all_sales.channel, count(*) as cnt, sum(all_sales.ext_sales_price) as gross, sum(all_sales.return_amount) as ret_amount limit 5;`

  ```text
  Syntax error in stdin:  --> 2:147
    |
  2 | where all_sales.is_returned is not null and all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date select all_sales.channel, count(*) as cnt, sum(all_sales.ext_sales_price) as gross, sum(all_sales.return_amount) as ret_amount limit 5;
    |                                                                                                                                                   ^---
    |
    = expected access_chain
  Location:
  ...lect all_sales.channel, count( ??? *) as cnt, sum(all_sales.ext_s...
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Channel type label
auto channel_label <- simple_case(sales.channel,
    'STORE',…aving
    total_gross_sales > 0 or total_returns > 0
order by
    g_channel asc,
    channel_label asc,
    sale_entity_id asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:34
    |
  4 | auto channel_label <- simple_case(sales.channel,
    |                                  ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...o channel_label <- simple_case ??? (sales.channel,     'STORE', '...

  Write stats: received 2300 chars / 2300 bytes; tail: …'nel_label asc,\\n    sale_entity_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.all_sales:all_sales select sales.channel, count(sales.row_one) as cnt group by 1;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ..., count(sales.row_one) as cnt ??? group by 1;
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Channel type label
auto channel_label <- case
    when sales.channel = 'STORE' t…_entity
having
    total_gross_sales > 0 or total_returns > 0
order by
    @g1() asc,
    channel_label asc,
    entity_id asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 44:8
     |
  44 | def g1() -> grouping(sales.channel) by rollup sales.channel, entity_id;
     |        ^---
     |
     = expected IDENTIFIER
  Location:
  ...s.channel, entity_id;  def g1( ??? ) -> grouping(sales.channel) b...

  Write stats: received 2063 chars / 2063 bytes; tail: …' channel_label asc,\\n    entity_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, count(*) as cnt where all_sales.channel_dim_id is null and all_sales.return_channel_dim_id is not null and all_sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date limit 10;`

  ```text
  Syntax error in stdin:  --> 2:33
    |
  2 | select all_sales.channel, count(*) as cnt where all_sales.channel_dim_id is null and all_sales.return_channel_dim_id is not null and all_sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date limit 10;
    |                                 ^---
    |
    = expected access_chain
  Location:
  ...lect all_sales.channel, count( ??? *) as cnt where all_sales.chan...
  ```
- `trilogy run --import raw.all_sales:all_sales select case when all_sales.channel = 'STORE' then 'store channel' when all_sales.channel = 'CATALOG' then 'catal…channel = 'WEB' then concat('web_site', all_sales.return_channel_dim_text_id) end end) as total where all_sales.channel in ('STORE', 'WEB') limit 30;`

  ```text
  Syntax error in stdin:  --> 2:1061
    |
  2 | select case when all_sales.channel = 'STORE' then 'store channel' when all_sales.channel = 'CATALOG' then 'catalog channel' when all_sales.channel = 'WEB' then 'web channel' end as channel_label, case when all_sales.channel_dim_id is not null then case when all_sales.channel = 'STORE' then concat('store', all_sales.channel_dim_text_id) when all_sales.channel = 'CATALOG' then concat('catalog_page', all_sales.channel_dim_text_id) when all_sales.channel = 'WEB' then concat('web_site', all_sales.channel_dim_text_id) end when all_sales.return_channel_dim_id is not null then case when all_sales.channel = 'STORE' then concat('store', all_sales.return_channel_dim_text_id) when all_sales.channel = 'CATALOG' then concat('catalog_page', all_sales.return_channel_dim_text_id) when all_sales.channel = 'WEB' then concat('web_site', all_sales.return_channel_dim_text_id) end end as entity_id, sum(all_sales.ext_sales_price ? all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date and all_sales.channel_dim_id is not null) by rollup all_sales.channel, (case when all_sales.channel_dim_id is not null then case when all_sales.channel = 'STORE' then concat('store', all_sales.channel_dim_text_id) when all_sales.channel = 'CATALOG' then concat('catalog_page', all_sales.channel_dim_text_id) when all_sales.channel = 'WEB' then concat('web_site', all_sales.channel_dim_text_id) end when all_sales.return_channel_dim_id is not null then case when all_sales.channel = 'STORE' then concat('store', all_sales.return_channel_dim_text_id) when all_sales.channel = 'CATALOG' then concat('catalog_page', all_sales.return_channel_dim_text_id) when all_sales.channel = 'WEB' then concat('web_site', all_sales.return_channel_dim_text_id) end end) as total where all_sales.channel in ('STORE', 'WEB') limit 30;
    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...l) by rollup all_sales.channel ??? , (case when all_sales.channel...
  ```
- `trilogy run --import raw.all_sales:all_sales where all_sales.channel = 'STORE' and all_sales.channel_dim_id is not null and all_sales.channel_dim_text_id is … between '2000-08-23'::date and '2000-09-06'::date select all_sales.channel_dim_text_id, sum(all_sales.ext_sales_price) as total group by 1 limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...les.ext_sales_price) as total ??? group by 1 limit 10;
  ```
- `trilogy run --import raw.all_sales:all_sales where all_sales.channel_dim_id is not null and all_sales.channel_dim_text_id is null and all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date select all_sales.channel, count(*) as cnt limit 5;`

  ```text
  Syntax error in stdin:  --> 2:192
    |
  2 | where all_sales.channel_dim_id is not null and all_sales.channel_dim_text_id is null and all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date select all_sales.channel, count(*) as cnt limit 5;
    |                                                                                                                                                                                                ^---
    |
    = expected access_chain
  Location:
  ...lect all_sales.channel, count( ??? *) as cnt limit 5;
  ```
- `trilogy run --import raw/store_sales:ss select ss.item.category, avg(ss.item.current_price) as avg_price, count(ss.line_item) as cnt where ss.date.year=2001 and ss.date.month_of_year=1 group by ss.item.category limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...1 and ss.date.month_of_year=1 ??? group by ss.item.category limi...
  ```
- `trilogy run --import raw/item:it select it.category, avg(it.current_price) as avg_price_by_item, count(it.id) as num_items where it.category is not null group by it.category limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...where it.category is not null ??? group by it.category limit 10;...
  ```
- `trilogy file write query08.preql --content import raw.store_sales as ss;
import raw.customer as c;
import raw.address as a;

parameter zips string;

# Unnest…s.store.zip in pref_zip_set.zip
select
    ss.store.name as store_name,
    sum(ss.net_profit) as total_net_profit
order by
    store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:1
    |
  8 | rowset param_zips as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
   split zip list into a rowset ??? rowset param_zips as select

  Write stats: received 882 chars / 882 bytes; tail: …'ofit) as total_net_profit\\norder by\\n    store_name\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Step 1: Qualification - brand/class/category combos in all 3 channels 1999-2001
…l
order by sales.channel nulls first, sales.item.brand_id nulls first, sales.item.class_id nulls first, sales.item.category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...endent of main query's WHERE) ??? rowset qual_combo as where sal...

  Write stats: received 1478 chars / 1478 bytes; tail: …'d nulls first, sales.item.category_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Qualification - brand/class/category combos in all 3 channels 1999-2001
rowset q…l
order by sales.channel nulls first, sales.item.brand_id nulls first, sales.item.class_id nulls first, sales.item.category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | rowset qual_combo as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...s in all 3 channels 1999-2001 ??? rowset qual_combo as where sal...

  Write stats: received 1396 chars / 1396 bytes; tail: …'d nulls first, sales.item.category_id nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.date.quarter_name, count(ss.quantity) as qty_count where ss.date.year = 2001 group by ss.date.quarter_name order by ss.date.quarter_name limit 100;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...unt where ss.date.year = 2001 ??? group by ss.date.quarter_name
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.all_sales as all_sales;

# Frequent items: items whose count of …_sales.list_price) as total_sales
order by
    last_name asc nulls first,
    first_name asc nulls first,
    total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ..._sales.billing_customer.id in ??? (         select customer_stor...

  Write stats: received 1871 chars / 1871 bytes; tail: …'asc nulls first,\\n    total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query27.preql --content import raw.store_sales as store_sales;

-- Store sales in 2002 at stores in TN
-- Restricted to point-of-sale cust…ce) by rollup store_sales.item.id, store_sales.store.state as avg_unit_price
order by
  item_code asc nulls first,
  state asc nulls first
limit 100;`

  ```text
  refused to write 'query27.preql': not syntactically valid Trilogy.

  Parse error:
   --> 3:4
    |
  3 | -- Store sales in 2002 at stores in TN
    |    ^---
    |
    = expected PURPOSE, PROPERTY, UNIQUE, or AUTO
  Location:
  ...ore_sales as store_sales;  -- ??? Store sales in 2002 at stores

  Write stats: received 1039 chars / 1039 bytes; tail: …'tem_code asc nulls first,\\n  state asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query29.preql --content import raw.store_sales as ss;
import raw.physical_returns as pr;
import raw.catalog_sales as cs;

# Store sales in…
order by store_sales_filtered.item_code, store_sales_filtered.item_desc, store_sales_filtered.store_code, store_sales_filtered.store_name
limit 100;`

  ```text
  refused to write 'query29.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:1
    |
  8 | rowset store_sales_filtered as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ith sale year 1999/2000/2001  ??? rowset store_sales_filtered as...

  Write stats: received 2001 chars / 2001 bytes; tail: …'tered.store_code, store_sales_filtered.store_name\\nlimit 100;'.
  ```
- `trilogy file write query32.preql --content import raw/catalog_sales as cs;

# Per-item average extended discount amount for catalog sales in the date window
…item.manufacturer_id = 48
  and cs.discount_amount > 1.3 * item_avg_discount
select
    sum(cs.discount_amount) as total_extended_discount
limit 100;`

  ```text
  refused to write 'query32.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs;  # Pe...

  Write stats: received 412 chars / 412 bytes; tail: …'um(cs.discount_amount) as total_extended_discount\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs select count_distinct(ss.customer.last_name || '|' || ss.customer.first_name || '|' || …ast_name = cs.bill_customer.last_name and ss.customer.first_name = cs.bill_customer.first_name and ss.date.date = cs.sold_date.date)) as cnt limit 5;`

  ```text
  Syntax error in stdin:  --> 3:109
    |
  3 | select count_distinct(ss.customer.last_name || '|' || ss.customer.first_name || '|' || ss.date.date::string ? (ss.date.year = 2000 and cs.sold_date.year = 2000 and ss.customer.last_name = cs.bill_customer.last_name and ss.customer.first_name = cs.bill_customer.first_name and ss.date.date = cs.sold_date.date)) as cnt limit 5;
    |                                                                                                             ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...| '|' || ss.date.date::string ??? ? (ss.date.year = 2000 and cs....
  ```
- `trilogy file write query39.preql --content import raw.inventory as inv;
import raw.date as dt;

# For each (warehouse, item, month_of_year) in 2001, compute …n.m = 1
    and feb.m = 2
order by
    jan.wh_id,
    jan.item_id,
    jan.m,
    jan.mean_qty,
    jan.cv,
    feb.m,
    feb.mean_qty,
    feb.cv
;`

  ```text
  refused to write 'query39.preql': not syntactically valid Trilogy.

  Parse error:
    --> 42:5
     |
  42 |     on jan.wh_id = feb.wh_id
     |     ^---
     |
     = expected limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...red.jan = cv_filtered.feb     ??? on jan.wh_id = feb.wh_id     a...

  Write stats: received 1282 chars / 1282 bytes; tail: …'n_qty,\\n    jan.cv,\\n    feb.m,\\n    feb.mean_qty,\\n    feb.cv\\n;'.
  ```
- `trilogy file write query39.preql --content import raw.inventory as inv;
import raw.date as dt;

# Compute per (warehouse, item, month) stats for 2001
with mo…lls first,
    jan.mn nulls first,
    jan.sd / jan.mn nulls first,
    feb.mo nulls first,
    feb.mn nulls first,
    feb.sd / feb.mn nulls first
;`

  ```text
  refused to write 'query39.preql': not syntactically valid Trilogy.

  Parse error:
    --> 30:5
     |
  30 |     on jan.wh_id = feb.wh_id
     |     ^---
     |
     = expected limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...s.jan = monthly_stats.feb     ??? on jan.wh_id = feb.wh_id     a...

  Write stats: received 1031 chars / 1031 bytes; tail: …'t,\\n    feb.mn nulls first,\\n    feb.sd / feb.mn nulls first\\n;'.
  ```
- `trilogy file write query39.preql --content import raw.inventory as inv;
import raw.date as dt;

# Compute per (warehouse, item, month) stats for 2001
with mo…lls first,
    jan.mn nulls first,
    jan.sd / jan.mn nulls first,
    feb.mo nulls first,
    feb.mn nulls first,
    feb.sd / feb.mn nulls first
;`

  ```text
  refused to write 'query39.preql': not syntactically valid Trilogy.

  Parse error:
    --> 32:18
     |
  32 |     and jan.mo = 1
     |                  ^---
     |
     = expected IDENTIFIER
  Location:
   feb.item_id     and jan.mo = ??? 1     and feb.mo = 2 order by

  Write stats: received 1032 chars / 1032 bytes; tail: …'t,\\n    feb.mn nulls first,\\n    feb.sd / feb.mn nulls first\\n;'.
  ```
- `trilogy file write query44.preql --content import raw.store_sales as ss;
import raw.store as store;
import raw.item as item;

# Filter to store.id = 1
where …_performers.rnk = worst_performers.rnk
order by
    rank asc,
    best_performer_product_name desc,
    worst_performer_product_name desc
limit 100;
`

  ```text
  refused to write 'query44.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...profit per item at this store ??? auto item_avg_profit <- avg(ss...

  Write stats: received 2367 chars / 2367 bytes; tail: …'name desc,\\n    worst_performer_product_name desc\\nlimit 100;\\n'.
  ```
- `trilogy file write query50.preql --content import raw.store_sales as store_sales;
import raw.physical_returns as physical_returns;

# Join store_sales and ph…sales.store.suite_number,
    store_sales.store.city,
    store_sales.store.county,
    store_sales.store.state,
    store_sales.store.zip
limit 100;`

  ```text
  refused to write 'query50.preql': not syntactically valid Trilogy.

  Parse error:
    --> 26:1
     |
  26 | inner join store_sales.ticket_number = physical_returns.ticket_number
     | ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...return_date.month_of_year = 8 ??? inner join store_sales.ticket_...

  Write stats: received 1921 chars / 1921 bytes; tail: …'tore_sales.store.state,\\n    store_sales.store.zip\\nlimit 100;'.
  ```
- `trilogy run --import raw/store_sales:store_sales select store_sales.date.year, min(store_sales.date.week_seq) as min_ws, max(store_sales.date.week_seq) as max_ws where store_sales.date.year in (2001,2002) group by store_sales.date.year order by store_sales.date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ales.date.year in (2001,2002) ??? group by store_sales.date.year...
  ```
- `trilogy file write query59.preql --content import raw.store_sales as ss;

# Sum of unit prices per (store, week, day_of_week)
auto sun_total <- sum(ss.sales_…year.week_seq - 52
order by this_year.store_name asc nulls first, this_year.store_code asc nulls first, this_year.week_seq asc nulls first
limit 100;`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
    --> 59:52
     |
  59 | inner join this_year.week_seq = next_year.week_seq - 52
     |                                                    ^---
     |
     = expected limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...week_seq = next_year.week_seq ??? - 52 order by this_year.store_...

  Write stats: received 2810 chars / 2810 bytes; tail: …'c nulls first, this_year.week_seq asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/store_sales:ss select ss.store.name, ss.store.text_id, ss.date.week_seq, ss.date.day_name, sum(ss.sales_price) as total where ss.dat…ore.name, ss.store.text_id, ss.date.week_seq, ss.date.day_name order by ss.store.name, ss.store.text_id, ss.date.week_seq, ss.date.day_name limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...tal where ss.date.year = 2001 ??? group by ss.store.name, ss.sto...
  ```
- `trilogy file write query63.preql --content import raw.store_sales as sales;

# Per-manager, per-month total of unit prices for qualifying items in 2000
auto …_id,
    mgr_month_total as monthly_total,
    mgr_avg as avg_monthly_sales
order by item.manager_id asc, mgr_avg asc, mgr_month_total asc
limit 100;`

  ```text
  refused to write 'query63.preql': not syntactically valid Trilogy.

  Parse error:
    --> 26:1
     |
  26 | select
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...l - mgr_avg) / mgr_avg) > 0.1 ??? select     item.manager_id,

  Write stats: received 1214 chars / 1214 bytes; tail: …'.manager_id asc, mgr_avg asc, mgr_month_total asc\\nlimit 100;'.
  ```
- `trilogy run --import raw/all_sales.preql:s select s.warehouse.id as wh_id, s.warehouse.name as w_name, s.date.month_of_year as month, sum(s.quantity * s.ext_…DHL', 'BARIAN') and s.warehouse.id is not null and s.warehouse.id = 5 group by s.warehouse.id, s.warehouse.name, s.date.month_of_year order by month;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...t null and s.warehouse.id = 5 ??? group by s.warehouse.id, s.war...
  ```
- `trilogy file write test_left4.preql --content import raw.all_sales as s;
import raw.date as d;

rowset all_months <-
where d.year = 2001
select
    d.month_o…
    all_months.month
inner join all_months.join_key = 1
left join sales_agg.wh_id = sales_agg.wh_id and all_months.month = sales_agg.month
limit 30;`

  ```text
  refused to write 'test_left4.preql': not syntactically valid Trilogy.

  Parse error:
    --> 32:34
     |
  32 | inner join all_months.join_key = 1
     |                                  ^---
     |
     = expected IDENTIFIER
  Location:
  ...er join all_months.join_key = ??? 1 left join sales_agg.wh_id =

  Write stats: received 986 chars / 986 bytes; tail: …'s_agg.wh_id and all_months.month = sales_agg.month\\nlimit 30;'.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as store_sales;

# Let's check what states and data we have
where store_sales.date.year = 2000
select
    store_sales.store.state,
    sum(store_sales.net_profit) as total_profit
order by total_profit desc;
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:1
    |
  9 | limit 100;
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...t order by total_profit desc; ??? limit 100;

  Write stats: received 240 chars / 240 bytes; tail: …'ofit) as total_profit\\norder by total_profit desc;\\nlimit 100;'.
  ```
- `trilogy file write query73.preql`

  ```text
  refused to write 'query73.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:109
     |
  20 |   and s.customer.household_demographic.dependent_count / cast(s.customer.household_demographic.vehicle_count, float) > 1.0
     |                                                                                                             ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...hold_demographic.vehicle_count ??? , float) > 1.0
   select
       s....

  Write stats: received 1314 chars / 1314 bytes; tail: …'c,\r\\n    s.ticket_number asc,\r\\n    s.customer.first_name asc;'.
  ```
- `trilogy run --import raw.all_sales:sales select sales.date.year, sales.item.category, count(sales.order_id) as cnt group by 1,2 order by 1,2 limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   count(sales.order_id) as cnt ??? group by 1,2 order by 1,2 limi...
  ```
- `trilogy file write query75.preql -e -c import raw.all_sales as sales;

# Deduplicate per-line records before aggregating: each (year, item, channel, order) c…ed.total_amt as amt_diff
having
    grouped.total_qty / (2001_grouped.total_qty * 1.0) < 0.9
order by
    qty_diff asc,
    amt_diff asc
limit 100
;
`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
    --> 31:25
     |
  31 | inner join grouped.yr = 2001
     |                         ^---
     |
     = expected IDENTIFIER
  Location:
   2002 inner join grouped.yr = ??? 2001    and grouped.brand_id =...

  Write stats: received 1763 chars / 1763 bytes; tail: …'0.9\\norder by\\n    qty_diff asc,\\n    amt_diff asc\\nlimit 100\\n;\\n'.
  ```
- `trilogy run --import raw.all_sales:sales where sales.date.year in (2001,2002) and sales.item.category = 'Books' select sales.date.year, count(distinct sales.item.id) as cnt_items, count(sales.order_id) as cnt_orders, count(*) as cnt_all order by 1;`

  ```text
  Syntax error in stdin:  --> 2:183
    |
  2 | where sales.date.year in (2001,2002) and sales.item.category = 'Books' select sales.date.year, count(distinct sales.item.id) as cnt_items, count(sales.order_id) as cnt_orders, count(*) as cnt_all order by 1;
    |                                                                                                                                                                                       ^---
    |
    = expected access_chain
  Location:
  ...rder_id) as cnt_orders, count( ??? *) as cnt_all order by 1;
  ```
- `trilogy file write query75_check4.preql -e -c import raw.all_sales as sales;

rowset deduped <- select
    sales.date.year as yr,
    sales.item.brand_id as …ed.yr, deduped.brand_id, deduped.class_id, deduped.cat_id, deduped.mfr_id
;

select
    aggregated.yr as yr,
    count(*) as cnt_combos
order by yr
;`

  ```text
  refused to write 'query75_check4.preql': not syntactically valid Trilogy.

  Parse error:
    --> 29:11
     |
  29 |     count(*) as cnt_combos
     |           ^---
     |
     = expected access_chain
  Location:
  ...ggregated.yr as yr,     count( ??? *) as cnt_combos order by yr ;...

  Write stats: received 905 chars / 905 bytes; tail: …'ggregated.yr as yr,\\n    count(*) as cnt_combos\\norder by yr\\n;'.
  ```
- `trilogy file write query76.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sale…ls first,
  combined.missing_ref nulls first,
  combined.year nulls first,
  combined.quarter nulls first,
  combined.category nulls first
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
    --> 47:1
     |
  47 | )
     | ^---
     |
     = expected concept_nullable_modifier or metadata
  Location:
  ...total_ext_sales_price numeric ??? )  select   combined.channel,

  Write stats: received 1753 chars / 1753 bytes; tail: …'rter nulls first,\\n  combined.category nulls first\\nlimit 100;'.
  ```
- `trilogy file write query76.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sale…ls first,
  combined.missing_ref nulls first,
  combined.year nulls first,
  combined.quarter nulls first,
  combined.category nulls first
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
    --> 39:87
     |
  39 | ) -> (channel, missing_ref, year, quarter, category, line_count, total_ext_sales_price)
     |                                                                                       ^---
     |
     = expected metadata, PURPOSE, or data_type
  Location:
  ...e_count, total_ext_sales_price ??? )  select   combined.channel,

  Write stats: received 1696 chars / 1696 bytes; tail: …'rter nulls first,\\n  combined.category nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.customer.id as cid, count(ss.quantity) as cnt where ss.date.year=2000 and ss.customer.id is not null and ss.is_returned=false group by cid order by cnt desc limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...null and ss.is_returned=false ??? group by cid order by cnt desc...
  ```
- `trilogy run --import raw.all_sales:all select all.channel, count(all.quantity) as cnt where all.date.year=2000 and all.billing_customer.id is not null and all.is_returned is null group by all.channel;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...l and all.is_returned is null ??? group by all.channel;
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

# Channel label mapping
auto channel_label <- case
    when all_sales.channel …_profit) - coalesce(sum(all_sales.return_net_loss), 0) as profit
order by
    channel_label nulls first,
    outlet_identifier nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
    --> 18:54
     |
  18 | def rollup_sales(metric) -> coalesce(sum(metric), 0) by rollup channel_label, outlet_identifier;
     |                                                      ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...) -> coalesce(sum(metric), 0) ??? by rollup channel_label, outle...

  Write stats: received 1275 chars / 1275 bytes; tail: …'el nulls first,\\n    outlet_identifier nulls first\\nlimit 100;'.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

# Channel label based on channel enum
auto channel_label <- case
    when all_…_profit) - coalesce(sum(all_sales.return_net_loss), 0) as profit
order by
    channel_label nulls first,
    outlet_identifier nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
    --> 19:5
     |
  19 |     by rollup channel_label, outlet_identifier;
     |     ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   coalesce(sum(metric), 0)     ??? by rollup channel_label, outle...

  Write stats: received 1387 chars / 1387 bytes; tail: …'el nulls first,\\n    outlet_identifier nulls first\\nlimit 100;'.
  ```
- `trilogy file write query81.preql --content import raw/catalog_returns as cr;

# Total return amount including tax per (returning customer, return_address sta…eet_number, street_name, street_type, suite_number,
    city, county, state, zip, country, gmt_offset, location_type,
    cust_state_total
limit 100;`

  ```text
  refused to write 'query81.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_returns as cr;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_returns as cr;  #

  Write stats: received 1592 chars / 1592 bytes; tail: …', gmt_offset, location_type,\\n    cust_state_total\\nlimit 100;'.
  ```
- `trilogy file write query82.preql --content import raw.store_sales as ss;
import raw.inventory as inv;

# Items that appear in store sales, have matching inve…nd 500
select
  ss.item.text_id as item_code,
  ss.item.desc as description,
  ss.item.current_price as current_price
order by
  item_code
limit 100;`

  ```text
  refused to write 'query82.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities — to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
   matching inventory snapshots ??? inner join inv.item.id = ss.it...

  Write stats: received 527 chars / 527 bytes; tail: …'rrent_price as current_price\\norder by\\n  item_code\\nlimit 100;'.
  ```
- `trilogy run --import raw.date:date select date.week_seq, min(date.date) as week_start, max(date.date) as week_end where date.week_seq in (5244, 5257, 5264) group by date.week_seq order by date.week_seq;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...eek_seq in (5244, 5257, 5264) ??? group by date.week_seq order b...
  ```
- `trilogy file write query83.preql --content import raw.store_sales as store;
import raw.catalog_returns as catalog;
import raw.web_returns as web;
import raw.…_code
inner join store_ret.item_code = web_ret.item_code
order by store_ret.item_code asc nulls first, store_ret.store_qty asc nulls first
limit 100;`

  ```text
  refused to write 'query83.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:11
     |
  22 |     count(*) as store_null_rows
     |           ^---
     |
     = expected access_chain
  Location:
  ...xt_id as item_code,     count( ??? *) as store_null_rows ;  # Cat...

  Write stats: received 2486 chars / 2486 bytes; tail: …' nulls first, store_ret.store_qty asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.web_returns:web select web.web_sales.item.text_id, sum(web.return_quantity) as total_qty, count(web.return_quantity) as qty_rows from web where web.return_date.week_seq in (5244,5257,5264) and web.web_sales.item.text_id = 'AAAAAAAAAHFBAAAA';`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ....return_quantity) as qty_rows ??? from web where web.return_date...
  ```
- `trilogy run --import raw.web_sales:web_sales select web_sales.item.category, web_sales.item.class, sum(web_sales.net_paid) as total_net_paid where web_sales.date.year = 2000 group by 1,2 limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...re web_sales.date.year = 2000 ??? group by 1,2 limit 5;
  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Rowset: distinct (las…  and store_tuples.first_name not in web_tuples.first_name
  and store_tuples.sale_date not in web_tuples.sale_date
select count(*) as unique_combos;`

  ```text
  refused to write 'query87.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:1
    |
  6 | rowset store_tuples as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ate) from store sales in 2000 ??? rowset store_tuples as where s...

  Write stats: received 1302 chars / 1302 bytes; tail: …'ot in web_tuples.sale_date\\nselect count(*) as unique_combos;'.
  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Rowset: distinct (las…  and store_tuples.first_name not in web_tuples.first_name
  and store_tuples.sale_date not in web_tuples.sale_date
select count(*) as unique_combos;`

  ```text
  refused to write 'query87.preql': not syntactically valid Trilogy.

  Parse error:
    --> 39:14
     |
  39 | select count(*) as unique_combos;
     |              ^---
     |
     = expected access_chain
  Location:
  ...tuples.sale_date select count( ??? *) as unique_combos;

  Write stats: received 1302 chars / 1302 bytes; tail: …'ot in web_tuples.sale_date\\nselect count(*) as unique_combos;'.
  ```
- `trilogy file write query95.preql --content import raw.web_sales as ws;

# Stage: find orders meeting multi-warehouse and return-existence criteria
rowset qua…rder_count,
    sum(ws.ext_ship_cost) as total_extended_shipping_cost,
    sum(ws.net_profit) as total_net_profit
order by order_count asc
limit 100;`

  ```text
  refused to write 'query95.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | rowset qualifying_orders as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...and return-existence criteria ??? rowset qualifying_orders as wh...

  Write stats: received 926 chars / 926 bytes; tail: …'fit) as total_net_profit\\norder by order_count asc\\nlimit 100;'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.all_sales:all_sales select distinct sales.channel, count(*) from sales group by 1;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct sales.channel as distinct_sales_channel`
  Location:
  ...as all_sales; select distinct ??? sales.channel, count(*) from s...
  ```
- `trilogy run --import raw.date:date select distinct date.quarter_name, date.year, date.quarter limit 100;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct date.quarter_name as distinct_date_quarter_name`
  Location:
  ...date as date; select distinct ??? date.quarter_name, date.year,
  ```
- `trilogy run --import raw.catalog_store_returns:csr select csr.store_sale_date.quarter_name, csr.store_return_date.quarter_name, csr.catalog_date.quarter_name, count(csr.store_quantity) where csr.store_sale_date.year = 2001 limit 100;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(csr.store_quantity) as store_quantity_count`
  Location:
  ...me, count(csr.store_quantity) ??? where csr.store_sale_date.year...
  ```
- `trilogy file write query40.preql --content import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Filter conditions
where cs.item.current_price…eft join cs.order_number = cr.order_number
    left join cs.item.id = cr.sales.item.id
order by
    warehouse_state asc,
    item_code asc
limit 100;`

  ```text
  refused to write 'query40.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `Period 1: sales before 2000-03-11
      sum(
          (cs.sales_price - coalesce(cr.refunded_cash, 0)) ? cs.sold_date.date < '2000-03-11'::date
      ) as period_1_sales_before_2000_03_11_sum_cs_`
  Location:
  ...d as item_code,     -- Period ??? 1: sales before 2000-03-11

  Write stats: received 854 chars / 854 bytes; tail: …'der by\\n    warehouse_state asc,\\n    item_code asc\\nlimit 100;'.
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Items matching any of the 8 attribute profiles
# Profiles: (category, color, units, si…id between 1 and 500
    and item.manufact in shared_manufs.manufact
select distinct
    item.product_name
order by
    item.product_name
limit 100;
`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct
      item.product_name as distinct_item_product_name`
  Location:
  ....manufact select distinct     ??? item.product_name order by

  Write stats: received 1562 chars / 1562 bytes; tail: …'item.product_name\\norder by\\n    item.product_name\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs --import raw.web_sales:ws select count(ss.quantity) where ss.date.year=2000 and ss.customer.id is not null and ss.is_returned=false;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(ss.quantity) as quantity_count`
  Location:
  ...ws; select count(ss.quantity) ??? where ss.date.year=2000 and ss...
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

# Channel label based on channel enum
auto channel_label <- case
    when all_…t_loss), 0) by rollup channel_label, outlet_identifier as profit
order by
    channel_label nulls first,
    outlet_identifier nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `coalesce(sum(all_sales.ext_sales_price), 0) by rollup channel_label as coalesce_sum_all_sales_ext_sales_price_0`
  Location:
  ...ll_sales.ext_sales_price), 0) ??? by rollup channel_label, outle...

  Write stats: received 1359 chars / 1359 bytes; tail: …'el nulls first,\\n    outlet_identifier nulls first\\nlimit 100;'.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

# Channel label based on channel enum
auto channel_label <- case
    when all_…s), 0) by rollup channel_label, outlet_identifier as return_loss
order by
    channel_label nulls first,
    outlet_identifier nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `coalesce(sum(all_sales.ext_sales_price), 0) by rollup channel_label as coalesce_sum_all_sales_ext_sales_price_0`
  Location:
  ...ll_sales.ext_sales_price), 0) ??? by rollup channel_label, outle...

  Write stats: received 1434 chars / 1434 bytes; tail: …'el nulls first,\\n    outlet_identifier nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/catalog_returns:cr select distinct cr.billing_customer.household_demographic.buy_potential;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct cr.billing_customer.household_demographic.buy_potential as distinct_cr_billing_customer_household_d`
  Location:
  ...eturns as cr; select distinct ??? cr.billing_customer.household_...
  ```

### `cli-misuse`

- `trilogy explore raw/store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_returns.preql' does not exist.
  ```
- `trilogy run query04.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy run query14.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy run query94.preql --format rich`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```

### `undefined-concept`

- `trilogy explore raw/../query04.preql`

  ```text
  Failed to parse raw\..\query04.preql: (UndefinedConceptException(...), "Undefined concept: ann_val.channel. Suggestions: ['ann_val.s.channel', 's.channel', 'ann_val.val', 'ann_val.fname']")
  ```
- `trilogy explore query39.preql --show concepts`

  ```text
  Failed to parse query39.preql: (UndefinedConceptException(...), "Undefined concept: cv_filtered.m. Suggestions: ['cv_filtered.monthly_stats.m', 'monthly_stats.m', 'cv_filtered.cv', '_cv_filtered_cv']")
  ```
- `trilogy explore query39_explore2.preql`

  ```text
  Failed to parse query39_explore2.preql: (UndefinedConceptException(...), "Undefined concept: jan.wh_id (line 27, col 8, in SELECT). Suggestions: ['jan.monthly_stats.wh_id', 'monthly_stats.wh_id']")
  ```
- `trilogy explore query44.preql`

  ```text
  Failed to parse query44.preql: (UndefinedConceptException(...), "Undefined concept: item_profits.id. Suggestions: ['item_profits.item.id', 'ss.item.id', 'ss.date.id', 'ss.return_date.id', 'ss.time.id', 'ss.return_time.id']")
  ```
- `trilogy explore query51.preql`

  ```text
  Failed to parse query51.preql: (UndefinedConceptException(...), '2 undefined concept references; fix all before re-running:\n  - combined.web_daily_amt (line 34, in SELECT); did you mean: combined.web_daily.web_daily_amt, web_daily.web_daily_amt, combined.store_daily_amt?\n  - combined.store_daily_amt (line 34, in SELECT); did you mean: combined.store_daily.store_daily_amt, store_daily.store_daily_amt, combined.web_daily_amt?')
  ```
- `trilogy explore query63.preql`

  ```text
  Failed to parse query63.preql: (UndefinedConceptException(...), '2 undefined concept references; fix all before re-running:\n  - mgr_month.manager_id (line 25, col 5, in SELECT); did you mean: mgr_month.sales.item.manager_id, sales.item.manager_id, mgr_month.monthly_total?\n  - mgr_month.manager_id (line 30, col 10, in ORDER BY); did you mean: mgr_month.sales.item.manager_id, sales.item.manager_id, mgr_month.monthly_total?')
  ```
- `trilogy explore query94.preql`

  ```text
  Failed to parse query94.preql: (UndefinedConceptException(...), "Undefined concept: qualified_orders.order_number (line 25, col 35, in WHERE). Suggestions: ['qualified_orders.web_sales.order_number', 'web_sales.order_number', 'qualified_orders.has_return']")
  ```

### `file-not-found`

- `trilogy run query04.preql`

  ```text
  Unexpected error in query04.preql: Join key `store01.cust_id` does not exist
  ```
- `trilogy explore raw/../query04.preql`

  ```text
  Failed to parse raw\..\query04.preql: Join key `store01.cust_id` does not exist
  ```
- `trilogy run query39.preql`

  ```text
  Unexpected error in query39.preql: Join key `jan.wh_id` does not exist
  ```
- `trilogy explore query39.preql --show concepts`

  ```text
  Failed to parse query39.preql: Join key `jan.wh_id` does not exist
  ```
- `trilogy run query39.preql`

  ```text
  Unexpected error in query39.preql: Join key `jan.wh_id` does not exist
  ```
- `trilogy run test_left.preql`

  ```text
  Unexpected error in test_left.preql: Join key `wm.wh_id` does not exist
  ```
- `trilogy run query78.preql`

  ```text
  Unexpected error in query78.preql: Join key `store_cat.item_id` does not exist
  ```

### `join-resolution`

- `trilogy run --import raw/store_sales:ss WITH distinct_items AS SELECT ss.item.category as cat, ss.item.id as item_id, ss.item.current_price as price;
WITH ca…ddress.state,
    ss.item.category,
    ss.item.current_price,
    cat_avg.avg_price
WHERE ss.date.year = 2001 AND ss.date.month_of_year = 1
LIMIT 5;`

  ```text
  Resolution error in stdin: Could not resolve connections for query with output ['ss.customer.address.state<Purpose.PROPERTY>Derivation.ROOT>', 'ss.item.category<Purpose.PROPERTY>Derivation.ROOT>', 'ss.item.current_price<Purpose.PROPERTY>Derivation.ROOT>', 'cat_avg.avg_price<Purpose.METRIC>Derivation.ROWSET>'] from current model.
  ```
- `trilogy run query21.preql`

  ```text
  Resolution error in query21.preql: Could not resolve connections for query with output ['local._after_win_warehouse_name<Purpose.PROPERTY>Derivation.BASIC>', 'local._after_win_item_code<Purpose.KEY>Derivation.BASIC>', 'local._after_win_after_total<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Could not resolve connections for query with output ['item.id<Purpose.KEY>Derivation.ROOT>', 'item.product_name<Purpose.PROPERTY>Derivation.ROOT>', 'local.item_avg_profit<Purpose.METRIC>Derivation.AGGREGATE>', 'local._best_ranked_rnk<Purpose.PROPERTY>Derivation.WINDOW>'] from current model.
  ```
- `trilogy run query47.preql`

  ```text
  Resolution error in query47.preql: Could not resolve connections for query with output ['item.category<Purpose.PROPERTY>Derivation.ROOT>', 'item.brand_name<Purpose.PROPERTY>Derivation.ROOT>', 'store.name<Purpose.PROPERTY>Derivation.ROOT>', 'store.company_name<Purpose.PROPERTY>Derivation.ROOT>', 'date.year<Purpose.PROPERTY>Derivation.ROOT>', 'date.month_of_year<Purpose.PROPERTY>Derivation.ROOT>', 'local.avg_sales<Purpose.METRIC>Derivation.BASIC>', 'local.month_total<Purpose.METRIC>Derivation.BASIC>', 'local.prior_month_total<Purpose.PROPERTY>Derivation.WINDOW>', 'local.next_month_total<Purpose.PROPERTY>Derivation.WINDOW>'] from current model.
  ```
- `trilogy run query47.preql`

  ```text
  Resolution error in query47.preql: Could not resolve connections for query with output ['item.category<Purpose.PROPERTY>Derivation.ROOT>', 'item.brand_name<Purpose.PROPERTY>Derivation.ROOT>', 'store.name<Purpose.PROPERTY>Derivation.ROOT>', 'store.company_name<Purpose.PROPERTY>Derivation.ROOT>', 'date.year<Purpose.PROPERTY>Derivation.ROOT>', 'date.month_of_year<Purpose.PROPERTY>Derivation.ROOT>', 'local.avg_sales<Purpose.METRIC>Derivation.BASIC>', 'local.month_total<Purpose.METRIC>Derivation.BASIC>', 'local.prior_month_total<Purpose.PROPERTY>Derivation.WINDOW>', 'local.next_month_total<Purpose.PROPERTY>Derivation.WINDOW>'] from current model.
  ```
- `trilogy run query47.preql`

  ```text
  Resolution error in query47.preql: Could not resolve connections for query with output ['item.category<Purpose.PROPERTY>Derivation.ROOT>', 'item.brand_name<Purpose.PROPERTY>Derivation.ROOT>', 'store.name<Purpose.PROPERTY>Derivation.ROOT>', 'store.company_name<Purpose.PROPERTY>Derivation.ROOT>', 'date.year<Purpose.PROPERTY>Derivation.ROOT>', 'date.month_of_year<Purpose.PROPERTY>Derivation.ROOT>', 'local._monthly_data_month_total<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```

### `type-error`

- `trilogy run query97_check4.preql`

  ```text
  Unexpected error in query97_check4.preql: Invalid argument type 'INTEGER' passed into CONCAT function in position 1 from concept: store_sales.customer.id. Valid: 'STRING'.
  ```
