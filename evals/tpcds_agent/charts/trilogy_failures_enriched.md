# Trilogy failure analysis — 20260629-164947

- Run `20260629-164947` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1302 | failed: 133 (10%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 87 | 65% |
| `syntax-parse` | 36 | 27% |
| `cli-misuse` | 6 | 5% |
| `syntax-missing-alias` | 4 | 3% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  Resolution error in query02.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 42). The requested concepts split into 8 disconnected subgraphs: {lagged.agg_sales.total_sales, lagged.next_year_sales, weeks_in_2001.week_seq}; {local._virt_filter_total_sales_4006337828950318}; {local._virt_filter_total_sales_6084061215480102}; {local._virt_filter_total_sales_7302728106892369}; {local._virt_filter_total_sales_7792942807412026}; {local._virt_filter_total_sales_7953324662356686}; {local._virt_filter_total_sales_9709289527696109}; {local._virt_filter_total_sales_9989415342883202}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query02.preql`

  ```text
  Resolution error in query02.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 49). The requested concepts split into 8 disconnected subgraphs: {lagged.agg.day_sales, lagged.next_yr_sales, weeks_in_2001.ws}; {local._virt_filter_day_sales_1067907726933205}; {local._virt_filter_day_sales_1069561135224164}; {local._virt_filter_day_sales_1559847466727042}; {local._virt_filter_day_sales_3062834168341722}; {local._virt_filter_day_sales_3104972574530403}; {local._virt_filter_day_sales_4349778085645256}; {local._virt_filter_day_sales_4374998356623214}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/all_sales:sales select sales.billing_customer.text_id as cid, sales.channel, sales.date.year, sum(sales.ext_list_price - sales.ext_w…as annual_value where sales.date.year in (2001,2002) and sales.channel in ('STORE','CATALOG','WEB') order by sales.billing_customer.text_id limit 20;`

  ```text
  Syntax error in stdin: Comparison `sales.channel in ('STORE', 'CATALOG', 'WEB')` matches every value of enum field 'sales.channel', which contains only these values: 'WEB', 'CATALOG', 'STORE'. It is always true and should be removed.
  ```
- `trilogy file read query04.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/all_sales:all_sales --import raw/store:store select all_sales.return_channel_dim_text_id, store.text_id where all_sales.channel = 'CATALOG' and all_sales.return_channel_dim_text_id = store.text_id limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {all_sales.channel, all_sales.return_channel_dim_text_id}; {store.text_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(combined.ch)` (line 77), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.ch) as g order by g desc`.
  ```
- `trilogy file read query06.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Syntax error in query08.preql: Undefined concept: customer.preferred_cust_flag.
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: Undefined concept: sr.cust_id. Suggestions: ['store_rev.cust_id', 'web_rev.cust_id', 'ss.customer.id']
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(s.channel)` (line 18), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(s.channel) as g order by g desc`.
  ```
- `trilogy run query15.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260629-164947\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 98356)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query15.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260629-164947\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 98356)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query15.preql`

  ```text
  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy run query15.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260629-164947\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 98356)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy file read query15.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query15.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260629-164947\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 98356)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query15.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260629-164947\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 98356)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query18.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260629-164947\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 98356)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query18.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260629-164947\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 98356)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query18.preql`

  ```text
  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query18.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260629-164947\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 98356)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy file read query18.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query18.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260629-164947\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 98356)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query20.preql`

  ```text
  Resolution error in query20.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {cs.sold_date.date, local.pct_of_class_total, local.total_ext_sales_price}; {item.category, local.category, local.class, local.current_price, local.description, local.item_code}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query23.preql`

  ```text
  Resolution error in query23.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 36). The requested concepts split into 3 disconnected subgraphs: {catalog_sales.bill_customer.id}; {item_date_pairs.fd}; {local.is_frequent_desc}. Are you missing a join or merge statement to relate them? Here `local.is_frequent_desc`, `catalog_sales.bill_customer.id` is referenced only inside a filter on rowset output `item_date_pairs.fd` (rowset `item_date_pairs`), and isn't related to it without a join. Relate them by adding `local.is_frequent_desc`, `catalog_sales.bill_customer.id` to rowset `item_date_pairs`'s select, by an existence comparison against a base-concept set (e.g. `item_date_pairs.fd in (<base concept> ? <condition>)`), or by joining the rowset back to the source.
  ```
- `trilogy file read query23.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query26.preql`

  ```text
  Syntax error in query26.preql: Comparison `catalog_sales.bill_customer_demographic.gender = 'Male'` can never match enum field 'catalog_sales.bill_customer_demographic.gender', which contains only these values: 'M', 'F'. It is always false and should be removed.
  ```
- `trilogy run query29.preql`

  ```text
  Syntax error in query29.preql: 4 undefined concept references; fix all before re-running:
    - local.item_code (line 56, col 10, in ORDER BY); did you mean: ss_agg.item_code, item_desc, store_code?
    - local.item_desc (line 56, col 21, in ORDER BY); did you mean: ss_agg.item_desc, item_code, ss.item.desc, sr.item.desc?
    - local.store_code (line 56, col 32, in ORDER BY); did you mean: ss_agg.store_code, item_code, _ss_agg_store_code?
    - local.store_name (line 56, col 44, in ORDER BY); did you mean: ss_agg.store_name, ss.store.name, sr.store.name?
  ```
- `trilogy run query29.preql`

  ```text
  Unexpected error in query29.preql: Join chain repeats source `ss` (keys `ss.customer.id` and `ss.item.id`). A `=` chain joins ONE key across distinct sources; join a composite key with `and` or separate clauses (e.g. `a.k1 = b.k1 and a.k2 = b.k2`).
  ```
- `trilogy file read query40.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query41.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text
  Unexpected error in query44.preql: (_duckdb.BinderException) Binder Error: column "SS_ITEM_SK" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(SS_ITEM_SK)" if the exact value of "SS_ITEM_SK" is not important.

  LINE 11: ...hold" and "store_sales_store_sales"."SS_STORE_SK" = 1 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END as...
                                                                         ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      avg(CASE WHEN "store_sales_store_sales"."SS_STORE_SK" = 1 and "store_sales_store_sales"."SS_ADDR_SK" is null THEN "store_sales_store_sales"."SS_NET_PROFIT" ELSE NULL END) * 0.9 as "threshold"
  FROM
      "store_sales" as "store_sales_store_sales"),
  questionable as (
  SELECT
      "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id",
      CASE WHEN avg("store_sales_store_sales"."SS_NET_PROFIT") > "thoughtful"."threshold" and "store_sales_store_sales"."SS_STORE_SK" = 1 THEN "store_sales_store_sales"."SS_ITEM_SK" ELSE NULL END as "_virt_filter_id_4923065367559473"
  FROM
      "thoughtful"
      LEFT OUTER JOIN "store_sales" as "store_sales_store_sales" on 1=1
  GROUP BY
      1,
      "thoughtful"."threshold"
  HAVING
      avg("store_sales_store_sales"."SS_NET_PROFIT") > "thoughtful"."threshold"
  ),
  uneven as (
  SELECT
      "questionable"."_virt_filter_id_4923065367559473" as "_virt_filter_id_4923065367559473"
  FROM
      "questionable"
  WHERE
      "questionable"."store_sales_store_id" = 1
  ),
  highfalutin as (
  SELECT
      "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id",
      avg("store_sales_store_sales"."SS_NET_PROFIT") as "_qualifying_items_avg_net_profit"
  FROM
      "store_sales" as "store_sales_store_sales"
  WHERE
      "store_sales_store_sales"."SS_STORE_SK" = 1 and "store_sales_store_sales"."SS_ITEM_SK" in (select uneven."_virt_filter_id_4923065367559473" from uneven where uneven."_virt_filter_id_4923065367559473" is not null)

  GROUP BY
      1),
  yummy as (
  SELECT
      "highfalutin"."_qualifying_items_avg_net_profit" as "_qualifying_items_avg_net_profit",
      "store_sales_item_items"."I_ITEM_SK" as "_qualifying_items_item_id",
      "store_sales_item_items"."I_PRODUCT_NAME" as "_qualifying_items_product_name"
  FROM
      "highfalutin"
      INNER JOIN "item" as "store_sales_item_items" on "highfalutin"."store_sales_item_id" = "store_sales_item_items"."I_ITEM_SK"
  WHERE
      "store_sales_item_items"."I_ITEM_SK" in (select uneven."_virt_filter_id_4923065367559473" from uneven where uneven."_virt_filter_id_4923065367559473" is not null)
  ),
  juicy as (
  SELECT
      "yummy"."_qualifying_items_avg_net_profit" as "qualifying_items_avg_net_profit",
      "yummy"."_qualifying_items_item_id" as "qualifying_items_item_id",
      "yummy"."_qualifying_items_product_name" as "qualifying_items_product_name"
  FROM
      "yummy"),
  young as (
  SELECT
      "juicy"."qualifying_items_avg_net_profit" as "qualifying_items_avg_net_profit",
      "juicy"."qualifying_items_item_id" as "qualifying_items_item_id",
      "juicy"."qualifying_items_product_name" as "qualifying_items_product_name"
  FROM
      "juicy"
  GROUP BY
      1,
      2,
      3),
  vacuous as (
  SELECT
      "juicy"."qualifying_items_avg_net_profit" as "qualifying_items_avg_net_profit",
      "juicy"."qualifying_items_item_id" as "qualifying_items_item_id"
  FROM
      "juicy"
  GROUP BY
      1,
      2),
  concerned as (
  SELECT
      "vacuous"."qualifying_items_avg_net_profit" as "qualifying_items_avg_net_profit",
      "vacuous"."qualifying_items_item_id" as "qualifying_items_item_id",
      rank() over (order by "vacuous"."qualifying_items_avg_net_profit" asc ) as "_best_performers_rnk",
      rank() over (order by "vacuous"."qualifying_items_avg_net_profit" desc ) as "_worst_performers_rnk"
  FROM
      "vacuous"),
  sweltering as (
  SELECT
      "concerned"."_best_performers_rnk" as "_best_performers_rnk",
      "young"."qualifying_items_product_name" as "qualifying_items_product_name"
  FROM
      "young"
      INNER JOIN "concerned" on "young"."qualifying_items_avg_net_profit" = "concerned"."qualifying_items_avg_net_profit" AND "young"."qualifying_items_item_id" = "concerned"."qualifying_items_item_id"
  WHERE
      "concerned"."_best_performers_rnk" <= 10

  ORDER BY
      "concerned"."_best_performers_rnk" asc),
  sparkling as (
  SELECT
      "concerned"."_worst_performers_rnk" as "_worst_performers_rnk",
      "young"."qualifying_items_product_name" as "qualifying_items_product_name"
  FROM
      "young"
      INNER JOIN "concerned" on "young"."qualifying_items_avg_net_profit" = "concerned"."qualifying_items_avg_net_profit" AND "young"."qualifying_items_item_id" = "concerned"."qualifying_items_item_id"
  WHERE
      "concerned"."_worst_performers_rnk" <= 10

  ORDER BY
      "concerned"."_worst_performers_rnk" asc),
  late as (
  SELECT
      "sweltering"."_best_performers_rnk" as "best_performers_rnk",
      "sweltering"."qualifying_items_product_name" as "best_performers_qualifying_items_product_name"
  FROM
      "sweltering"),
  abhorrent as (
  SELECT
      "sparkling"."_worst_performers_rnk" as "worst_performers_rnk",
      "sparkling"."qualifying_items_product_name" as "worst_performers_qualifying_items_product_name"
  FROM
      "sparkling")
  SELECT
      "late"."best_performers_rnk" as "rank",
      "late"."best_performers_qualifying_items_product_name" as "best_performer_product_name",
      "abhorrent"."worst_performers_qualifying_items_product_name" as "worst_performer_product_name"
  FROM
      "late"
      INNER JOIN "abhorrent" on "late"."best_performers_rnk" = "abhorrent"."worst_performers_rnk"
  GROUP BY
      1,
      2,
      3
  ORDER BY
      "rank" asc,
      "best_performer_product_name" desc,
      "worst_performer_product_name" desc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query45.preql`

  ```text
  Syntax error in query45.preql: Undefined concept: item.id. Suggestions: ['web_sales.item.id', 'web_sales.date.id', 'web_sales.ship_date.id', 'web_sales.return_date.id', 'web_sales.time.id', 'web_sales.return_time.id']
  ```
- `trilogy file read query46.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query53.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write query54.preql --content # Query 54: Customers who bought Women/maternity via catalog or web in Dec 1998
# then sum their store sales in Ja…log_cust.cust_id or web_cust.cust_id}
#   AND store.county = customer.address.county
#   AND store.state = customer.address.state {'escapes': 'true'}`

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy run --import raw.catalog_sales:cs select cs.bill_customer.id, cs.item.category, cs.item.class, cs.date.year, cs.date.month_of_year where cs.bill_cust….id = some_customer_id and cs.item.category = 'Women' and cs.item.class = 'maternity' and cs.date.month_of_year = 12 and cs.date.year = 1998 limit 5;`

  ```text
  Syntax error in stdin: Undefined concept: local.some_customer_id (line 2, col 126, in WHERE). Suggestions: ['cs.ship_customer.id', 'cs.bill_customer.id', 'cs.ship_customer.text_id']
  ```
- `trilogy run query57.preql`

  ```text
  Syntax error in query57.preql: Comparison `cs.sold_date.month_of_year between 1 and 12` matches every value of enum field 'cs.sold_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy run query58.preql duckdb`

  ```text
  Syntax error in query58.preql: 4 undefined concept references; fix all before re-running:
    - date.week_seq (line 4, col 5, in SELECT); did you mean: all_sales.date.week_seq, all_sales.return_date.week_seq, all_sales.billing_customer.first_sales_date.week_seq, all_sales.billing_customer.first_shipto_date.week_seq, all_sales.ship_customer.first_sales_date.week_seq, all_sales.ship_customer.first_shipto_date.week_seq?
    - date.date (line 5, col 5, in SELECT); did you mean: all_sales.date.date, all_sales.return_date.date, all_sales.billing_customer.first_sales_date.date, all_sales.billing_customer.first_shipto_date.date, all_sales.ship_customer.first_sales_date.date, all_sales.ship_customer.first_shipto_date.date?
    - date.date (line 6, col 7, in WHERE); did you mean: all_sales.date.date, all_sales.return_date.date, all_sales.billing_customer.first_sales_date.date, all_sales.billing_customer.first_shipto_date.date, all_sales.ship_customer.first_sales_date.date, all_sales.ship_customer.first_shipto_date.date?
    - date.week_seq (line 7, col 10, in ORDER BY); did you mean: all_sales.date.week_seq, all_sales.return_date.week_seq, all_sales.billing_customer.first_sales_date.week_seq, all_sales.billing_customer.first_shipto_date.week_seq, all_sales.ship_customer.first_sales_date.week_seq, all_sales.ship_customer.first_shipto_date.week_seq?
  ```
- `trilogy run query59.preql`

  ```text
  Resolution error in query59.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 7). The requested concepts split into 3 disconnected subgraphs: {d.year, local._weekly_2001_week_seq}; {local._weekly_2001_fri_total, local._weekly_2001_mon_total, local._weekly_2001_sat_total, local._weekly_2001_sun_total, local._weekly_2001_thu_total, local._weekly_2001_tue_total, local._weekly_2001_wed_total}; {local._weekly_2001_store_code, local._weekly_2001_store_id, local._weekly_2001_store_name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run --import raw.store_sales:ss with combined as 
where ss.date.year in (2001, 2002)
select
    ss.store.id as store_id,
    ss.store.name as store_n…ere c2001.year = 2001 and c2002.year = 2002
order by c2001.store_name nulls first, c2001.store_code nulls first, c2001.week_seq nulls first
limit 20;`

  ```text
  Syntax error in stdin: Undefined concept: c2001.store_id. Suggestions: ['combined.store_id', 'c2002.store_id', 'c2001.store_code', 'c2001.store_name']
  ```
- `trilogy run --import raw.store_sales:ss with weekly as
select
    ss.store.id as store_id,
    ss.store.name as store_name,
    ss.store.text_id as store_cod….store_id order by weekly.week_seq) != 0
order by weekly.store_name nulls first, weekly.store_code nulls first, weekly.week_seq nulls first
limit 20;`

  ```text
  Syntax error in stdin: ORDER BY references 'weekly.week_seq', a measure at a finer grain (Grain<weekly.store_code,weekly.store_id,weekly.store_name,weekly.sun_total,weekly.week_seq,weekly.year_val>) than the select grain (Grain<local.sun_ratio,local.this_year_week_seq,weekly.store_code,weekly.store_name>|weekly.year_val = 2002 and local._virt_window_lag_1415401741488838 is not MagicConstants.NULL and local._virt_window_lag_1415401741488838 != 0); it has no single value per output row. Project it (prefix with `--` to keep it out of the rows) and order by that alias instead.
  ```
- `trilogy file read raw/store.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: sr.ticket_number (line 36, col 27, in WHERE). Suggestions: ['ss.ticket_number', 'ss.store.suite_number', 'cr.order_number']
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 26). The requested concepts split into 4 disconnected subgraphs: {local.cat_ext_list_price_by_item}; {local.cat_refund_by_item}; {local.coupon_sum, local.list_price_sum, local.sale_lines, local.wholesale_sum, ss.customer.address.city, ss.customer.address.street_name, ss.customer.address.street_number, ss.customer.address.zip, ss.customer.demographics.marital_status, ss.customer.first_sales_date.year, ss.customer.first_shipto_date.year, ss.customer_demographic.marital_status, ss.date.year, ss.item.color, ss.item.current_price, ss.item.id, ss.item.product_name, ss.item.text_id, ss.sale_address.city, ss.sale_address.street_name, ss.sale_address.street_number, ss.sale_address.zip, ss.store.name, ss.store.zip, ss.ticket_number}; {ret_lines.ret_item_id, ret_lines.sr.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query64.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: ORDER BY references 'store_sales.store.id', which is not in the SELECT projection (line 11). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_sales.store.id order by store_sales.store.id asc`.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_sales:ss --import raw/web_sales:ws --import raw/catalog_sales:cs select ss.customer.id, ws.billing_customer.id, cs.ship_customer.id limit 3;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 3 disconnected subgraphs: {cs.ship_customer.id}; {ss.customer.id}; {ws.billing_customer.id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run --import raw/store_sales:ss --import raw/web_sales:ws --import raw/catalog_sales:cs merge ss.customer into ~ws.billing_customer; merge ss.customer into ~cs.ship_customer; select ss.customer.id, ws.billing_customer.id, cs.ship_customer.id limit 3;`

  ```text
  Syntax error in stdin: Undefined concept: ss.customer. Suggestions: ['ss.customer.demographics.id', 'ss.customer.demographics.gender', 'ss.customer.demographics.marital_status', 'ss.customer.demographics.education_status', 'ss.customer.demographics.purchase_estimate', 'ss.customer.demographics.credit_rating']
  ```
- `trilogy file read query69.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query70.preql`

  ```text
  Syntax error in query70.preql: ORDER BY references 'local.state_val', which is not in the SELECT projection (line 15). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.state_val order by local.state_val asc`.
  ```
- `trilogy run query70.preql`

  ```text
  Unexpected error in query70.preql: maximum recursion depth exceeded
  ```
- `trilogy file read query71.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query74.preql`

  ```text
  Resolution error in query74.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 23). The requested concepts split into 2 disconnected subgraphs: {store_totals.customer_id, store_totals.store_net_paid, store_totals.year}; {web_totals.customer_id, web_totals.web_net_paid, web_totals.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query74.preql`

  ```text
  Resolution error in query74.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 15). The requested concepts split into 2 disconnected subgraphs: {local.customer_code, local.first_name, local.last_name, local.store_2001, local.store_2002, store_sales.customer.id}; {local.web_2001, local.web_2002, web_sales.billing_customer.id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query74.preql`

  ```text
  Resolution error in query74.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 50). The requested concepts split into 2 disconnected subgraphs: {customer.id, local.customer_code, local.first_name, local.last_name}; {pivoted.cid, pivoted.store_2001, pivoted.store_2002, pivoted.web_2001, pivoted.web_2002}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Undefined concept: lines.item.brand_id. Suggestions: ['s.item.brand_id', 'item.brand_id', 'lines.item_id']
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: 3 undefined concept references; fix all before re-running:
    - yr2002.qty_2001 (line 56, in SELECT); did you mean: yr2001.qty_2001, yr2002.qty_2002, yr2002.amt_2001?
    - yr2002.amt_2001 (line 56, in SELECT); did you mean: yr2001.amt_2001, yr2002.amt_2002, yr2002.qty_2001?
    - yr2002.qty_2001 (line 56, col 7, in WHERE); did you mean: yr2001.qty_2001, yr2002.qty_2002, yr2002.amt_2001?
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 6). The requested concepts split into 2 disconnected subgraphs: {item.category}; {local._lines_item_id, local._lines_net_amt, local._lines_net_qty, local._lines_yr, s.channel, s.ext_sales_price, s.order_id, s.quantity, s.return_amount, s.return_quantity}.
    - `item.category` is disconnected — did you mean `s.item.category`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `s.item.category`) instead of importing a second, disconnected copy.
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 21). The requested concepts split into 4 disconnected subgraphs: {lines.yr}; {local._agg_total_amt}; {local._agg_total_qty}; {s.item.brand_id, s.item.category_id, s.item.class_id, s.item.manufacturer_id}. Are you missing a join or merge statement to relate them?
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
  Syntax error in query77.preql: ORDER BY contains aggregate `grouping(combined.outlet_id)` (line 37), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.outlet_id) as g order by g desc`.
  ```
- `trilogy run query79.preql`

  ```text
  Syntax error in query79.preql: 11 undefined concept references; fix all before re-running:
    - customer.last_name (line 10, col 5, in SELECT); did you mean: store_sales.customer.last_name, store_sales.return_customer.last_name, customer.first_name?
    - customer.first_name (line 11, col 5, in SELECT); did you mean: store_sales.customer.first_name, store_sales.return_customer.first_name, customer.last_name?
    - store.city (line 3, in SELECT); did you mean: store_sales.store.city, store_sales.customer.address.city, store_sales.return_customer.address.city, store_sales.return_store.city, store_sales.sale_address.city, store_city?
    - date.day_of_week (line 4, col 5, in WHERE); did you mean: store_sales.date.day_of_week, store_sales.store.date.day_of_week, store_sales.return_store.date.day_of_week, store_sales.return_date.day_of_week, store_sales.customer.first_sales_date.day_of_week, store_sales.customer.first_shipto_date.day_of_week?
    - date.year (line 5, col 10, in WHERE); did you mean: store_sales.date.year, store_sales.store.date.year, store_sales.return_store.date.year, store_sales.return_date.year, store_sales.customer.first_sales_date.year, store_sales.customer.first_shipto_date.year?
    - store.employees (line 6, col 9, in WHERE); did you mean: store_sales.store.employees, store_sales.return_store.employees?
    - household_demographic.dependent_count (line 7, col 10, in WHERE); did you mean: store_sales.customer.household_demographic.dependent_count, store_sales.return_customer.household_demographic.dependent_count, store_sales.household_demographic.dependent_count, store_sales.customer.demographics.dependent_count, store_sales.return_customer.demographics.dependent_count, store_sales.customer_demographic.dependent_count?
    - household_demographic.vehicle_count (line 7, col 55, in WHERE); did you mean: store_sales.customer.household_demographic.vehicle_count, store_sales.return_customer.household_demographic.vehicle_count, store_sales.household_demographic.vehicle_count, household_demographic.dependent_count?
    - customer.id (line 8, col 9, in WHERE); did you mean: store_sales.customer.demographics.id, store_sales.customer.household_demographic.income_band.id, store_sales.customer.household_demographic.id, store_sales.customer.address.id, store_sales.customer.first_sales_date.id, store_sales.customer.first_shipto_date.id?
    - customer.last_name (line 17, col 5, in ORDER BY); did you mean: store_sales.customer.last_name, store_sales.return_customer.last_name, customer.first_name?
    - customer.first_name (line 18, col 5, in ORDER BY); did you mean: store_sales.customer.first_name, store_sales.return_customer.first_name, customer.last_name?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query80.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query83.preql`

  ```text
  Resolution error in query83.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 12). The requested concepts split into 2 disconnected subgraphs: {local.catalog_pct, local.catalog_qty, local.item_code, local.store_pct, local.store_qty, local.three_ch_avg, local.web_pct, local.web_qty, r.is_returned, r.item.text_id, r.return_date.week_seq}; {qual.cat_cnt, qual.store_cnt, qual.web_cnt}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query84.preql`

  ```text
  Resolution error in query84.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {customer.address.city, customer.demographics.id, customer.household_demographic.income_band.lower_bound, customer.household_demographic.income_band.upper_bound, customer.text_id, local.customer_code, local.full_name}; {store_returns.customer_demographic.id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query86.preql`

  ```text
  Syntax error in query86.preql: 5 undefined concept references; fix all before re-running:
    - item.category (line 21, col 5, in SELECT); did you mean: web_sales.item.category, g_category, web_sales.item.category_id?
    - item.class (line 22, col 5, in SELECT); did you mean: web_sales.item.class, item.category, web_sales.item.class_id?
    - item.category (line 18, in SELECT); did you mean: web_sales.item.category, g_category, web_sales.item.category_id?
    - item.class (line 18, in SELECT); did you mean: web_sales.item.class, item.category, web_sales.item.class_id?
    - item.category (line 27, col 34, in ORDER BY); did you mean: web_sales.item.category, g_category, web_sales.item.category_id?
  ```
- `trilogy file read query88.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query89.preql`

  ```text
  Resolution error in query89.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 7). The requested concepts split into 4 disconnected subgraphs: {date.month_of_year, date.year}; {item.brand_name, item.category, item.class}; {local.avg_total, local.total}; {store.company_name, store.name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query93.preql`

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
- `trilogy file read query95.preql`

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

### `syntax-parse`

- `trilogy file write query01.preql --content import raw.store_returns as sr;

# Total returns per billing customer per store (Tennessee, 2000)
auto cust_store_…tomer.text_id,
    --cust_store_total,
    --store_avg,
having cust_store_total > 1.2 * store_avg
order by sr.billing_customer.text_id asc
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:5
    |
  5 |     where sr.return_date.year = 2000
    |     ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...omer.text_id, sr.store.id     ??? where sr.return_date.year = 20...

  Write stats: received 536 chars / 536 bytes; tail: …'tore_avg\\norder by sr.billing_customer.text_id asc\\nlimit 100;'.
  ```
- `trilogy file write query02.preql --content import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Combine web and catalog sales into one rowset…t_year_sales ? lagged.day_of_week = 6), 0), 2) as sat,
where
    lagged.week_seq in weeks_in_2001.week_seq
order by
    lagged.week_seq nulls first
;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...in_2001 as union(     (select ??? distinct catalog.sold_date.wee...

  Write stats: received 2257 chars / 2257 bytes; tail: …'_in_2001.week_seq\\norder by\\n    lagged.week_seq nulls first\\n;'.
  ```
- `trilogy run --import raw/all_sales:sales select sales.date.year, count(sales.order_id) as cnt group by sales.date.year order by sales.date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   count(sales.order_id) as cnt ??? group by sales.date.year order...
  ```
- `trilogy run --import raw/all_sales:sales select sales.billing_customer.text_id, sales.channel, sales.date.year, sum(sales.ext_list_price - sales.ext_wholesal….year in (2001,2002) and sales.channel in ('STORE','CATALOG','WEB') group by sales.billing_customer.text_id, sales.channel, sales.date.year limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   in ('STORE','CATALOG','WEB') ??? group by sales.billing_custome...
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, sum(all_sales.ext_sales_price) as ext_sales, sum(all_sa…is not null group by all_sales.channel, all_sales.channel_dim_text_id order by all_sales.channel, all_sales.channel_dim_text_id nulls first limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...es.channel_dim_id is not null ??? group by all_sales.channel, al...
  ```
- `trilogy run --import raw/all_sales:all_sales --import raw/store:store select distinct all_sales.return_channel_dim_text_id, store.text_id where all_sales.channel = 'CATALOG' and all_sales.return_channel_dim_text_id = store.text_id limit 5;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...rt raw.store as store; select ??? distinct all_sales.return_chan...
  ```
- `trilogy file write query05.preql --content import raw.all_sales as s;

# Combine sales and returns per channel, attributing both to the selling entity
with c… grouping(combined.ch) asc,
    grouping(combined.entity_id) asc,
    combined.ch asc nulls first,
    combined.entity_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 18:47
     |
  18 |       and (s.date.date is null or s.date.date not between '2000-08-23'::date and '2000-09-06'::date)
     |                                               ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...e.date is null or s.date.date ??? not between '2000-08-23'::date...

  Write stats: received 4236 chars / 4236 bytes; tail: …'ls first,\\n    combined.entity_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Step 1: Find (brand_id, class_id, category_id) combinations that appear in all three….channel asc nulls first,
    s.item.brand_id asc nulls first,
    s.item.class_id asc nulls first,
    s.item.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:5
     |
  17 |     by (channel_bc.brand_id, channel_bc.class_id, channel_bc.category_id);
     |     ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...l_bc.channel = 'WEB') > 0     ??? by (channel_bc.brand_id, chann...

  Write stats: received 1766 chars / 1766 bytes; tail: …'lls first,\\n    s.item.category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Step 1: Compute (brand_id, class_id, category_id) combos appearing in all three chan….channel asc nulls first,
    s.item.brand_id asc nulls first,
    s.item.class_id asc nulls first,
    s.item.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | rowset all_channel_combos as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ree channels during 1999-2001 ??? rowset all_channel_combos as w...

  Write stats: received 1496 chars / 1496 bytes; tail: …'lls first,\\n    s.item.category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Step 1: Compute (brand_id, class_id, category_id) combos appearing in all three chan….channel asc nulls first,
    s.item.brand_id asc nulls first,
    s.item.class_id asc nulls first,
    s.item.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:1
     |
  21 | ;
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
   s.ext_list_price is not null ??? ;  # Step 3: Main query for No...

  Write stats: received 1586 chars / 1586 bytes; tail: …'lls first,\\n    s.item.category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.item as item;
import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import …total_sales) as total_sales
order by
    combined.last_name nulls first,
    combined.first_name nulls first,
    total_sales nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 51:10
     |
  51 |   union((catalog_feb), (web_feb)) -> (last_name, first_name, total_sales)
     |          ^---
     |
     = expected select_statement
  Location:
  ...als with combined as   union(( ??? catalog_feb), (web_feb)) -> (l...

  Write stats: received 2607 chars / 2607 bytes; tail: …'st_name nulls first,\\n    total_sales nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store_sales:store_sales select substring(store_sales.item.desc, 1, 30) as fd, count(store_sales.line_item) as cnt where store_sales.date.year between 2000 and 2003 group by fd having cnt > 4 limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...te.year between 2000 and 2003 ??? group by fd having cnt > 4 lim...
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale… total_sales
order by
    cat_web_combined.last_name nulls first,
    cat_web_combined.first_name nulls first,
    total_sales nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens - write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ...e.year between 2000 and 2003) ??? by substring(store_sales.item....

  Write stats: received 3487 chars / 3487 bytes; tail: …'rst_name nulls first,\\n    total_sales nulls first\\nlimit 100;'.
  ```
- `trilogy file write query24.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;

# First stage: join store_sales…tage1.sub_total) as total
having
    total > 0.05 * avg_sub_total
order by
    stage1.last_name asc,
    stage1.first_name asc,
    stage1.name asc;
`

  ```text
  refused to write 'query24.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities - to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...= store_returns.ticket_number ??? inner join store_sales.item.id...

  Write stats: received 1477 chars / 1477 bytes; tail: …'t_name asc,\\n    stage1.first_name asc,\\n    stage1.name asc;\\n'.
  ```
- `trilogy file write query26.preql --content import raw/catalog_sales as catalog_sales;

where
  catalog_sales.sold_date.year = 2000
  and catalog_sales.bill_c…avg(catalog_sales.coupon_amt) as avg_coupon_amt,
  avg(catalog_sales.sales_price) as avg_sales_price
order by
  catalog_sales.item.text_id
limit 100;`

  ```text
  refused to write 'query26.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as catalog_sales;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as catalog_s...

  Write stats: received 695 chars / 695 bytes; tail: …'sales_price\\norder by\\n  catalog_sales.item.text_id\\nlimit 100;'.
  ```
- `trilogy file write query31.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

# Compute totals by county and quarter …2 > 0
    and store_sum_q2 > 0
    and web_q2_over_q1 > store_q2_over_q1
    and web_q3_over_q2 > store_q3_over_q2
order by
    all_counties.county
;`

  ```text
  refused to write 'query31.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:38
     |
  11 |   by store_sales.sale_address.county as store_sum_q1;
     |                                      ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...ore_sales.sale_address.county ??? as store_sum_q1;  auto store_q...

  Write stats: received 2471 chars / 2471 bytes; tail: …'ver_q2 > store_q3_over_q2\\norder by\\n    all_counties.county\\n;'.
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Items matching any of the 8 profiles, with manufacturer_id 1-500
auto profile_items <-…s product_name
group by product_name
having count(profile_items ? item.manufact in shared_manuf) by product_name > 0
order by product_name
limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ....product_name as product_name ??? group by product_name having c...

  Write stats: received 1414 chars / 1414 bytes; tail: …'_manuf) by product_name > 0\\norder by product_name\\nlimit 100;'.
  ```
- `trilogy run --import raw/store_sales:store_sales select distinct store_sales.date.day_of_week, store_sales.date.day_name limit 10;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ..._sales as store_sales; select ??? distinct store_sales.date.day_...
  ```
- `trilogy file write query43.preql --content import raw.store_sales as store_sales;

# def macro: sum of sales_price (unit price) for a given day_of_week
def s… @thursday_sales as thursday,
    @friday_sales as friday,
    @saturday_sales as saturday
order by
    store_name asc,
    store_code asc
limit 100;`

  ```text
  refused to write 'query43.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:5
     |
  17 |     @sunday_sales as sunday,
     |     ^---
     |
     = expected select_item, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...re.text_id as store_code,     ??? @sunday_sales as sunday,     @...

  Write stats: received 1158 chars / 1158 bytes; tail: …'y\\norder by\\n    store_name asc,\\n    store_code asc\\nlimit 100;'.
  ```
- `trilogy file write query44.preql --content import raw.store_sales as store_sales;

# Filter to store id = 1 only
auto store1_sales <- store_sales.net_profit …_performers.rnk = worst_performers.rnk
order by
    rank asc,
    best_performer_product_name desc,
    worst_performer_product_name desc
limit 100;
`

  ```text
  refused to write 'query44.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
     avg_net_profit > threshold ??? group by     item_id,     prod...

  Write stats: received 1889 chars / 1889 bytes; tail: …'name desc,\\n    worst_performer_product_name desc\\nlimit 100;\\n'.
  ```
- `trilogy file write query54.preql --escapes --content import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;
import raw.store_sales as …ore.county = store_sales.customer.address.county
        and store_sales.store.state = store_sales.customer.address.state
    )) as customer_count
;
`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries - joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...lifying_customer_ids <- union( ??? (select catalog_women_maternit...

  Write stats: received 1986 chars / 1986 bytes; tail: …'ore_sales.customer.address.state\\n    )) as customer_count\\n;\\n'.
  ```
- `trilogy run --import raw.store_sales:ss with w2002 as where ss.date.year = 2002 select ss.store.id as sid, ss.date.week_seq as wk; select w2002.sid, w2002.wk, count(w2002.sid) as cnt group by w2002.sid, w2002.wk having cnt > 1 limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...2.wk, count(w2002.sid) as cnt ??? group by w2002.sid, w2002.wk h...
  ```
- `trilogy run --import raw.store_sales:ss where ss.store.id = 2 and ss.date.year between 2001 and 2002 select ss.date.week_seq, ss.date.year, sum(ss.sales_price) as total by ss.date.week_seq, ss.date.year order by ss.date.week_seq limit 60;`

  ```text
  Syntax error in stdin:  --> 2:130
    |
  2 | where ss.store.id = 2 and ss.date.year between 2001 and 2002 select ss.date.week_seq, ss.date.year, sum(ss.sales_price) as total by ss.date.week_seq, ss.date.year order by ss.date.week_seq limit 60;
    |                                                                                                                                  ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
   sum(ss.sales_price) as total ??? by ss.date.week_seq, ss.date.y...
  ```
- `trilogy file write query63.preql --content import raw.store_sales as ss;

# Filter: store sales in year 2000
where ss.date.year = 2000
  # store sales with a…m.manager_id as mgr_id,
  sum(ss.sales_price) as per_month_total,
  avg(per_month_total) over (partition by ss.item.manager_id) as avg_monthly_sales
`

  ```text
  refused to write 'query63.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...ager_id) as avg_monthly_sales ???

  Write stats: received 904 chars / 904 bytes; tail: …'over (partition by ss.item.manager_id) as avg_monthly_sales\\n'.
  ```
- `trilogy file write query63.preql --content import raw.store_sales as ss;

# Filter: store sales in year 2000, with a recorded store, and items in the two pro… avg_monthly > 0
    and abs(per_month_total - avg_monthly) / avg_monthly > 0.1
order by
    mgr_id,
    avg_monthly,
    per_month_total
limit 100;
`

  ```text
  refused to write 'query63.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...r, month) total of unit price ??? auto per_month_total <- sum(ss...

  Write stats: received 1191 chars / 1191 bytes; tail: …'    mgr_id,\\n    avg_monthly,\\n    per_month_total\\nlimit 100;\\n'.
  ```
- `trilogy file write query67.preql --content import raw.store_sales as ss;

# Compute summed sales (ext_sales_price = sales_price * quantity, treat null as 0)
…s.date.month_of_year asc nulls first,
    ss.store.text_id asc nulls first,
    summed_sales asc nulls first,
    cat_rank asc nulls first
limit 100;`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
    --> 33:1
     |
  33 | having
     | ^---
     |
     = expected limit or order_by
  Location:
  ...th_of_year, ss.store.text_id) ??? having     cat_rank <= 100 ord...

  Write stats: received 1707 chars / 1707 bytes; tail: …'les asc nulls first,\\n    cat_rank asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query67.preql --content import raw.store_sales as ss;

# Compute summed sales (ext_sales_price = sales_price * quantity, treat null as 0)
…s.date.month_of_year asc nulls first,
    ss.store.text_id asc nulls first,
    summed_sales asc nulls first,
    cat_rank asc nulls first
limit 100;`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:1
     |
  23 | having
     | ^---
     |
     = expected limit or order_by
  Location:
  ...th_of_year, ss.store.text_id) ??? having     cat_rank <= 100 ord...

  Write stats: received 1308 chars / 1308 bytes; tail: …'les asc nulls first,\\n    cat_rank asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query75.preql --content import raw.all_sales as s;
import raw.item as item;

# For items in 'Books' category, compute net sales per line (…curr_qty / prev_qty < 0.9
    and prev_qty is not null
    and curr_qty is not null
order by
    curr_qty - prev_qty asc,
    amt_diff asc
limit 100;`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
    --> 38:5
     |
  38 |     agg.brand_id,
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...ar,     2002 as curr_year     ??? agg.brand_id,     agg.class_id...

  Write stats: received 1324 chars / 1324 bytes; tail: …' by\\n    curr_qty - prev_qty asc,\\n    amt_diff asc\\nlimit 100;'.
  ```
- `trilogy file write query81.preql --content import raw/catalog_returns as cr;

# For catalog returns in year 2000, total return amount inc tax per (returning …r.address.country,
    cr.billing_customer.address.gmt_offset,
    cr.billing_customer.address.location_type,
    total_return_amt_inc_tax
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

  Write stats: received 1816 chars / 1816 bytes; tail: …'dress.location_type,\\n    total_return_amt_inc_tax\\nlimit 100;'.
  ```
- `trilogy run --import raw.all_sales:r select r.item.text_id, sum(1 ? r.channel = 'STORE') as s, sum(1 ? r.channel = 'CATALOG') as c, sum(1 ? r.channel = 'WEB') as w where r.return_date.week_seq in (5244, 5257, 5264) and r.is_returned group by r.item.text_id having s > 0 and c > 0 and w > 0 limit 30;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...5257, 5264) and r.is_returned ??? group by r.item.text_id having...
  ```
- `trilogy run --import raw.web_sales:ws select ws.date.year, count(ws.line_item) as cnt group by ws.date.year order by ws.date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...r, count(ws.line_item) as cnt ??? group by ws.date.year order by...
  ```
- `trilogy run --import raw.web_returns:wr select distinct wr.returning_demographic.marital_status, wr.refunded_demographic.marital_status where wr.web_sales.date.year = 2000;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...raw.web_returns as wr; select ??? distinct wr.returning_demograp...
  ```
- `trilogy run --import raw.web_returns:wr select wr.returning_demographic.marital_status, wr.returning_demographic.education_status, count(wr.web_sales.line_it…wr.web_sales.date.year = 2000 group by wr.returning_demographic.marital_status, wr.returning_demographic.education_status order by cnt desc limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...wr.web_sales.date.year = 2000 ??? group by wr.returning_demograp...
  ```
- `trilogy file write query86.preql --content import raw.web_sales as web_sales;

# Filter to year 2000
where web_sales.date.year = 2000

# Define grouping leve…parent,
    rnk
by rollup (item.category, item.class)
order by level desc nulls first, item.category asc nulls first, rnk asc nulls first
limit 100;
`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...000  # Define grouping levels ??? auto g_category <- grouping(it...

  Write stats: received 1049 chars / 1049 bytes; tail: …'em.category asc nulls first, rnk asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.customer.id, count(store_sales.line_item) as cnt where store_sales.return_reason.desc = 'reason 28' group by store_sales.customer.id order by cnt desc limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...urn_reason.desc = 'reason 28' ??? group by store_sales.customer....
  ```
- `trilogy file write query99.preql --content import raw/catalog_sales as cs;

where cs.ship_date.year = 2000
  and cs.warehouse.id is not null
  and cs.ship_mo…er_120
order by
    warehouse_name_20 asc nulls first,
    shipping_mode_type asc nulls first,
    call_center_name_lower asc nulls first
limit 100;
`

  ```text
  refused to write 'query99.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs;  wher...

  Write stats: received 857 chars / 857 bytes; tail: …'irst,\\n    call_center_name_lower asc nulls first\\nlimit 100;\\n'.
  ```

### `cli-misuse`

- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy explore raw/customer_demographics.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_demographics.preql' does not exist.
  ```
- `trilogy explore raw/date_dim.preql`

  ```text
  Invalid value for 'PATH': File 'raw/date_dim.preql' does not exist.
  ```
- `trilogy syntax example union-stack-channels`

  ```text
  No such command 'syntax'.
  ```
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy explore raw/billing_customer.preql`

  ```text
  Invalid value for 'PATH': File 'raw/billing_customer.preql' does not exist.
  ```

### `syntax-missing-alias`

- `trilogy file write query54.preql --escapes --content import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;
import raw.store_sales as …,
    segment * 50 as segment_times_50
order by
    segment asc nulls first,
    customer_count asc nulls first,
    segment_times_50 asc
limit 100;
`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `catalog_buyers.st
  union(
      (select web_buyers.cust_id, web_buyers.cnty, web_buyers.st),
      (select catalog_buyers.cust_id, catalog_buyers.cnty, catalog_buyers.st)
  ) -> (cust_id, cnty, st) as catalog_buyers_st_union_select_web_buyer`
  Location:
  ...s.cnty,     catalog_buyers.st ??? union(     (select web_buyers....

  Write stats: received 2217 chars / 2217 bytes; tail: …'_count asc nulls first,\\n    segment_times_50 asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query75.preql --content import raw.all_sales as s;

# Deduplicate per-line records for Books category
with lines as
where s.item.category …d.qty_2002 - pivoted.qty_2001 as qty_diff,
    pivoted.amt_2002 - pivoted.amt_2001 as amt_diff
order by
    qty_diff asc,
    amt_diff asc
limit 100;`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `lines.item_id in s.item.id
      and lines.yr in s.date.year
      lines.yr as lines_item_id_in_s_item_id_and_lines_yr_`
  Location:
  ...ines.item_id in s.item.id     ??? and lines.yr in s.date.year

  Write stats: received 1484 chars / 1484 bytes; tail: …'_diff\\norder by\\n    qty_diff asc,\\n    amt_diff asc\\nlimit 100;'.
  ```
- `trilogy run --import raw.all_sales:r select r.item.text_id, r.channel, count(r.is_returned) where r.return_date.week_seq in (5244, 5257, 5264) and r.is_returned limit 30;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(r.is_returned) as is_returned_count`
  Location:
  ...channel, count(r.is_returned) ??? where r.return_date.week_seq i...
  ```
- `trilogy file write query91.preql --content import raw.catalog_returns as catalog_returns;

# Catalog returns in 11th month of 1998
# The returning customer =…ation_status drive per-combination grouping but are not output
    sum(catalog_returns.net_loss) as total_net_loss
order by
    total_net_loss desc;
`

  ```text
  refused to write 'query91.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `marital_status and education_status drive per-combination grouping but are not output
      sum(catalog_returns.net_loss) as marital_status_and_education_status_driv`
  Location:
  ...manager,     --marital_status ??? and education_status drive per...

  Write stats: received 1365 chars / 1365 bytes; tail: …'t_loss) as total_net_loss\\norder by\\n    total_net_loss desc;\\n'.
  ```
