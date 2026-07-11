# Trilogy failure analysis — 20260711-185953

- Run `20260711-185953_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 176 | failed: 28 (16%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 16 | 57% |
| `syntax-parse` | 7 | 25% |
| `planner-recursion` | 5 | 18% |

## Detail

### `other`

- `trilogy run answer_1858999935.preql`

  ```text
  Syntax error in answer_1858999935.preql: Undefined concept: local.r0.
  ```
- `trilogy file read answer_1858999935.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3863442186.preql`

  ```text
  Unexpected error in answer_3863442186.preql: Conflicting join types (full, left outer) on keys joined into one group: a FULL/UNION join cannot be mixed with another type on the same key (it is ambiguous whether the key is required or one-sided). Make the whole group one type (e.g. `UNION JOIN a = b = c`), or use a distinct key.
  ```
- `trilogy run answer_3863442186.preql`

  ```text
  Syntax error in answer_3863442186.preql: Undefined concept: local.sv01. Suggestions: ['s01.sv01']
  ```
- `trilogy run answer_3863442186.preql`

  ```text
  failed to pin block of size 256.0 KiB (24.9 GiB/25.0 GiB used)\n\nPossible solutions:\n* Reducing the number of threads (SET threads=X)\n* Disabling insertion-order preservation (SET preserve_insertion_order=false)\n* Increasing the memory limit (SET memory_limit='...GB')\n\nSee also https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads\n[SQL: \nWITH \nhard as (\nSELECT\n    \"web_billing_customer_customers\".\"C_CUSTOMER_ID\" as \"_w_ann_cid\",\n    \"web_date_date\".\"D_YEAR\" as \"_w_ann_yr\",\n    sum(( ( \"web_web_sales\".\"WS_EXT_LIST_PRICE\" - \"web_web_sales\".\"WS_E
  …
  02_s_ann_cid\" AND coalesce(\"courageous\".\"sc_s02_s_ann_cid\", \"flashy\".\"sc_s02_s_ann_cid\") = \"cool\".\"sc_s02_s_ann_cid\"\nORDER BY \n    \"customer_id\" asc nulls first,\n    \"first_name\" asc nulls first,\n    \"last_name\" asc nulls first,\n    \"preferred_cust_flag\" asc nulls first\nLIMIT (100)]\n(Background on this error at: https://sqlalche.me/e/20/e3q8)",
    "error_type": "OperationalError"
  }
  {
    "event": "output_truncated",
    "dropped_events": 2,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```
- `trilogy run answer_3863442186.preql`

  ```text
  Syntax error in answer_3863442186.preql: Nothing was executed: parsed 7 definition statement(s) (3 imports, 2 merges, 2 rowsets) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.all_sales:all_sales select s.channel, s.channel_dim_text_id, s.return_channel_dim_text_id, s.date.date, s.return_date.date, s.is_returned where s.is_returned = true and s.channel = 'STORE' limit 5;`

  ```text
  Syntax error in stdin: 8 undefined concept references; fix all before re-running:
    - s.channel (line 2, col 8, in SELECT); did you mean: s.channel_dim_text_id, all_sales.channel?
    - s.channel_dim_text_id (line 2, col 19, in SELECT); did you mean: s.return_channel_dim_text_id, s.channel, all_sales.channel_dim_text_id, all_sales.return_channel_dim_text_id?
    - s.return_channel_dim_text_id (line 2, col 42, in SELECT); did you mean: s.channel_dim_text_id, all_sales.return_channel_dim_text_id, all_sales.return_channel_dim_id?
    - s.date.date (line 2, col 72, in SELECT); did you mean: s.return_date.date, all_sales.date.date, all_sales.return_date.date, all_sales.ship_customer.first_sales_date.date, all_sales.ship_customer.first_shipto_date.date, all_sales.billing_customer.first_sales_date.date?
    - s.return_date.date (line 2, col 85, in SELECT); did you mean: s.date.date, s.is_returned, all_sales.return_date.date, all_sales.date.date, all_sales.ship_customer.first_sales_date.date, all_sales.ship_customer.first_shipto_date.date?
    - s.is_returned (line 2, col 105, in SELECT); did you mean: all_sales.is_returned?
    - s.is_returned (line 2, col 125, in WHERE); did you mean: all_sales.is_returned?
    - s.channel (line 2, col 150, in WHERE); did you mean: s.channel_dim_text_id, all_sales.channel?
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Syntax error in answer_3705756794.preql: ORDER BY contains aggregate `grouping(local.channel_type)` (line 70), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.channel_type) as g order by g desc`.
  ```
- `trilogy run --import raw.web_sales:web_sales select ws.web_site.id, ws.return_amount, ws.return_net_loss, ws.return_date.date, ws.date.date where ws.return_date.date between '2000-08-23'::date and '2000-09-06'::date and ws.return_amount is not null and ws.web_site.id is null limit 5;`

  ```text
  Syntax error in stdin: 8 undefined concept references; fix all before re-running:
    - ws.web_site.id (line 2, col 8, in SELECT); did you mean: web_sales.web_site.id, web_sales.item.id, web_sales.date.id, web_sales.time.id, web_sales.web_page.id, web_sales.ship_date.id?
    - ws.return_amount (line 2, col 24, in SELECT); did you mean: ws.return_net_loss, ws.return_date.date, web_sales.return_amount?
    - ws.return_net_loss (line 2, col 42, in SELECT); did you mean: ws.return_amount, web_sales.return_net_loss?
    - ws.return_date.date (line 2, col 62, in SELECT); did you mean: ws.date.date, ws.return_amount, ws.return_net_loss, web_sales.return_date.date, web_sales.date.date, web_sales.ship_date.date?
    - ws.date.date (line 2, col 83, in SELECT); did you mean: ws.return_date.date, web_sales.date.date, web_sales.ship_date.date, web_sales.return_date.date, web_sales.ship_customer.first_sales_date.date, web_sales.ship_customer.first_shipto_date.date?
    - ws.return_date.date (line 2, col 102, in WHERE); did you mean: ws.date.date, ws.return_amount, ws.return_net_loss, web_sales.return_date.date, web_sales.date.date, web_sales.ship_date.date?
    - ws.return_amount (line 2, col 176, in WHERE); did you mean: ws.return_net_loss, ws.return_date.date, web_sales.return_amount?
    - ws.web_site.id (line 2, col 209, in WHERE); did you mean: web_sales.web_site.id, web_sales.item.id, web_sales.date.id, web_sales.time.id, web_sales.web_page.id, web_sales.ship_date.id?
  ```
- `trilogy file read answer_3705756794.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_883027685.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3697706765.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Syntax error in answer_3697706765.preql: Undefined concept: customer.preferred_cust_flag.
  ```
- `trilogy file read answer_3697706765.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_4199102535.preql`

  ```text
  Resolution error in answer_4199102535.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 32). The requested concepts split into 2 disconnected subgraphs: {college_dependent_count, credit_rating, customer_count, customer_count_2, customer_count_3, customer_count_4, customer_count_5, customer_count_6, dependent_count, education_status, employed_dependent_count, gender, marital_status, purchase_estimate}; {store_customers.customer_sk}. Are you missing a join or merge statement to relate them?
  ```

### `syntax-parse`

- `trilogy file write answer_1858999935.preql -e -c import raw.web_sales as web;
import raw.catalog_sales as cat;

# Build daily sales for each (week_seq, day_o…total is not null
  and f4.future_total is not null
  and f5.future_total is not null
  and f6.future_total is not null
order by f0.ws asc
limit 100;`

  ```text
  refused to write 'answer_1858999935.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...6.future_total, 2) as ratio_6 ??? subset join f0.dow = 0, f1.dow...

  Write stats: received 1750 chars / 1750 bytes; tail: …'nd f6.future_total is not null\\norder by f0.ws asc\\nlimit 100;'.
  ```
- `trilogy file write answer_1858999935.preql -e -c import raw.web_sales as web;
import raw.catalog_sales as cat;

# Stack web and catalog sales with their sold…re_total ? future.dow = 6), 2) as ratio_6
where
  future.future_total is not null
  and future.ws in ws_2001_all.ws
order by future.ws asc
limit 200;`

  ```text
  refused to write 'answer_1858999935.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...r 2001 with ws_2001 as select ??? distinct   web.date.week_seq a...

  Write stats: received 1991 chars / 1991 bytes; tail: …'uture.ws in ws_2001_all.ws\\norder by future.ws asc\\nlimit 200;'.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, count(all_sales.channel_dim_text_id) as cnt where all_sales.channel_dim_text_id = 'AAAAAAAABAAAAAAA' group by all_sales.channel;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ..._text_id = 'AAAAAAAABAAAAAAA' ??? group by all_sales.channel;
  ```
- `trilogy file write answer_3705756794.preql -e -c import raw.all_sales as s;

# Sales aggregation: filter by sales date in range, non-null sale entity
with sa…urns,
    coalesce(sum(sales_agg.net_profit), 0) as net_profit
subset join sales_agg.ch = returns_agg.ch
subset join sales_agg.eid = returns_agg.eid
`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...sales_agg.ch = returns_agg.ch ??? subset join sales_agg.eid = re...

  Write stats: received 1682 chars / 1682 bytes; tail: …' returns_agg.ch\\nsubset join sales_agg.eid = returns_agg.eid\\n'.
  ```
- `trilogy run --import raw.web_sales:web_sales select ws.date.date, ws.return_date.date, ws.web_site.id as sale_site from raw.web_sales as ws where ws.return_date.date between '2000-08-23'::date and '2000-09-06'::date and ws.return_amount is not null and ws.web_site.id = 'AAAAAAAAKAAAAAAA' limit 5;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ..., ws.web_site.id as sale_site ??? from raw.web_sales as ws where...
  ```
- `trilogy file write answer_3697706765.preql --content import raw.store_sales as store_sales;

parameter zips string;

# Step 1: Find 5-digit ZIP codes where m…plit(zips, ',')
  )
select
  store_sales.store.name as store_name,
  sum(store_sales.net_profit) as total_net_profit
order by
  store_name
limit 100;`

  ```text
  refused to write 'answer_3697706765.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...ect substring(zip5, 1, 2)     ??? from qualifying_zips     where...

  Write stats: received 2454 chars / 2454 bytes; tail: …'profit) as total_net_profit\\norder by\\n  store_name\\nlimit 100;'.
  ```
- `trilogy file write answer_3263796749.preql --content import raw.store_sales as ss;

# Bucket 1-20: threshold 74129
# Bucket 21-40: threshold 122840
# Bucket …s.ext_discount_amount ? ss.quantity between 81 and 100)
        else avg(ss.net_paid ? ss.quantity between 81 and 100)
    end as bucket_81_100
by *;`

  ```text
  refused to write 'answer_3263796749.preql': not syntactically valid Trilogy.

  Parse error:
    --> 30:1
     |
  30 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...100)     end as bucket_81_100 ??? by *;

  Write stats: received 1377 chars / 1377 bytes; tail: …'.quantity between 81 and 100)\\n    end as bucket_81_100\\nby *;'.
  ```

### `planner-recursion`

- `trilogy run answer_3863442186.preql`

  ```text
  Resolution error in answer_3863442186.preql: query could not be planned; this is a bug.
  ```
- `trilogy run answer_3863442186.preql`

  ```text
  Resolution error in answer_3863442186.preql: query could not be planned; this is a bug.
  ```
- `trilogy run answer_3863442186.preql`

  ```text
  Resolution error in answer_3863442186.preql: query could not be planned; this is a bug.
  ```
- `trilogy run answer_3863442186.preql`

  ```text
  Resolution error in answer_3863442186.preql: query could not be planned; this is a bug.
  ```
- `trilogy run answer_3863442186.preql`

  ```text
  Resolution error in answer_3863442186.preql: query could not be planned; this is a bug.
  ```
