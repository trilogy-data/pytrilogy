# Trilogy failure analysis — 20260709-020531

- Run `20260709-020529_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 330 | failed: 33 (10%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 23 | 70% |
| `syntax-parse` | 9 | 27% |
| `syntax-missing-alias` | 1 | 3% |

## Detail

### `other`

- `trilogy file read query03.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 90 column 3 (char 2529). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query04.preql`

  ```text
  Syntax error in query04.preql: 3 undefined concept references; fix all before re-running:
    - first_name (line 131, col 5, in ORDER BY); did you mean: ss.customer.first_name, ss.return_customer.first_name, cs.ship_customer.first_name, cs.billing_customer.first_name, ws.billing_customer.first_name, ws.ship_customer.first_name?
    - last_name (line 132, col 5, in ORDER BY); did you mean: ss.customer.last_name, ss.return_customer.last_name, cs.ship_customer.last_name, cs.billing_customer.last_name, ws.billing_customer.last_name, ws.ship_customer.last_name?
    - preferred_cust_flag (line 133, col 5, in ORDER BY); did you mean: ss.customer.preferred_cust_flag, store_cust_2002.preferred_cust_flag, ws.ship_customer.preferred_cust_flag, ss.return_customer.preferred_cust_flag, cs.ship_customer.preferred_cust_flag, cs.billing_customer.preferred_cust_flag?
  ```
- `trilogy file read query04.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/all_sales:sales select sales.channel, sales.channel_dim_text_id, sales.store.id, sales.return_channel_dim_text_id limit 5;`

  ```text
  Syntax error in stdin: Undefined concept: sales.store.id (line 2, col 50, in SELECT). Suggestions: ['sales.time.id', 'sales.item.id', 'sales.date.id', 'sales.return_date.id', 'sales.billing_customer.address.id', 'sales.billing_customer.first_sales_date.id']
  ```
- `trilogy run --import raw/all_sales:sales select 
 case 
   when sales.channel = 'STORE' then 'store channel'
   when sales.channel = 'CATALOG' then 'catalog …hannel_label, entity_id)
order by grouping(channel_label) desc, grouping(entity_id) desc, channel_label nulls first, entity_id nulls first
limit 100;`

  ```text
  Syntax error in stdin: ORDER BY contains aggregate `grouping(local.channel_label)` (line 2), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.channel_label) as g order by g desc`.
  ```
- `trilogy run --import raw/all_sales:sales select 
 case 
   when sales.channel = 'STORE' then 'store channel'
   when sales.channel = 'CATALOG' then 'catalog …hannel_label, entity_id)
order by grouping(channel_label) desc, grouping(entity_id) desc, channel_label nulls first, entity_id nulls first
limit 100;`

  ```text
  Unexpected error in stdin: (_duckdb.BinderException) Binder Error: GROUPING function is not supported here
  [SQL:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_dim_unified"."CP_CATALOG_PAGE_SK" as "sales_channel_dim_id",
      "sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as "sales_channel_dim_text_id"
  FROM
      "catalog_page" as "sales_catalog_dim_unified"
  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_dim_unified"."S_STORE_SK" as "sales_channel_dim_id",
      "sales_store_dim_unified"."S_STORE_ID" as "sales_channel_dim_text_id"
  FROM
      "store" as "sales_store_dim_unified"
  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_dim_unified"."web_site_sk" as "sales_channel_dim_id",
      "sales_web_dim_unified"."web_site_id" as "sales_channel_dim_text_id"
  FROM
      "web_site" as "sales_web_dim_unified"),
  abundant as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_sk",
      "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
      "sales_catalog_returns_unified"."CR_RETURN_AMOUNT" as "sales_return_amount",
      "sales_catalog_returns_unified"."CR_NET_LOSS" as "sales_return_net_loss"
  FROM
      "catalog_returns" as "sales_catalog_returns_unified"
  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_sk",
      "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
      "sales_store_returns_unified"."SR_RETURN_AMT" as "sales_return_amount",
      "sales_store_returns_unified"."SR_NET_LOSS" as "sales_return_net_loss"
  FROM
      "store_returns" as "sales_store_returns_unified"
  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_sk",
      "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
      "sales_web_returns_unified"."WR_RETURN_AMT" as "sales_return_amount",
      "sales_web_returns_unified"."WR_NET_LOSS" as "sales_return_net_loss"
  FROM
      "web_returns" as "sales_web_returns_unified"),
  vacuous as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_sales_unified"."CS_CATALOG_PAGE_SK" as "sales_channel_dim_id",
      "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_sk",
      "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
      "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_sk",
      "sales_catalog_sales_unified"."CS_NET_PROFIT" as "sales_net_profit",
      "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id"
  FROM
      "catalog_sales" as "sales_catalog_sales_unified"
  WHERE
      "sales_catalog_sales_unified"."CS_CATALOG_PAGE_SK" is not null

  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_sales_unified"."SS_STORE_SK" as "sales_channel_dim_id",
      "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_sk",
      "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
      "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_sk",
      "sales_store_sales_unified"."SS_NET_PROFIT" as "sales_net_profit",
      "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id"
  FROM
      "store_sales" as "sales_store_sales_unified"
  WHERE
      "sales_store_sales_unified"."SS_STORE_SK" is not null

  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_sales_unified"."WS_WEB_SITE_SK" as "sales_channel_dim_id",
      "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_sk",
      "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
      "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_sk",
      "sales_web_sales_unified"."WS_NET_PROFIT" as "sales_net_profit",
      "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id"
  FROM
      "web_sales" as "sales_web_sales_unified"
  WHERE
      "sales_web_sales_unified"."WS_WEB_SITE_SK" is not null
  ),
  young as (
  SELECT
      "abundant"."sales_return_amount" as "sales_return_amount",
      "abundant"."sales_return_net_loss" as "sales_return_net_loss",
      "vacuous"."sales_ext_sales_price" as "sales_ext_sales_price",
      "vacuous"."sales_net_profit" as "sales_net_profit",
      CASE
  	WHEN coalesce("abundant"."sales_channel","cheerful"."sales_channel","vacuous"."sales_channel") = 'STORE' THEN 'store channel'
  	WHEN coalesce("abundant"."sales_channel","cheerful"."sales_channel","vacuous"."sales_channel") = 'CATALOG' THEN 'catalog channel'
  	WHEN coalesce("abundant"."sales_channel","cheerful"."sales_channel","vacuous"."sales_channel") = 'WEB' THEN 'web channel'
  	END as "channel_label",
      CASE
  	WHEN coalesce("abundant"."sales_channel","cheerful"."sales_channel","vacuous"."sales_channel") = 'STORE' THEN CONCAT('store', "cheerful"."sales_channel_dim_text_id")
  	WHEN coalesce("abundant"."sales_channel","cheerful"."sales_channel","vacuous"."sales_channel") = 'CATALOG' THEN CONCAT('catalog_page', "cheerful"."sales_channel_dim_text_id")
  	WHEN coalesce("abundant"."sales_channel","cheerful"."sales_channel","vacuous"."sales_channel") = 'WEB' THEN CONCAT('web_site', "cheerful"."sales_channel_dim_text_id")
  	END as "entity_id"
  FROM
      "vacuous"
      INNER JOIN "date_dim" as "sales_date_date" on "vacuous"."sales_date_sk" = "sales_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "abundant" on "vacuous"."sales_channel" = "abundant"."sales_channel" AND "vacuous"."sales_item_sk" = "abundant"."sales_item_sk" AND "vacuous"."sales_order_id" = "abundant"."sales_order_id"
      LEFT OUTER JOIN "cheerful" on "vacuous"."sales_channel_dim_id" = "cheerful"."sales_channel_dim_id" AND coalesce("vacuous"."sales_channel", "abundant"."sales_channel") = "cheerful"."sales_channel"
  WHERE
      cast("sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and coalesce("cheerful"."sales_channel_dim_id","vacuous"."sales_channel_dim_id") is not null
  )
  SELECT
      "young"."channel_label" as "channel_label",
      "young"."entity_id" as "entity_id",
      coalesce(sum("young"."sales_ext_sales_price"),0) as "total_ext_sales",
      coalesce(sum("young"."sales_return_amount"),0) as "total_returns",
      coalesce(sum("young"."sales_net_profit") - sum("young"."sales_return_net_loss"),0) as "net_profit"
  FROM
      "young"
  GROUP BY
      ROLLUP (1, 2)
  ORDER BY
      MIN(grouping("young"."channel_label")) desc,
      MIN(grouping("young"."entity_id")) desc,
      "young"."channel_label" asc nulls first,
      "young"."entity_id" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query06.preql`

  ```text
  Syntax error in query06.preql: Undefined concept: item.current_price. Suggestions: ['sales.item.current_price']
  ```
- `trilogy run query06.preql`

  ```text
  Resolution error in query06.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 12). The requested concepts split into 2 disconnected subgraphs: {cat_prices.avg_price, cat_prices.item.category}; {line_item_count, state, sales.customer.address.sk, sales.date.month_of_year, sales.date.year, sales.item.category, sales.item.current_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query06.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.customer:c --param zips=68014,39127,13394,31387,58048,76614,99999 select c.address.zip as zip where c.address.zip in split(zips, ',') limit 10;`

  ```text
  Syntax error in stdin: Undefined concept: local.zips (line 2, in WHERE). Suggestions: ['zip']
  ```
- `trilogy file read query08.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query11.preql`

  ```text
  Resolution error in query11.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 51). The requested concepts split into 2 disconnected subgraphs: {store_01.store_2001, store_01.store_rev.cust_id, store_02.store_2002, web_01.web_2001, web_02.web_2002}; {store_sales.customer.first_name, store_sales.customer.last_name, store_sales.customer.preferred_cust_flag}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query11.preql`

  ```text
  Unexpected error in query11.preql: Conflicting join types (full, left outer) on keys joined into one group: a FULL/UNION join cannot be mixed with another type on the same key (it is ambiguous whether the key is required or one-sided). Make the whole group one type (e.g. `UNION JOIN a = b = c`), or use a distinct key.
  ```
- `trilogy run query13.preql`

  ```text
  Syntax error in query13.preql: 6 undefined concept references; fix all before re-running:
    - store_sales.customer_demographics.marital_status (line 11, col 10, in WHERE); did you mean: store_sales.customer_demographic.marital_status, store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status?
    - store_sales.customer_demographics.education_status (line 11, col 69, in WHERE); did you mean: store_sales.customer_demographic.education_status, store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status?
    - store_sales.customer_demographics.marital_status (line 13, col 10, in WHERE); did you mean: store_sales.customer_demographic.marital_status, store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status?
    - store_sales.customer_demographics.education_status (line 13, col 69, in WHERE); did you mean: store_sales.customer_demographic.education_status, store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status?
    - store_sales.customer_demographics.marital_status (line 15, col 10, in WHERE); did you mean: store_sales.customer_demographic.marital_status, store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status?
    - store_sales.customer_demographics.education_status (line 15, col 69, in WHERE); did you mean: store_sales.customer_demographic.education_status, store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 102 column 4 (char 3717). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: Undefined concept: multi_channel.brand_id. Suggestions: ['ss.item.brand_id', 'cs.item.brand_id', 'ws.item.brand_id', 'store_combos.ss.item.brand_id', 'catalog_combos.cs.item.brand_id', 'web_combos.ws.item.brand_id']
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: Undefined concept: local.ss.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query17.preql`

  ```text
  Syntax error in query17.preql: Undefined concept: store_sales_agg.ticket_number. Suggestions: ['store_sales_agg.ss.item.id', 'store_sales_agg.ss.quantity', 'store_sales_agg.ss.item.desc', 'store_returns_agg.sr.ticket_number', 'ss.ticket_number', 'sr.ticket_number']
  ```
- `trilogy run query17.preql`

  ```text
  Syntax error in query17.preql: Undefined concept: ss_grp.ss.customer.id. Suggestions: ['ss_grp.customer_id', 'ss_grp.ss.item.id', 'ss_grp.ss.store.state', 'sr_grp.customer_id', 'cs_grp.customer_id', 'ss.item.id']
  ```
- `trilogy file read query18.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query04.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# ---- Store sales: ann…b_val_2001,
    combined.web_val_2002,
    catalog_growth,
    store_growth,
    web_growth
order by
    combined.cust_id asc nulls first
limit 100
;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
     --> 110:1
      |
  110 | select
      | ^---
      |
      = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...d catalog_growth > web_growth ??? select     combined.cust_id as...

  Write stats: received 3219 chars / 3219 bytes; tail: …'th\\norder by\\n    combined.cust_id asc nulls first\\nlimit 100\\n;'.
  ```
- `trilogy file write query04.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# ---- Store sales: ann…y
    customer_id asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100
;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...l: Filter and join names ---- ??? subset join with_growth.cust_i...

  Write stats: received 3591 chars / 3591 bytes; tail: …'s first,\\n    preferred_cust_flag asc nulls first\\nlimit 100\\n;'.
  ```
- `trilogy file write query08.preql --content parameter zips string;

import raw/store_sales as ss;

# Step 1: Find 5-digit ZIPs where >10 preferred customers h… 1998
  and ss.date.quarter = 2
select
    ss.store.name as store_name,
    sum(ss.net_profit) as total_net_profit
order by
    store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
   --> 3:8
    |
  3 | import raw/store_sales as ss;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  ...arameter zips string;  import ??? raw/store_sales as ss;  # Step...

  Write stats: received 879 chars / 879 bytes; tail: …'ofit) as total_net_profit\\norder by\\n    store_name\\nlimit 100;'.
  ```
- `trilogy run --import raw.customer:c select distinct c.address.zip as zip where c.address.zip in ('24128','76232','65084') limit 10;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...ort raw.customer as c; select ??? distinct c.address.zip as zip
  ```
- `trilogy file write query09.preql --content import raw.store_sales as sale;

# Quantity buckets: 1-20, 21-40, 41-60, 61-80, 81-100
# Thresholds: 74129, 122840…1_80 else avg_paid_61_80 end as bucket_61_80,
    case when cnt_81_100 > 165306 then avg_disc_81_100 else avg_paid_81_100 end as bucket_81_100
by *;
`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...d_81_100 end as bucket_81_100 ??? by *;

  Write stats: received 2010 chars / 2010 bytes; tail: …'disc_81_100 else avg_paid_81_100 end as bucket_81_100\\nby *;\\n'.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as sale;

# Single output row with 5 columns, one per quantity bucket (1-20,21-40,41-60,61-…scount_amount ? sale.quantity between 81 and 100)
         else avg(sale.net_paid ? sale.quantity between 81 and 100)
    end as bucket_81_100
by *;
`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
    --> 28:1
     |
  28 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...100)     end as bucket_81_100 ??? by *;

  Write stats: received 1514 chars / 1514 bytes; tail: …'quantity between 81 and 100)\\n    end as bucket_81_100\\nby *;\\n'.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as sale;

# Single output row with 5 columns, one per quantity bucket (1-20,21-40,41-60,61-… 100) by * end;

select
    b1 as bucket_1_20,
    b2 as bucket_21_40,
    b3 as bucket_41_60,
    b4 as bucket_61_80,
    b5 as bucket_81_100
by *;
`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...1_80,     b5 as bucket_81_100 ??? by *;

  Write stats: received 1649 chars / 1649 bytes; tail: …'1_60,\\n    b4 as bucket_61_80,\\n    b5 as bucket_81_100\\nby *;\\n'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Find (brand_i…hannel nulls first,
    channel_data.brand_id nulls first,
    channel_data.class_id nulls first,
    channel_data.category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:6
    |
  7 |     (import raw.store_sales as ss; where ss.date.year between 1999 and 2001
    |      ^---
    |
    = expected select_statement
  Location:
  ...ti_channel as intersect(     ( ??? import raw.store_sales as ss;

  Write stats: received 3994 chars / 3994 bytes; tail: …'s first,\\n    channel_data.category_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query18.preql --content import raw/catalog_sales as cs;

where 
    cs.sold_date.year = 1998
    and cs.billing_customer_demographic.gende…y asc nulls first,
    item_code asc nulls first,
    g_country asc nulls first,
    g_state asc nulls first,
    g_county asc nulls first
limit 100;`

  ```text
  refused to write 'query18.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs;  wher...

  Write stats: received 1417 chars / 1417 bytes; tail: …'ate asc nulls first,\\n    g_county asc nulls first\\nlimit 100;'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.store_sales:ss select ss.date.year, ss.date.quarter, count(ss.ticket_number) limit 10;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(ss.ticket_number) as ticket_number_count`
  Location:
  ...rter, count(ss.ticket_number) ??? limit 10;
  ```
