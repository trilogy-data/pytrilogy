# Trilogy failure analysis — 20261001-122723

- Run `20261001-122722_ingest_deepseek_deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 1349 | failed: 57 (4%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `undefined-concept` | 24 | 42% |
| `syntax-parse` | 17 | 30% |
| `join-resolution` | 8 | 14% |
| `other` | 8 | 14% |

## Detail

### `undefined-concept`

- `trilogy file write answer_689898357.preql --run`

  ```text
  Syntax error in answer_689898357.preql: 11 undefined concept references; fix all before re-running:
    - local.quantity (line 5, in SELECT); did you mean: ss.quantity, avg_quantity?
    - local.list_price (line 6, in SELECT); did you mean: ss.list_price, avg_list_price, ss.ext_list_price?
    - local.coupon_amt (line 7, in SELECT); did you mean: ss.coupon_amt, avg_coupon_amount?
    - local.sales_price (line 8, in SELECT); did you mean: ss.sales_price, avg_sales_price, list_price?
    - date_dim.year (line 9, col 7, in WHERE); did you mean: ss.date_dim.year, ss.store.date_dim.year, ss.promotion.end_date.year, ss.promotion.start_date.year, ss.customer.first_sales_date.year, ss.customer.last_review_date.year?
    - customer_demographics.gender (line 10, col 7, in WHERE); did you mean: ss.customer_demographics.gender, ss.customer.customer_demographics.gender, customer_demographics.education_status, customer_demographics.marital_status, ss.customer_demographics.demo_sk?
    - customer_demographics.marital_status (line 11, col 7, in WHERE); did you mean: ss.customer_demographics.marital_status, ss.customer.customer_demographics.marital_status, customer_demographics.education_status, customer_demographics.gender?
    - customer_demographics.education_status (line 12, col 7, in WHERE); did you mean: ss.customer_demographics.education_status, ss.customer.customer_demographics.education_status, customer_demographics.marital_status, customer_demographics.gender?
    - promotion.channel_email (line 13, col 8, in WHERE); did you mean: ss.promotion.channel_email, promotion.channel_event, ss.promotion.channel_dmail, ss.promotion.channel_details?
    - promotion.channel_event (line 13, col 41, in WHERE); did you mean: ss.promotion.channel_event, promotion.channel_email, ss.promotion.channel_tv?
    - item.item_id (line 14, col 10, in ORDER BY); did you mean: ss.item.item_id, ss.promotion.item.item_id, ss.item.item_desc, ss.item.item_sk?
  ```
- `trilogy file write probe_grain.preql --run-and-delete`

  ```text
  Syntax error in probe_grain.preql: Undefined concept: ss.item.item_item_sk (line 7, in SELECT). Suggestions: ['ss.item.item_sk', 'ss.item.item_desc', 'ss.item.item_id']
  ```
- `trilogy file write answer_3849221871.preql --run`

  ```text
  Syntax error in answer_3849221871.preql: 6 undefined concept references; fix all before re-running:
    - local.quantity_on_hand (line 8, in SELECT); did you mean: inventory.quantity_on_hand?
    - item.product_name (line 3, in SELECT); did you mean: inventory.item.product_name, product_name?
    - item.brand (line 3, in SELECT); did you mean: inventory.item.brand, item.class, brand, inventory.item.brand_id?
    - item.class (line 3, in SELECT); did you mean: inventory.item.class, item.category, item.brand, class?
    - item.category (line 3, in SELECT); did you mean: inventory.item.category, item.class, category, inventory.item.category_id?
    - date_dim.year (line 9, col 7, in WHERE); did you mean: inventory.date_dim.year, inventory.date_dim.fy_year?
  ```
- `trilogy file write answer_3825713089.preql --run`

  ```text
  Syntax error in answer_3825713089.preql: 2 undefined concept references; fix all before re-running:
    - ss.sold_date.year (line 5, col 7, in WHERE); did you mean: ss.sold_date.moy, ss.date_dim.year, ss.store.date_dim.year, cs.sold_date.year, cs.ship_date.year, ss.promotion.end_date.year?
    - ss.sold_date.moy (line 6, col 7, in WHERE); did you mean: ss.sold_date.year, ss.date_dim.moy, ss.store.date_dim.moy, cs.sold_date.moy, cs.ship_date.moy, ss.promotion.end_date.moy?
  ```
- `trilogy file write answer_3809267817.preql --run`

  ```text
  Syntax error in answer_3809267817.preql: 6 undefined concept references; fix all before re-running:
    - state (line 40, col 10, in ORDER BY); did you mean: ss.store.state, ws.web_site.state, ws.bill_addr.state, ws.ship_addr.state, ws.warehouse.state, cs.bill_addr.state?
    - gender (line 40, col 33, in ORDER BY); did you mean: ws.bill_cdemo.gender, ws.ship_cdemo.gender, cs.bill_cdemo.gender, cs.ship_cdemo.gender, c.customer_demographics.gender, ss.customer_demographics.gender?
    - marital_status (line 40, col 57, in ORDER BY); did you mean: ws.bill_cdemo.marital_status, ws.ship_cdemo.marital_status, cs.bill_cdemo.marital_status, cs.ship_cdemo.marital_status, c.customer_demographics.marital_status, ss.customer_demographics.marital_status?
    - dep_count (line 40, col 89, in ORDER BY); did you mean: ws.bill_cdemo.dep_count, ws.ship_cdemo.dep_count, ws.bill_hdemo.dep_count, ws.ship_hdemo.dep_count, cs.bill_cdemo.dep_count, cs.ship_cdemo.dep_count?
    - dep_employed_count (line 40, col 116, in ORDER BY); did you mean: ws.bill_cdemo.dep_employed_count, ws.ship_cdemo.dep_employed_count, cs.bill_cdemo.dep_employed_count, cs.ship_cdemo.dep_employed_count, c.customer_demographics.dep_employed_count, ss.customer_demographics.dep_employed_count?
    - dep_college_count (line 40, col 152, in ORDER BY); did you mean: ws.bill_cdemo.dep_college_count, ws.ship_cdemo.dep_college_count, cs.bill_cdemo.dep_college_count, cs.ship_cdemo.dep_college_count, c.customer_demographics.dep_college_count, ss.customer_demographics.dep_college_count?
  ```
- `trilogy file write answer_3036656719.preql --run`

  ```text
  Syntax error in answer_3036656719.preql: Undefined concept: _virt_agg_grouping_1232307470070869.
  ```
- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  Syntax error in probe_b.preql: Undefined concept: _virt_agg_grouping_1232307470070869.
  ```
- `trilogy file write probe_c.preql --run-and-delete`

  ```text
  Syntax error in probe_c.preql: Undefined concept: _virt_agg_grouping_1232307470070869.
  ```
- `trilogy file write probe_d.preql --run-and-delete`

  ```text
  Syntax error in probe_d.preql: Undefined concept: _virt_agg_grouping_6135892101925565.
  ```
- `trilogy file write probe_e.preql --run-and-delete`

  ```text
  Syntax error in probe_e.preql: Undefined concept: _virt_agg_grouping_591157893606151.
  ```
- `trilogy file write probe_f.preql --run-and-delete`

  ```text
  Syntax error in probe_f.preql: Undefined concept: _virt_agg_grouping_449731194548500.
  ```
- `trilogy file write probe_g.preql --run-and-delete`

  ```text
  Syntax error in probe_g.preql: Undefined concept: _virt_agg_grouping_3513081987384927.
  ```
- `trilogy file write probe_h.preql --run-and-delete`

  ```text
  Syntax error in probe_h.preql: Undefined concept: _virt_agg_grouping_2599847655796781.
  ```
- `trilogy file write probe_i.preql --run-and-delete`

  ```text
  Syntax error in probe_i.preql: Undefined concept: _virt_agg_grouping_2599847655796781.
  ```
- `trilogy file write probe_q.preql --run-and-delete`

  ```text
  Syntax error in probe_q.preql: Undefined concept: _virt_agg_grouping_2599847655796781.
  ```
- `trilogy file write answer_2874833976.preql --run`

  ```text
  Syntax error in answer_2874833976.preql: 2 undefined concept references; fix all before re-running:
    - ss.sold_date.year (line 6, col 7, in WHERE); did you mean: ss.date_dim.year, ss.store.date_dim.year, ss.promotion.end_date.year, ss.customer.first_sales_date.year, ss.customer.last_review_date.year, ss.promotion.start_date.year?
    - store.store_sk (line 6, col 36, in WHERE); did you mean: ss.store.store_sk, store.state, ss.store.store_id, ss.store.store_name?
  ```
- `trilogy file write answer_747155668.preql --run`

  ```text
  Syntax error in answer_747155668.preql: Undefined concept: ss.sold_date.date_sk. Suggestions: ['ss.date_dim.date_sk', 'ss.store.date_dim.date_sk', 'ss.promotion.end_date.date_sk', 'ws.sold_date.date_sk', 'cs.sold_date.date_sk', 'ws.ship_date.date_sk']
  ```
- `trilogy file write answer_2852230229.preql --run`

  ```text
  Syntax error in answer_2852230229.preql: Undefined concept: _virt_agg_grouping_5549377253046357.
  ```
- `trilogy file write probe4.preql --run-and-delete`

  ```text
  Syntax error in probe4.preql: Undefined concept: gs.item.category. Suggestions: ['ws.item.category', 'ws.promotion.item.category', 'ws.item.category_id']
  ```
- `trilogy file write probe5.preql --run-and-delete`

  ```text
  Syntax error in probe5.preql: Undefined concept: _virt_agg_grouping_164218295708188.
  ```
- `trilogy file write probe7.preql --run-and-delete`

  ```text
  Syntax error in probe7.preql: Undefined concept: _virt_agg_grouping_5549377253046357.
  ```
- `trilogy file write probe8.preql --run-and-delete`

  ```text
  Syntax error in probe8.preql: Undefined concept: _virt_agg_grouping_5549377253046357.
  ```
- `trilogy file write answer_2852230229.preql --run`

  ```text
  Syntax error in answer_2852230229.preql: Undefined concept: _virt_agg_grouping_164218295708188.
  ```
- `trilogy file write answer_3562094594.preql --run`

  ```text
  Syntax error in answer_3562094594.preql: Undefined concept: ss.sold_date.year. Suggestions: ['ss.date_dim.year', 'ss.store.date_dim.year', 'ss.promotion.end_date.year', 'cs.sold_date.year', 'cs.ship_date.year', 'cs.promotion.end_date.year']
  ```

### `syntax-parse`

- `trilogy file write probe3863442186b.preql --run-and-delete`

  ```text
  refused to write 'probe3863442186b.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   (web_agg.v02 / web_agg.v01)  ??? group by store_agg.cust_id  li...
  ```
- `trilogy file write probe_3697440276.preql --run-and-delete`

  ```text
  refused to write 'probe_3697440276.preql': not syntactically valid Trilogy.

  Parse error:
    --> 24:1
     |
  24 | select
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
   store_2001)) as grew_faster  ??? select      chan_rev.cust_code...
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  refused to write 'probe2.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:1
     |
  21 | equal join ss.item.item_sk = sr.item.item_sk = cs.item.item_sk
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...omer.customer_sk is not null  ??? equal join ss.item.item_sk = s...
  ```
- `trilogy file write answer_2133330107.preql --run`

  ```text
  refused to write 'answer_2133330107.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:58
    |
  7 |   and substring(ss.customer.customer_address.zip, 1, 5) <> substring(ss.store.zip, 1, 5)
    |                                                          ^---
    |
    = expected sum_operator
  Location:
  ....customer_address.zip, 1, 5) < ??? > substring(ss.store.zip, 1, 5...
  ```
- `trilogy file write answer_2928586490.preql --run`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
    --> 32:1
     |
  32 | with combined <- union(
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...omers, combined per customer  ??? with combined <- union(      (...
  ```
- `trilogy file write answer_2844519538.preql --run`

  ```text
  refused to write 'answer_2844519538.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:45
    |
  8 |       and upper(ss.customer.birth_country) <> upper(cast(ss.customer_address.country as string))
    |                                             ^---
    |
    = expected sum_operator
  Location:
  ...r(ss.customer.birth_country) < ??? > upper(cast(ss.customer_addre...
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  refused to write 'probe_a.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:36
    |
  6 |     sum(it.manufact_id is null ? 1 : 0) as n_null_mfr
    |                                    ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...um(it.manufact_id is null ? 1 ??? : 0) as n_null_mfr  where it.c...
  ```
- `trilogy file write probe_verify.preql --run-and-delete`

  ```text
  refused to write 'probe_verify.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:1
    |
  9 | by *;
    | ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...sk is null) as avg_null_addr  ??? by *;
  ```
- `trilogy file write probe_week2.preql --run-and-delete`

  ```text
  refused to write 'probe_week2.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:1
    |
  8 | where d.week_seq = 5218;
    | ^---
    |
    = expected limit, ORDERING_DIRECTION, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...n_days,  order by d.week_seq  ??? where d.week_seq = 5218;
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  refused to write 'probe3.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:48
    |
  8 |   and ss.customer_demographics.marital_status <> ss.customer.customer_demographics.marital_status
    |                                                ^---
    |
    = expected sum_operator
  Location:
  ..._demographics.marital_status < ??? > ss.customer.customer_demogra...
  ```
- `trilogy file write answer_3063407983.preql --run`

  ```text
  refused to write 'answer_3063407983.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:42
    |
  7 |   and ss.customer.customer_address.city <> ss.customer_address.city
    |                                          ^---
    |
    = expected sum_operator
  Location:
  ...stomer.customer_address.city < ??? > ss.customer_address.city  se...
  ```
- `trilogy file write probe_flag.preql --run-and-delete`

  ```text
  refused to write 'probe_flag.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:11
     |
  10 |     isnull(cs.promotion.promo_sk) as no_promo_flag,
     |           ^---
     |
     = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, select_grouping, or JOIN_TYPE
  Location:
  ...m.week_seq  select      isnull ??? (cs.promotion.promo_sk) as no_...
  ```
- `trilogy file write answer_3046445280.preql --run`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...egory_id = y2002.category_id  ??? union join y2001.manufact_id =...
  ```
- `trilogy file write answer_3046445280.preql --run`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...- y2001.net_amt as amt_diff,  ??? union join y2001.brand_id = y2...
  ```
- `trilogy file write probe_r.preql --run-and-delete`

  ```text
  refused to write 'probe_r.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:57
    |
  6 | having count(grain(ss.item.item_sk, ss.ticket_number)) <> 0
    |                                                         ^---
    |
    = expected sum_operator
  Location:
  ....item_sk, ss.ticket_number)) < ??? > 0  order by b  limit 3;
  ```
- `trilogy file write probe_f.preql --run-and-delete`

  ```text
  refused to write 'probe_f.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...21 or item.manufact_id = 423  ??? by item.manufact_id;
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  refused to write 'probe_a.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:19
    |
  5 | with store_all as (
    |                   ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, or tvf_intersect_invocation
  Location:
  ...s as ws;    with store_all as ??? (      where ss.date_dim.year
  ```

### `join-resolution`

- `trilogy file write answer_4199102535.preql --run`

  ```text
  Resolution error in answer_4199102535.preql: Could not resolve connections for query with output ['local.g<Purpose.PROPERTY>Derivation.BASIC>', 'local.ms<Purpose.PROPERTY>Derivation.BASIC>', 'local.es<Purpose.PROPERTY>Derivation.BASIC>', 'local.c1<Purpose.METRIC>Derivation.AGGREGATE>', 'local.pe<Purpose.PROPERTY>Derivation.BASIC>', 'local.c2<Purpose.METRIC>Derivation.AGGREGATE>', 'local.cr<Purpose.PROPERTY>Derivation.BASIC>', 'local.c3<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dc<Purpose.PROPERTY>Derivation.BASIC>', 'local.c4<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dec<Purpose.PROPERTY>Derivation.BASIC>', 'local.c5<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dcc<Purpose.PROPERTY>Derivation.BASIC>', 'local.c6<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy file write probe_g.preql --run-and-delete`

  ```text
  Resolution error in probe_g.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {cust (= ss.customer.customer_sk), item_code (= ss.item.item_id), np (= ss.net_profit), ss.customer.customer_sk, ss.date_dim.moy, ss.date_dim.year, ss.item.item_id, store_code (= ss.store.store_id), ticket (= ss.ticket_number)}; {sr.customer.customer_sk, sr.date_dim.moy, sr.date_dim.moy, sr.date_dim.year}.
    - `sr.date_dim.moy` is disconnected, did you mean `ss.store.date_dim.moy`? (connected to the other concepts)
    - `sr.date_dim.moy` is disconnected, did you mean `ss.store.date_dim.moy`? (connected to the other concepts)
    - `sr.date_dim.year` is disconnected, did you mean `ss.store.date_dim.year`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `ss.store.date_dim.moy`) instead of importing a second, disconnected copy.
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Resolution error in probe2.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {inv.item.current_price, inv.item.item_id, inv.item.item_sk, inv.item.manufact_id, inv_rows}; {cs_rows}.
    - `inv.item.current_price` is disconnected, did you mean `cs.promotion.item.current_price`? (connected to the other concepts)
    - `inv.item.item_id` is disconnected, did you mean `cs.promotion.item.item_id`? (connected to the other concepts)
    - `inv.item.item_sk` is disconnected, did you mean `cs.promotion.item.item_sk`? (connected to the other concepts)
    - `inv.item.manufact_id` is disconnected, did you mean `cs.promotion.item.manufact_id`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `cs.promotion.item.current_price`) instead of importing a second, disconnected copy.
  ```
- `trilogy file write answer_755724379.preql --run`

  ```text
  Resolution error in answer_755724379.preql: Could not resolve connections for query with output ['local.gender<Purpose.PROPERTY>Derivation.BASIC>', 'local.marital_status<Purpose.PROPERTY>Derivation.BASIC>', 'local.education_status<Purpose.PROPERTY>Derivation.BASIC>', 'local.purchase_estimate<Purpose.PROPERTY>Derivation.BASIC>', 'local.credit_rating<Purpose.PROPERTY>Derivation.BASIC>', 'local.count1<Purpose.METRIC>Derivation.AGGREGATE>', 'local.count2<Purpose.METRIC>Derivation.AGGREGATE>', 'local.count3<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy file write answer_755724379.preql --run`

  ```text
  Resolution error in answer_755724379.preql: Could not resolve connections for query with output ['local.gender<Purpose.PROPERTY>Derivation.BASIC>', 'local.marital_status<Purpose.PROPERTY>Derivation.BASIC>', 'local.education_status<Purpose.PROPERTY>Derivation.BASIC>', 'local.purchase_estimate<Purpose.PROPERTY>Derivation.BASIC>', 'local.credit_rating<Purpose.PROPERTY>Derivation.BASIC>', 'local.count1<Purpose.METRIC>Derivation.AGGREGATE>', 'local.count2<Purpose.METRIC>Derivation.AGGREGATE>', 'local.count3<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy file write probeC.preql --run-and-delete`

  ```text
  Resolution error in probeC.preql: Could not resolve connections for query with output ['local.gender<Purpose.PROPERTY>Derivation.BASIC>', 'local.count1<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy file write answer_2374450308.preql --run`

  ```text
  Resolution error in answer_2374450308.preql: Could not resolve connections for query with output ['item.item_id<Purpose.PROPERTY>Derivation.ROOT>', 'item.item_desc<Purpose.PROPERTY>Derivation.ROOT>', 'item.current_price<Purpose.PROPERTY>Derivation.ROOT>'] from current model.
  ```
- `trilogy file write probe_d.preql --run-and-delete`

  ```text
  Resolution error in probe_d.preql: Could not resolve connections for query with output ['item.item_id<Purpose.PROPERTY>Derivation.ROOT>', 'item.item_desc<Purpose.PROPERTY>Derivation.ROOT>', 'item.current_price<Purpose.PROPERTY>Derivation.ROOT>'] from current model.
  ```

### `other`

- `trilogy file write answer_4140546834.preql --run`

  ```text
  Syntax error in answer_4140546834.preql: ORDER BY references 'local.parent', which is not in the SELECT projection (line 11). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.parent order by local.parent asc`.
  ```
- `trilogy file write probe_rowset.preql --run-and-delete`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file write answer_1484301313.preql --run`

  ```text
  Syntax error in answer_1484301313.preql: Impossible comparison in ref:ss.promotion.channel_email = Y: 'Y' can never match a declared value of enum<'N'> — fix the constant, or update the enum declaration if the domain is stale
  ```
- `trilogy file write probe_nocat.preql --run-and-delete`

  ```text
  Syntax error in probe_nocat.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 64). Move the filter to WHERE to filter before aggregation.
  ```
- `trilogy file write answer_2874833976.preql --run`

  ```text
  Syntax error in answer_2874833976.preql: ORDER BY references 'local.parent', which is not in the SELECT projection (line 18). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.parent order by local.parent asc`.
  ```
- `trilogy file write answer_1772060640.preql --run`

  ```text
  Syntax error in answer_1772060640.preql: Impossible comparison in SubselectComparison(left=ref:ss.store.county, right=('Orange County', 'Bronx County', 'Franklin Parish', 'Williamson County'), operator=<ComparisonOperator.IN: 'in'>): 'Orange County' can never match a declared value of enum<'Williamson County'> — fix the constant, or update the enum declaration if the domain is stale
  ```
- `trilogy file write answer_1772060640.preql --run`

  ```text
  Syntax error in answer_1772060640.preql: ORDER BY references 'ss.customer.customer_sk', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.customer.customer_sk order by ss.customer.customer_sk asc`.
  ```
- `trilogy file write probe6.preql --run-and-delete`

  ```text
  Syntax error in probe6.preql: ORDER BY references 'local.sortcat', which is not in the SELECT projection (line 12). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.sortcat order by local.sortcat asc`.
  ```
