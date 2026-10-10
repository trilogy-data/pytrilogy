# Trilogy failure analysis — 20261001-122723

- Run `20261001-122722_enriched_deepseek_deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 1508 | failed: 67 (4%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `undefined-concept` | 22 | 33% |
| `syntax-parse` | 17 | 25% |
| `join-resolution` | 12 | 18% |
| `other` | 9 | 13% |
| `cli-misuse` | 3 | 4% |
| `syntax-missing-alias` | 1 | 1% |
| `file-not-found` | 1 | 1% |
| `import-path` | 1 | 1% |
| `no-output` | 1 | 1% |

## Detail

### `undefined-concept`

- `trilogy file write answer_4077069387.preql --run`

  ```text
  Syntax error in answer_4077069387.preql: Undefined concept: item.id. Suggestions: ['web_sales.item.id', 'web_sales.web_site.id', 'web_sales.ship_date.id', 'web_sales.ship_customer.id', 'web_sales.sale_date.id', 'web_sales.sale_time.id']
  ```
- `trilogy file write probe_bench.preql --run-and-delete`

  ```text
  Syntax error in probe_bench.preql: Undefined concept: sales.sale_date (line 6, in SELECT). Suggestions: ['sales.sale_date.sk', 'sales.sale_date.id', 'sales.sale_date.date', 'sales.sale_date.year', 'sales.sale_date.day_of_week', 'sales.sale_date.day_of_month']
  ```
- `trilogy file write probe_triple.preql --run-and-delete`

  ```text
  Syntax error in probe_triple.preql: Undefined concept: sales.sale_date. Suggestions: ['sales.sale_date.sk', 'sales.sale_date.id', 'sales.sale_date.date', 'sales.sale_date.year', 'sales.sale_date.day_of_week', 'sales.sale_date.day_of_month']
  ```
- `trilogy file write _probe5.preql --run-and-delete`

  ```text
  Syntax error in _probe5.preql: Undefined concept: csr.store_state (line 6, in SELECT). Suggestions: ['csr.store.state', 'csr.store.name', 'csr.store_sale_date.sk']
  ```
- `trilogy file write probe_counts.preql --run-and-delete`

  ```text
  Syntax error in probe_counts.preql: Undefined concept: x.store_id (line 4, in SELECT). Suggestions: ['x.store.id', 'x.store.zip', 'x.store.city']
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Syntax error in probe1.preql: Undefined concept: _virt_agg_grouping_2599847655796781.
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Syntax error in probe1.preql: Undefined concept: _virt_agg_grouping_3513081987384927.
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Syntax error in probe3.preql: Undefined concept: _virt_agg_grouping_2599847655796781.
  ```
- `trilogy file write probe4.preql --run-and-delete`

  ```text
  Syntax error in probe4.preql: Undefined concept: _virt_agg_grouping_3513081987384927.
  ```
- `trilogy file write probe5.preql --run-and-delete`

  ```text
  Syntax error in probe5.preql: Undefined concept: _virt_agg_grouping_3513081987384927.
  ```
- `trilogy file write probe6.preql --run-and-delete`

  ```text
  Syntax error in probe6.preql: Undefined concept: _virt_agg_grouping_2599847655796781.
  ```
- `trilogy file write answer_609701806.preql --run`

  ```text
  Syntax error in answer_609701806.preql: 5 undefined concept references; fix all before re-running:
    - local.ext_sales_price (line 6, in SELECT); did you mean: ss.ext_sales_price, total_ext_sales_price, ss.sales_price?
    - sale_date.year (line 8, col 3, in WHERE); did you mean: ss.sale_date.year, sale_date.month_of_year, ss.return_date.year, ss.customer.first_sales_date.year, ss.return_customer.first_sales_date.year, ss.customer.first_shipto_date.year?
    - sale_date.month_of_year (line 9, col 7, in WHERE); did you mean: ss.sale_date.month_of_year, sale_date.year, ss.return_date.month_of_year, ss.customer.first_sales_date.month_of_year, ss.return_customer.first_sales_date.month_of_year, ss.customer.first_shipto_date.month_of_year?
    - item.manager_id (line 10, col 7, in WHERE); did you mean: ss.item.manager_id, item.brand_id, ss.item.manufacturer_id?
    - item.brand_id (line 11, col 38, in ORDER BY); did you mean: ss.item.brand_id, item.brand_name, item.manager_id, brand_id?
  ```
- `trilogy file write answer_3036656719.preql --run`

  ```text
  Syntax error in answer_3036656719.preql: Undefined concept: _virt_agg_grouping_9185816277693433.
  ```
- `trilogy file write probeB.preql --run-and-delete`

  ```text
  Syntax error in probeB.preql: Undefined concept: _virt_agg_grouping_3448352567439983.
  ```
- `trilogy file write answer_2874833976.preql --run`

  ```text
  Syntax error in answer_2874833976.preql: Undefined concept: _virt_agg_grouping_4453574046401692.
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  Syntax error in probe_a.preql: Undefined concept: _virt_agg_grouping_4453574046401692.
  ```
- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  Syntax error in probe_b.preql: Undefined concept: _virt_agg_grouping_9769516219811737.
  ```
- `trilogy file write probe_main.preql --run-and-delete`

  ```text
  Syntax error in probe_main.preql: Undefined concept: c.current_demographics. Suggestions: ['c.current_demographics.sk', 'c.current_demographics.gender', 'c.current_demographics.marital_status', 'c.current_demographics.education_status', 'c.current_demographics.purchase_estimate', 'c.current_demographics.credit_rating']
  ```
- `trilogy file write answer_2852230229.preql --run`

  ```text
  Syntax error in answer_2852230229.preql: Undefined concept: _virt_agg_grouping_5549377253046357.
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Syntax error in probe3.preql: Undefined concept: _virt_agg_grouping_164218295708188.
  ```
- `trilogy file write probe4.preql --run-and-delete`

  ```text
  Syntax error in probe4.preql: Undefined concept: _virt_agg_grouping_5549377253046357.
  ```
- `trilogy file write answer_32504671.preql --run`

  ```text
  Syntax error in answer_32504671.preql: Undefined concept: line_item. Suggestions: ['ss.line_item', 'ss.line_item_count']
  ```

### `syntax-parse`

- `trilogy file write probe_struct.preql --run-and-delete`

  ```text
  refused to write 'probe_struct.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...eturn_date.sk) as n_ret_date  ??? group by a.channel  order by a...
  ```
- `trilogy file write probe_q1.preql --run-and-delete`

  ```text
  refused to write 'probe_q1.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens - write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ...d ss.sale_date.year <= 2003)) ??? by ss.item.sk, (substring(ss.i...
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  refused to write 'probe_a.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:41
    |
  9 |   and lower(ss.customer.birth_country) <> lower(ss.customer.current_address.country)
    |                                         ^---
    |
    = expected sum_operator
  Location:
  ...r(ss.customer.birth_country) < ??? > lower(ss.customer.current_ad...
  ```
- `trilogy file write probe_csr.preql --run-and-delete`

  ```text
  refused to write 'probe_csr.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | limit 20;
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ntity, csr.catalog_quantity;  ??? limit 20;
  ```
- `trilogy file write probe_cand_b.preql --run-and-delete`

  ```text
  refused to write 'probe_cand_b.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...e.year in (1999, 2000, 2001)  ??? union join ss.customer.sk = cs...
  ```
- `trilogy file write probe10.preql --run-and-delete`

  ```text
  refused to write 'probe10.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | by ss.item.class
    | ^---
    |
    = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...ss.item.category = 'Jewelry'  ??? by ss.item.class  order by n a...
  ```
- `trilogy file write _probeB.preql --run-and-delete`

  ```text
  refused to write '_probeB.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:1
     |
  12 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...nd item.size = 'N/A')) as p8  ??? by *;
  ```
- `trilogy file write probe_itemids2.preql --run-and-delete`

  ```text
  refused to write 'probe_itemids2.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:1
     |
  10 | by ref_item_ids.ref_id;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
   count(item.sk) as item_rows  ??? by ref_item_ids.ref_id;
  ```
- `trilogy file write probeA.preql --run-and-delete`

  ```text
  refused to write 'probeA.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
   a, cs.net_paid_inc_tax as b  ??? from raw.catalog_sales as cs
  ```
- `trilogy file write probeB.preql --run-and-delete`

  ```text
  refused to write 'probeB.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...e.sk as tsk, a.quantity as q  ??? from raw.all_sales as a  limit...
  ```
- `trilogy file write probeF.preql --run-and-delete`

  ```text
  refused to write 'probeF.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:44
    |
  4 |   count(web.line_item ? (web.sale_time.sk <> web.sale_time.time)) as web_mismatch,
    |                                            ^---
    |
    = expected sum_operator
  Location:
  ...ine_item ? (web.sale_time.sk < ??? > web.sale_time.time)) as web_...
  ```
- `trilogy file write probe_roles.preql --run-and-delete --content import raw.all_sales as s;

select
  s.channel as channel,
  sum(1 ? s.channel is not null) a…ustomer.sk is not null and s.purchasing_customer.sk <> s.ship_customer.sk) as ps_diff,
  count_distinct(s.purchasing_customer.id) as cust_distinct
;
`

  ```text
  refused to write 'probe_roles.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:116
    |
  7 |   sum(1 ? s.purchasing_customer.sk is not null and s.billing_customer.sk is not null and s.purchasing_customer.sk <> s.billing_customer.sk) as pb_diff,
    |                                                                                                                    ^---
    |
    = expected sum_operator
  Location:
  ...and s.purchasing_customer.sk < ??? > s.billing_customer.sk) as pb...
  ```
- `trilogy file write answer_2910545909.preql --run`

  ```text
  refused to write 'answer_2910545909.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:9
     |
  22 |        --ss.pos_address.sk
     |         ^---
     |
     = expected access_chain
  Location:
      --ss.customer.sk         - ??? -ss.pos_address.sk         --s...
  ```
- `trilogy file write probe_main.preql --run-and-delete`

  ```text
  refused to write 'probe_main.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [231]: A `subset|union join` cannot follow a trailing `where`. Put the join right after the select list, with the filter either before `select` or after the join: `where <filters> select <cols> subset join a.key = b.key` or `select <cols> subset join a.key = b.key where <filters>`. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...me_band.upper_bound <= 88128  ??? union join c.current_demograph...
  ```
- `trilogy file write answer_840315271.preql --run`

  ```text
  refused to write 'answer_840315271.preql': not syntactically valid Trilogy.

  Parse error:
    --> 40:16
     |
  40 |     avg_total <> 0
     |                ^---
     |
     = expected sum_operator
  Location:
  ...otal,  having      avg_total < ??? > 0      and abs(monthly_total...
  ```
- `trilogy file write probe_d.preql --run-and-delete`

  ```text
  refused to write 'probe_d.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...count(ws.line_item) as lines  ??? group by ws.web_site.company_n...
  ```
- `trilogy file write probe_e.preql --run-and-delete`

  ```text
  refused to write 'probe_e.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...:date and '1999-04-02'::date  ??? group by ws.pos_ship_address.s...
  ```

### `join-resolution`

- `trilogy file write answer_4199102535.preql --run`

  ```text
  Resolution error in answer_4199102535.preql: Could not resolve connections for query with output ['local.gen<Purpose.PROPERTY>Derivation.BASIC>', 'local.ms<Purpose.PROPERTY>Derivation.BASIC>', 'local.edu<Purpose.PROPERTY>Derivation.BASIC>', 'local.cnt1<Purpose.METRIC>Derivation.AGGREGATE>', 'local.pe<Purpose.PROPERTY>Derivation.BASIC>', 'local.cnt2<Purpose.METRIC>Derivation.AGGREGATE>', 'local.cr<Purpose.PROPERTY>Derivation.BASIC>', 'local.cnt3<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dc<Purpose.PROPERTY>Derivation.BASIC>', 'local.cnt4<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dec<Purpose.PROPERTY>Derivation.BASIC>', 'local.cnt5<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dcc<Purpose.PROPERTY>Derivation.BASIC>', 'local.cnt6<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Resolution error in probe2.preql: Could not resolve connections for query with output ['local.gen<Purpose.PROPERTY>Derivation.BASIC>', 'local.cnt1<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy file write probe4.preql --run-and-delete`

  ```text
  Resolution error in probe4.preql: Could not resolve connections for query with output ['local.gen<Purpose.PROPERTY>Derivation.BASIC>', 'local.cnt1<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy file write answer_3809267817.preql --run`

  ```text
  Resolution error in answer_3809267817.preql: Could not resolve connections for query with output ['local.state<Purpose.PROPERTY>Derivation.BASIC>', 'local.gender<Purpose.PROPERTY>Derivation.BASIC>', 'local.marital_status<Purpose.PROPERTY>Derivation.BASIC>', 'local.dep_count<Purpose.PROPERTY>Derivation.BASIC>', 'local.dep_cnt<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_min<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_max<Purpose.METRIC>Derivation.AGGREGATE>', 'local.dep_avg<Purpose.METRIC>Derivation.AGGREGATE>', 'local.emp_count<Purpose.PROPERTY>Derivation.BASIC>', 'local.emp_cnt<Purpose.METRIC>Derivation.AGGREGATE>', 'local.emp_min<Purpose.METRIC>Derivation.AGGREGATE>', 'local.emp_max<Purpose.METRIC>Derivation.AGGREGATE>', 'local.emp_avg<Purpose.METRIC>Derivation.AGGREGATE>', 'local.coll_count<Purpose.PROPERTY>Derivation.BASIC>', 'local.coll_cnt<Purpose.METRIC>Derivation.AGGREGATE>', 'local.coll_min<Purpose.METRIC>Derivation.AGGREGATE>', 'local.coll_max<Purpose.METRIC>Derivation.AGGREGATE>', 'local.coll_avg<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy file write p3.preql --run-and-delete`

  ```text
  Resolution error in p3.preql: Could not resolve connections for query with output ['local.state<Purpose.PROPERTY>Derivation.BASIC>', 'local.cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy file write p4.preql --run-and-delete`

  ```text
  Resolution error in p4.preql: Could not resolve connections for query with output ['local.state<Purpose.PROPERTY>Derivation.BASIC>', 'local.cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy file write answer_3553309440.preql --run`

  ```text
  Resolution error in answer_3553309440.preql: Could not resolve connections for query with output ['local.segment<Purpose.PROPERTY>Derivation.BASIC>', 'local.cust_count<Purpose.METRIC>Derivation.AGGREGATE>', 'local.seg_x_50<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy file write probe_c.preql --run-and-delete`

  ```text
  Resolution error in probe_c.preql: Could not resolve connections for query with output ['cust_totals.customer_sk<Purpose.KEY>Derivation.ROWSET>', 'cust_totals.total<Purpose.METRIC>Derivation.ROWSET>'] from current model.
  ```
- `trilogy file write probe_f.preql --run-and-delete`

  ```text
  Resolution error in probe_f.preql: Could not resolve connections for query with output ['local.segment<Purpose.PROPERTY>Derivation.BASIC>', 'local.cust_count<Purpose.METRIC>Derivation.AGGREGATE>', 'local.seg_x_50<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  Resolution error in probe_a.preql: Could not resolve connections for query with output ['local.st<Purpose.PROPERTY>Derivation.BASIC>', 'local.c1<Purpose.METRIC>Derivation.AGGREGATE>', 'local.c2<Purpose.METRIC>Derivation.AGGREGATE>', 'local.c3<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy file write answer_755724379.preql --run`

  ```text
  Resolution error in answer_755724379.preql: Could not resolve connections for query with output ['local.cd_gender<Purpose.PROPERTY>Derivation.BASIC>', 'local.cd_marital_status<Purpose.PROPERTY>Derivation.BASIC>', 'local.cd_education_status<Purpose.PROPERTY>Derivation.BASIC>', 'local.cnt1<Purpose.METRIC>Derivation.AGGREGATE>', 'local.cd_purchase_estimate<Purpose.PROPERTY>Derivation.BASIC>', 'local.cnt2<Purpose.METRIC>Derivation.AGGREGATE>', 'local.cd_credit_rating<Purpose.PROPERTY>Derivation.BASIC>', 'local.cnt3<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy file write probe_d.preql --run-and-delete`

  ```text
  Resolution error in probe_d.preql: Could not resolve connections for query with output ['local.total<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```

### `other`

- `trilogy file write probe_q1.preql --run-and-delete`

  ```text
  Syntax error in probe_q1.preql: Cannot compare ArrayType<UNKNOWN> ((AggregateWrapper(function=count(<Filter: ref:ss.ticket_number where (ref:ss.sale_date.year >= 2000 and ref:ss.sale_date.year <= 2003) = True>), by=[ref:ss.item.sk], grouping=<AggregateGroupingMode.STANDARD: 'standard'>, grouping_sets=[], grain_inherited=False), (substring(ref:ss.item.desc,1,30)), ref:ss.sale_date.date)) and INTEGER (4) of different types with operator > in (AggregateWrapper(function=count(<Filter: ref:ss.ticket_number where (ref:ss.sale_date.year >= 2000 and ref:ss.sale_date.year <= 2003) = True>), by=[ref:ss.item.sk], grouping=<AggregateGroupingMode.STANDARD: 'standard'>, grouping_sets=[], grain_inherited=False), (substring(ref:ss.item.desc,1,30)), ref:ss.sale_date.date) > 4
  ```
- `trilogy file write probe_d.preql --run-and-delete`

  ```text
  Syntax error in probe_d.preql: Output column 'subtotal' renames 'local.subtotal' back to the name of an existing concept 'subtotal' (defined at line 3) that 'local.subtotal' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'subtotal_out').
  ```
- `trilogy file write probe_d.preql --run-and-delete`

  ```text
  Syntax error in probe_d.preql: Output column 'avg_all' renames 'local.avg_all' back to the name of an existing concept 'avg_all' (defined at line 4) that 'local.avg_all' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'avg_all_out').
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Syntax error in probe3.preql: Output column 'parent' renames 'local.parent' back to the name of an existing concept 'parent' (defined at line 6) that 'local.parent' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'parent_out').
  ```
- `trilogy file write answer_145690531.preql --run`

  ```text
  Syntax error in answer_145690531.preql: Output column 'store_total' renames 'local.store_total' back to the name of an existing concept 'store_total' (defined at line 3) that 'local.store_total' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'store_total_out').
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ':' delimiter: line 1 column 87 (char 86). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Syntax error in probe3.preql: ORDER BY references 'agg99.sum_ws', which is not in the SELECT projection (line 69). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --agg99.sum_ws order by agg99.sum_ws asc`.
  ```
- `trilogy file write probe_e.preql --run-and-delete`

  ```text
  Resolution error in probe_e.preql: WHERE input(s) ['ss.customer.current_address.state', 'ss.customer.sk', 'ss.sale_date.month_of_year', 'ss.sale_date.year'] cannot restrict output(s) ['x'] (statement at line 12): no join or merge relates the filter's source to the source of those outputs, so the WHERE has no single row population to define -- the outputs would cross-join in unfiltered. Add a join/merge relating them, or scope the filter to the source it belongs to with an inline filtered aggregate (e.g. `sum(x ? <condition>)`).
  ```
- `trilogy file write answer_2852230229.preql --run`

  ```text
  Syntax error in answer_2852230229.preql: ORDER BY references 'rolled.parent', which is not in the SELECT projection (line 15). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --rolled.parent order by rolled.parent asc`.
  ```

### `cli-misuse`

- `trilogy file write _probe2.preql --run --dry-run`

  ```text
  No such option '--dry-run'. Did you mean '--run'?
  ```
- `trilogy file delete _probe2.preql _probe3.preql`

  ```text
  Got unexpected extra argument (_probe3.preql)
  ```
- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```

### `syntax-missing-alias`

- `trilogy file write probe_ops.preql --run-and-delete`

  ```text
  refused to write 'probe_ops.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(ss.line_item ? (
              (ss.pos_customer_demographic.marital_status = 'M'
               and ss.pos_customer_demographic.education_status = 'Advanced Degree'
               and ss.sales_price between 100 and 150
               and ss.pos_household_demographic.dependent_count = 3)
           or (ss.pos_customer_demographic.marital_status = 'S'
               and ss.pos_customer_demographic.education_status = 'College'
               and ss.sales_price between 50 and 100
               and ss.pos_household_demographic.dependent_count = 1)
           or (ss.pos_customer_demographic.marital_status = 'W'
               and ss.pos_customer_demographic.education_status = '2 yr Degree'
               and ss.sales_price between 150 and 200
               and ss.pos_household_demographic.dependent_count = 1))
           and
              (ss.pos_address.country = 'United States'
               and ss.pos_address.state in ('TX', 'OH')
               and ss.net_profit between 100 and 200
              or ss.pos_address.country = 'United States'
               and ss.pos_address.state in ('OR', 'NM', 'KY')
               and ss.net_profit between 150 and 300
              or ss.pos_address.country = 'United States'
               and ss.pos_address.state in ('VA', 'TX', 'MS')
               and ss.net_profit between 50 and 250))) as both_between_in,
         count(ss.line_item ? (
              (ss.pos_customer_demographic.marital_status = 'M'
               and ss.pos_customer_demographic.education_status = 'Advanced Degree'
               and ss.sales_price >= 100 and ss.sales_price <= 150
               and ss.pos_household_demographic.dependent_count = 3)
           or (ss.pos_customer_demographic.marital_status = 'S'
               and ss.pos_customer_demographic.education_status = 'College'
               and ss.sales_price >= 50 and ss.sales_price <= 100
               and ss.pos_household_demographic.dependent_count = 1)
           or (ss.pos_customer_demographic.marital_status = 'W'
               and ss.pos_customer_demographic.education_status = '2 yr Degree'
               and ss.sales_price >= 150 and ss.sales_price <= 200
               and ss.pos_household_demographic.dependent_count = 1))
           and
              ((ss.pos_address.country = 'United States'
               and (ss.pos_address.state = 'TX' or ss.pos_address.state = 'OH')
               and ss.net_profit >= 100 and ss.net_profit <= 200)
              or (ss.pos_address.country = 'United States'
               and (ss.pos_address.state = 'OR' or ss.pos_address.state = 'NM' or ss.pos_address.state = 'KY')
               and ss.net_profit >= 150 and ss.net_profit <= 300)
              or (ss.pos_address.country = 'United States'
               and (ss.pos_address.state = 'VA' or ss.pos_address.state = 'TX' or ss.pos_address.state = 'MS')
               and ss.net_profit >= 50 and ss.net_profit <= 250)))) as both_ge_or
  where ss.sale_date.year = 2001; as count_ss_line_item_ss_pos_customer_demog`
  Location:
  ...et_profit between 50 and 250)) ??? ) as both_between_in,
  ```

### `file-not-found`

- `trilogy file write probe_3770074305.preql --run-and-delete`

  ```text
  Unexpected error in probe_3770074305.preql: (_duckdb.CatalogException) Catalog Error: Table with name cs_item_items does not exist!
  Did you mean "item"?

  LINE 40:     (exists (select 1 from cs_item_items where cs_item_items."cs_item_sk" is not distinct...
                                      ^
  [SQL:
  WITH
  abundant as (
  SELECT
      "cs_item_items"."I_ITEM_SK" as "cs_item_sk"
  FROM
      "item" as "cs_item_items"),
  juicy as (
  SELECT
      "inv_item_items"."I_ITEM_ID" as "item_code",
      "inv_item_items"."I_ITEM_SK" as "inv_item_sk",
      "inv_item_items"."I_ITEM_SK" as "item_sk"
  FROM
      "item" as "inv_item_items"),
  cooperative as (
  SELECT
      "inv_item_items"."I_CURRENT_PRICE" as "inv_item_current_price",
      "inv_item_items"."I_ITEM_SK" as "inv_item_sk",
      "inv_item_items"."I_MANUFACT_ID" as "inv_item_manufacturer_id",
      "inv_warehouse_inventory"."inv_quantity_on_hand" as "inv_quantity_on_hand",
      cast("inv_date_date"."D_DATE" as date) as "inv_date_date"
  FROM
      "inventory" as "inv_warehouse_inventory"
      INNER JOIN "date_dim" as "inv_date_date" on "inv_warehouse_inventory"."inv_date_sk" = "inv_date_date"."D_DATE_SK"
      INNER JOIN "item" as "inv_item_items" on "inv_warehouse_inventory"."inv_item_sk" = "inv_item_items"."I_ITEM_SK"
  WHERE
      "inv_item_items"."I_CURRENT_PRICE" >= 68 and "inv_item_items"."I_CURRENT_PRICE" <= 98 and ("inv_item_items"."I_MANUFACT_ID" is not null and "inv_item_items"."I_MANUFACT_ID" in (677,940,694,808)) and "inv_warehouse_inventory"."inv_quantity_on_hand" >= 100 and "inv_warehouse_inventory"."inv_quantity_on_hand" <= 500 and cast("inv_date_date"."D_DATE" as date) >= date '2000-02-01' and cast("inv_date_date"."D_DATE" as date) <= date '2000-04-01'
  ),
  quizzical as (
  SELECT
      "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number"
  FROM
      "catalog_sales" as "cs_catalog_sales"
  GROUP BY
      1),
  questionable as (
  SELECT
      "cooperative"."inv_item_sk" as "inv_item_sk",
      "quizzical"."cs_order_number" as "cs_order_number",
      (exists (select 1 from cs_item_items where cs_item_items."cs_item_sk" is not distinct from "cooperative"."inv_item_sk")) as "appears"
  FROM
      "quizzical"
      RIGHT OUTER JOIN "cooperative" on 1=1
  WHERE
      "cooperative"."inv_item_current_price" >= 68 and "cooperative"."inv_item_current_price" <= 98 and ("cooperative"."inv_item_manufacturer_id" is not null and "cooperative"."inv_item_manufacturer_id" in (677,940,694,808)) and "cooperative"."inv_quantity_on_hand" >= 100 and "cooperative"."inv_quantity_on_hand" <= 500 and "cooperative"."inv_date_date" >= date '2000-02-01' and "cooperative"."inv_date_date" <= date '2000-04-01'
  ),
  vacuous as (
  SELECT
      "questionable"."appears" as "appears",
      "questionable"."cs_order_number" as "cs_order_number",
      "questionable"."inv_item_sk" as "inv_item_sk"
  FROM
      "questionable"
      LEFT OUTER JOIN "juicy" on "questionable"."inv_item_sk" = "juicy"."inv_item_sk"
  GROUP BY
      1,
      2,
      3),
  young as (
  SELECT
      "vacuous"."appears" as "appears",
      "vacuous"."inv_item_sk" as "inv_item_sk",
      count("vacuous"."cs_order_number") as "catalog_lines"
  FROM
      "vacuous"
  GROUP BY
      1,
      2)
  SELECT
      "juicy"."item_code" as "item_code",
      "juicy"."item_sk" as "item_sk",
      "young"."catalog_lines" as "catalog_lines",
      "young"."appears" as "appears"
  FROM
      "young"
      LEFT OUTER JOIN "juicy" on "young"."inv_item_sk" = "juicy"."inv_item_sk"
  ORDER BY
      "juicy"."item_code" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```

### `import-path`

- `trilogy file write probe_q.preql --run-and-delete`

  ```text
  Import error in probe_q.preql: Unable to import '.\raw\catalogue_sales.preql': [Errno 2] No such file or directory: '.\\raw\\catalogue_sales.preql'.
  ```

### `no-output`

- `trilogy file write probe01.preql --run-and-delete`

  ```text
  Nothing was executed: the script contains no statements. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```
