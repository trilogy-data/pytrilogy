# Trilogy failure analysis — 20260808-151955

- Run `20260808-151955_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1236 | failed: 73 (6%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `cli-misuse` | 24 | 33% |
| `disabled-tool` | 20 | 27% |
| `syntax-parse` | 8 | 11% |
| `other` | 7 | 10% |
| `undefined-concept` | 6 | 8% |
| `file-not-found` | 4 | 5% |
| `join-resolution` | 2 | 3% |
| `import-path` | 1 | 1% |
| `syntax-missing-alias` | 1 | 1% |

## Detail

### `cli-misuse`

- `trilogy file remove diag.preql`

  ```text
  No such command 'remove'. Did you mean 'move'?
  ```
- `trilogy file remove diag2.preql`

  ```text
  No such command 'remove'. Did you mean 'move'?
  ```
- `trilogy file remove diag3.preql`

  ```text
  No such command 'remove'. Did you mean 'move'?
  ```
- `trilogy file remove diag4.preql`

  ```text
  No such command 'remove'. Did you mean 'move'?
  ```
- `trilogy explore raw.store_sales`

  ```text
  Invalid value for 'PATH': File 'raw.store_sales' does not exist.
  ```
- `trilogy explore raw.item`

  ```text
  Invalid value for 'PATH': File 'raw.item' does not exist.
  ```
- `trilogy file delete diag1.preql diag2.preql diag3.preql diag4.preql diag5.preql diag6.preql diag7.preql diag8.preql diag9.preql diag10.preql diag11.preql diag12.preql`

  ```text
  Got unexpected extra arguments (diag2.preql diag3.preql diag4.preql diag5.preql diag6.preql diag7.preql diag8.preql diag9.preql diag10.preql diag11.preql diag12.preql)
  ```
- `trilogy file rm diag.preql`

  ```text
  No such command 'rm'.
  ```
- `trilogy file rm diag2.preql`

  ```text
  No such command 'rm'.
  ```
- `trilogy file rm diag3.preql`

  ```text
  No such command 'rm'.
  ```
- `trilogy file rm diag4.preql`

  ```text
  No such command 'rm'.
  ```
- `trilogy file rm diag5.preql`

  ```text
  No such command 'rm'.
  ```
- `trilogy file rm diag6.preql`

  ```text
  No such command 'rm'.
  ```
- `trilogy file`

  ```text
  Usage: python -m trilogy.scripts.trilogy file [OPTIONS] COMMAND [ARGS]...

    Create, read, update, and delete files against local or remote backends.

    The same commands work against any backend Trilogy knows about. Only the
    local filesystem ships today; future releases will add cloud storage and
    remote git model backends, so write CLI-friendly scripts (and agent loops)
    against ``trilogy file`` instead of ad-hoc shell or python plumbing.

  Options:
    --help  Show this message and exit.

  Commands:
    delete  Delete the file or directory at PATH.
    exists  Exit 0 if PATH exists, 1 otherwise.
    list    List files at PATH (default: current directory).
    move    Move (or rename) SRC to DST.
    read    Read the file at PATH and write its contents to stdout.
    write   Write/overwrite the file at PATH.
  ```
- `trilogy cat inspect.preql`

  ```text
  No such command 'cat'.
  ```
- `trilogy file`

  ```text
  Usage: python -m trilogy.scripts.trilogy file [OPTIONS] COMMAND [ARGS]...

    Create, read, update, and delete files against local or remote backends.

    The same commands work against any backend Trilogy knows about. Only the
    local filesystem ships today; future releases will add cloud storage and
    remote git model backends, so write CLI-friendly scripts (and agent loops)
    against ``trilogy file`` instead of ad-hoc shell or python plumbing.

  Options:
    --help  Show this message and exit.

  Commands:
    delete  Delete the file or directory at PATH.
    exists  Exit 0 if PATH exists, 1 otherwise.
    list    List files at PATH (default: current directory).
    move    Move (or rename) SRC to DST.
    read    Read the file at PATH and write its contents to stdout.
    write   Write/overwrite the file at PATH.
  ```
- `trilogy syntax example query-structure`

  ```text
  No such command 'syntax'.
  ```
- `trilogy explore raw/inventory_facts.preql`

  ```text
  Invalid value for 'PATH': File 'raw/inventory_facts.preql' does not exist.
  ```
- `trilogy explore raw.store_sales`

  ```text
  Invalid value for 'PATH': File 'raw.store_sales' does not exist.
  ```
- `trilogy explore raw.store`

  ```text
  Invalid value for 'PATH': File 'raw.store' does not exist.
  ```
- `trilogy explore store_sales.preql --regex grain|store_sales|item|date`

  ```text
  Invalid value for 'PATH': File 'store_sales.preql' does not exist.
  ```
- `trilogy explore repro.preql`

  ```text
  Invalid value for 'PATH': File 'repro.preql' does not exist.
  ```
- `trilogy explore all_sales.preql`

  ```text
  Invalid value for 'PATH': File 'all_sales.preql' does not exist.
  ```
- `trilogy explore web_sales.preql`

  ```text
  Invalid value for 'PATH': File 'web_sales.preql' does not exist.
  ```

### `disabled-tool`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

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
- `trilogy file read inspect.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read inspect6.preql`

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
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe6.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_840315271.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_diag.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read probe5.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write inspect5.preql`

  ```text
  refused to write 'inspect5.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...id
              else true end
   ??? group by sas.channel;
  ```
- `trilogy file write answer_3705756794.preql`

  ```text
  refused to write 'answer_3705756794.preql': not syntactically valid Trilogy.

  Parse error:
    --> 42:78
     |
  42 |         when combined.channel = 'STORE' and combined.entity is not null then + 'store' || combined.entity
     |                                                                              ^---
     |
     = expected sum_operator
  Location:
  ...bined.entity is not null then ??? + 'store' || combined.entity
  ```
- `trilogy file write answer_3263796749.preql`

  ```text
  refused to write 'answer_3263796749.preql': not syntactically valid Trilogy.

  Parse error:
   --> 3:166
    |
  3 | def bucket_val(lo, hi, thr) -> (count(ss.line_item ? ss.quantity between lo and hi) by * > thr) ? (avg(ss.ext_discount_amount ? ss.quantity between lo and hi) by *) : (avg(ss.net_paid ? ss.quantity between lo and hi) by *);
    |                                                                                                                                                                      ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...tity between lo and hi) by *) ??? : (avg(ss.net_paid ? ss.quanti...
  ```
- `trilogy file write answer_1809796058.preql`

  ```text
  refused to write 'answer_1809796058.preql': not syntactically valid Trilogy.

  Parse error:
    --> 24:31
     |
  24 |     @bcount(elapsed_days > 30 and elapsed_days <= 60) as days_31_60,
     |                               ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
      @bcount(elapsed_days > 30 ??? and elapsed_days <= 60) as day...
  ```
- `trilogy file write answer_3979964698.preql --content import raw.all_sales as sales;

# Per-line sales/net amount using each channel's formula.
auto row_sales…th_net(11) as net_nov,
    @month_net(12) as net_dec,
order by sales.warehouse.name asc nulls first, sales.sale_date.year asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_3979964698.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [227]: Named function `month_sales` must be invoked with a leading `@` - write `@month_sales(...)`. A user-defined (`def`) function is called with `@`; bare `month_sales(...)` is read as a concept reference, not a call, so it fails at the `(`. Built-in functions (`sum(...)`, `count(...)`, `coalesce(...)`) need no `@`.
  Location:
  ...year = m); def month_sf(m) -> ??? month_sales(m) / sales.warehou...
  ```
- `trilogy file write check_sn.preql`

  ```text
  refused to write 'check_sn.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ....sk, ss.ticket_number)) as n
   ??? group by ss.store.name;
  ```
- `trilogy file write answer_diag.preql`

  ```text
  refused to write 'answer_diag.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:1
    |
  1 | placeholder
    | ^---
    |
    = expected start
  Location:
   ??? placeholder
  ```
- `trilogy file write probe_reason.preql`

  ```text
  refused to write 'probe_reason.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...reason,
       count(1) as cnt
   ??? group by ss.return_reason.desc...
  ```

### `other`

- `trilogy run answer_3697440276.preql`

  ```text
  zero-length delimited identifier at or near \"\"\"\"\n\nLINE 115: ...r_id_at_sales_billing_customer_id_at_w02_cid_at_w02_cid']\".\"\",\"macho\".\"INVALID_ALIAS: [MODELS_EXECUTE] Concept local...\n                                                                        ^\n[SQL: \nWITH \nscrawny as (\nSELECT\n    \"sales_billing_customer_customers\".\"C_CUSTOMER_ID\" as \"w02_cid\",\n    sum(\"sales_web_sales_unified\".\"WS_EXT_LIST_PRICE\" - \"sales_web_sales_unified\".\"WS_EXT_DISCOUNT_AMT\") as \"w02_rev\"\nFROM\n    \"web_sales\" as \"sales_web_sales_unified\"\n    INNER JOIN \"date_dim\" as
  …
  t_w02_cid_at_w02_cid']\".\"\") asc nulls first,\n    \"abundant\".\"first_name\" asc nulls first,\n    \"abundant\".\"last_name\" asc nulls first,\n    \"abundant\".\"preferred_flag\" asc nulls first\nLIMIT (100)]\n(Background on this error at: https://sqlalche.me/e/20/f405)",
    "error_type": "ProgrammingError"
  }
  {
    "event": "summary",
    "statements": 1,
    "duration_ms": 22.989,
    "ok": false,
    "rows": 0
  }
  {
    "event": "output_truncated",
    "dropped_events": 1,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```
- `trilogy run /tmp/tmp_read.preql`

  ```text
  No dialect specified. Provide dialect as argument or set engine.dialect in config file.
  ```
- `trilogy run scratch1.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy run -`

  ```text
  Syntax error in stdin: Cannot compare DATE (ref:d.date.date) and STRING (2000-01-03) of different types with operator = in ref:d.date.date = 2000-01-03
  ```
- `trilogy run answer_1772060640.preql`

  ```text
  Syntax error in answer_1772060640.preql: ORDER BY references 'ss.customer.sk', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.customer.sk order by ss.customer.sk asc`.
  ```
- `trilogy run probe4.preql`

  ```text
  Syntax error in probe4.preql: Cannot compare DATE (ref:st.sale_date.date) and STRING (2000-08-23) of different types with operator >= in ref:st.sale_date.date >= 2000-08-23
  ```
- `trilogy run answer_943796012.preql`

  ```text
  Syntax error in answer_943796012.preql: `by rollup (…)` requires at least one aggregate (or re-aggregable pre-aggregated measure) in the select to group; found none.
  ```

### `undefined-concept`

- `trilogy run answer_883027685.preql`

  ```text
  Syntax error in answer_883027685.preql: Undefined concept: item.category. Suggestions: ['ss.item.category', 'ss.item.category_id', 'ss.item.color']
  ```
- `trilogy run answer_2928586490.preql`

  ```text
  Syntax error in answer_2928586490.preql: Undefined concept: store.sale_date. Suggestions: ['store.sale_date.sk', 'store.sale_date.id', 'store.sale_date.date', 'store.sale_date.year', 'store.sale_date.day_of_week', 'store.sale_date.day_of_month']
  ```
- `trilogy run -`

  ```text
  Syntax error in stdin: Undefined concept: d.date.week_seq (line 6, col 5, in SELECT). Suggestions: ['d.week_seq', 'd.date.day_of_week', 'd.date.date']
  ```
- `trilogy run answer_3979964698.preql`

  ```text
  Syntax error in answer_3979964698.preql: Undefined concept: sales.row_sales. Suggestions: ['sales.sales_price', 'sales.wholesale_cost', 'sales.sale_time.sk']
  ```
- `trilogy run answer_3036656719.preql`

  ```text
  Syntax error in answer_3036656719.preql: Undefined concept: _virt_agg_grouping_449731194548500.
  ```
- `trilogy run probe3.preql`

  ```text
  Syntax error in probe3.preql: Undefined concept: st.sale_store.sk (line 2, col 7, in WHERE). Suggestions: ['st.sale_time.sk', 'st.sale_date.sk', 'st.store.sk', 'st.return_store.sk', 'st.customer.sk', 'st.return_customer.sk']
  ```

### `file-not-found`

- `trilogy run dummy_placeholder.preql`

  ```text
  Input 'dummy_placeholder.preql' does not exist.
  ```
- `trilogy run /dev/stdin`

  ```text
  Input '/dev/stdin' does not exist.
  ```
- `trilogy run answer_diag.preql`

  ```text
  Input 'answer_diag.preql' does not exist.
  ```
- `trilogy run check_date.preql`

  ```text
  Input 'check_date.preql' does not exist.
  ```

### `join-resolution`

- `trilogy run test3.preql`

  ```text
  Resolution error in test3.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 27). The requested concepts split into 2 disconnected subgraphs: {catalog_cnt}; {item_id, return_qty, store_qty, store_ret.cust_sk, store_ret.item_sk}. Are you missing a join or merge statement to relate them?
  Note: the membership predicate(s) `(store_ret.cust_sk, store_ret.item_sk) in (catalog_set.cust_sk, catalog_set.item_sk)` span these subgraphs, but membership only filters rows on its left side — it does not join the two sides, so it cannot relate them for outputs or grouping. To combine values from both sides, author a query-scoped join or a merge on shared keys.
  ```
- `trilogy run answer_3553309440.preql`

  ```text
  Resolution error in answer_3553309440.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {_store_agg_cid, _store_agg_total, ss.customer.current_address.county, ss.customer.current_address.state, ss.customer.sk, ss.customer.sk, ss.sale_date.month_seq, ss.store.county, ss.store.state}; {dec98_seq, dec98_seq}. Are you missing a join or merge statement to relate them?
  Note: the membership predicate(s) `(ss.customer.sk) in (qual_cat)` span these subgraphs, but membership only filters rows on its left side — it does not join the two sides, so it cannot relate them for outputs or grouping. To combine values from both sides, author a query-scoped join or a merge on shared keys.
  ```

### `import-path`

- `trilogy run answer_1821211265.preql`

  ```text
  Import error in answer_1821211265.preql: Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```

### `syntax-missing-alias`

- `trilogy file write answer_2869182220.preql`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `hidden grain fields
      c.sk as hidden_grain_fields_c_sk`
  Location:
   as full_name,
       -- hidden ??? grain fields
       c.sk as c_sk...
  ```
